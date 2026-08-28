package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

func newStreamCursorTestContext() chasm.MutableContext {
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{
					NamespaceID: "ns-1",
					BusinessID:  "wf-1",
					RunID:       "run-1",
				}
			},
		},
	}
}

// Built directly rather than through NewStream, which wires a visibility field
// needing a live context.
func newAttachedStream(t *testing.T, ctx chasm.MutableContext, count int) *stream.Stream {
	t.Helper()

	s := &stream.Stream{
		State: &streampb.StreamState{
			CollectionId: "col-1",
			BucketSize:   stream.DefaultBucketSize,
			Producers:    make(map[string]*streampb.ProducerCursor),
			Consumers:    make(map[string]*streampb.ConsumerCursor),
		},
	}

	messages := make([]*streampb.StreamMessage, count)
	for i := range messages {
		messages[i] = &streampb.StreamMessage{Kind: streampb.STREAM_MESSAGE_KIND_DATA}
	}
	_, err := s.AddMessages(ctx, stream.AddMessagesRequest{Messages: messages, TxnID: 1})
	require.NoError(t, err)

	return s
}

// Subscribing has to pin the stream's floor in the same transaction that
// creates the cursor. Registered separately, the pin could be lost while the
// cursor survived, and truncation would then be free to take a range the
// cursor still points at.
func TestSubscribeRegistersTheStreamFloor(t *testing.T) {
	ctx := newStreamCursorTestContext()
	w := &Workflow{}
	owned := newAttachedStream(t, ctx, 4)
	w.Streams = chasm.Map[string, *stream.Stream]{
		DefaultStreamName: chasm.NewComponentField(ctx, owned),
	}

	start, err := w.SubscribeToOwnedStream(ctx, DefaultStreamName, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), start)

	// The pin is what Truncate consults, so assert through Truncate rather than
	// through the map: that is the behaviour the interlock owes.
	_, err = owned.Truncate(ctx, 1)
	require.ErrorContains(t, err, "an active consumer still needs")
}

func TestSubscribeFromTheTailResolvesToHead(t *testing.T) {
	ctx := newStreamCursorTestContext()
	w := &Workflow{}
	owned := newAttachedStream(t, ctx, 4)
	w.Streams = chasm.Map[string, *stream.Stream]{
		DefaultStreamName: chasm.NewComponentField(ctx, owned),
	}

	start, err := w.SubscribeToOwnedStream(ctx, DefaultStreamName, -1)
	require.NoError(t, err)
	require.Equal(t, int64(4), start, "a negative offset means from wherever the stream is now")
}

func TestSubscribeRejectsAStreamTheWorkflowDoesNotOwn(t *testing.T) {
	ctx := newStreamCursorTestContext()
	w := &Workflow{}

	_, err := w.SubscribeToOwnedStream(ctx, "absent", 0)
	require.ErrorContains(t, err, "does not own a stream")
}

// Committing a delivered range has to move the floor with the cursor,
// otherwise the pin holds storage forever at the offset it started from.
func TestCommitStreamCursorsAdvancesTheStreamFloor(t *testing.T) {
	ctx := newStreamCursorTestContext()
	w := &Workflow{}
	owned := newAttachedStream(t, ctx, 4)
	w.Streams = chasm.Map[string, *stream.Stream]{
		DefaultStreamName: chasm.NewComponentField(ctx, owned),
	}

	_, err := w.SubscribeToOwnedStream(ctx, DefaultStreamName, 0)
	require.NoError(t, err)

	cursor := w.StreamCursors[DefaultStreamName].Get(ctx)
	require.NoError(t, cursor.StagePending(ctx, 0, 3))

	recorded := w.CommitStreamCursors(ctx)
	require.Len(t, recorded, 1)
	require.Equal(t, int64(0), recorded[0].GetFromOffset())
	require.Equal(t, int64(3), recorded[0].GetToOffset())

	// Consumed offsets no longer need to be re-readable, so the floor may pass
	// them now and not before.
	_, err = owned.Truncate(ctx, 3)
	require.NoError(t, err)

	// The pin moved to 3 rather than being released: everything at or past the
	// cursor still has to be re-readable.
	_, err = owned.Truncate(ctx, 4)
	require.ErrorContains(t, err, "an active consumer still needs",
		"advancing the floor must not drop the pin altogether")
}

// An idle task records an empty range, which must leave the floor alone.
func TestCommitStreamCursorsWithAnEmptyRangeHoldsTheFloor(t *testing.T) {
	ctx := newStreamCursorTestContext()
	w := &Workflow{}
	owned := newAttachedStream(t, ctx, 4)
	w.Streams = chasm.Map[string, *stream.Stream]{
		DefaultStreamName: chasm.NewComponentField(ctx, owned),
	}

	_, err := w.SubscribeToOwnedStream(ctx, DefaultStreamName, 0)
	require.NoError(t, err)

	cursor := w.StreamCursors[DefaultStreamName].Get(ctx)
	require.NoError(t, cursor.StagePending(ctx, 0, 0))

	recorded := w.CommitStreamCursors(ctx)
	require.Len(t, recorded, 1, "an empty range is still recorded")
	require.Equal(t, recorded[0].GetFromOffset(), recorded[0].GetToOffset())

	_, err = owned.Truncate(ctx, 1)
	require.ErrorContains(t, err, "an active consumer still needs",
		"consuming nothing must not release the floor")
}

// A retried workflow task replays the same commands from the same completed
// event id. Storage keys a log node by node id and transaction id, so two
// attempts under one id collapse into a single row whose survivor is decided by
// arrival order rather than by which attempt committed.
func TestStreamTxnIDSeparatesWorkflowTaskAttempts(t *testing.T) {
	s := &stream.Stream{State: &streampb.StreamState{}}

	first := streamTxnID(s, 10, 1)
	retry := streamTxnID(s, 10, 2)
	require.Greater(t, retry, first, "a retry must supersede the attempt it replaces")

	// An unset attempt still has to produce the pre-existing id, so a caller
	// that does not populate it is not silently shifted.
	require.Equal(t, first, streamTxnID(s, 10, 0))

	// The committed id still wins when it has moved past the event id.
	s.State.LastTxnId = 50
	require.Equal(t, int64(51), streamTxnID(s, 10, 1))
}
