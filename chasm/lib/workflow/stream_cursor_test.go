package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	apistreampb "go.temporal.io/api/stream/v1"
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

// Two publishes in one workflow task must not share a transaction id, and
// neither must two attempts of that task. Storage keys a log node by node id
// and transaction id, so a shared id collapses two writes into one row whose
// survivor is arrival order rather than which one committed.
//
// The handler no longer derives an id at all. It takes one from the shard, so
// what this pins is that it asks each time rather than reusing.
func TestPublishTakesAFreshTransactionIDPerCommand(t *testing.T) {
	ctx := newStreamCursorTestContext()

	// A backend, because the handler writes the publish event and that event is
	// part of what the command owes.
	backend := &chasm.MockNodeBackend{
		HandleAddHistoryEvent: func(
			t enumspb.EventType, set func(*historypb.HistoryEvent),
		) *historypb.HistoryEvent {
			e := &historypb.HistoryEvent{EventType: t}
			set(e)
			return e
		},
	}
	w := &Workflow{MSPointer: chasm.NewMSPointer(backend)}

	issued := 0
	opts := CommandHandlerOptions{
		WorkflowTaskCompletedEventID: 10,
		NextTxnID: func() (int64, error) {
			issued++
			return int64(1000 + issued), nil
		},
	}

	publish := &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
		Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
			AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
				Messages: []*apistreampb.StreamMessage{
					{Body: &commonpb.Payload{Data: []byte("x")}},
				},
			},
		},
	}

	require.NoError(t, handleAddStreamMessagesCommand(ctx, w, allowAnySize{}, publish, opts))
	require.NoError(t, handleAddStreamMessagesCommand(ctx, w, allowAnySize{}, publish, opts))

	require.Equal(t, 2, issued, "each command must draw its own id")
	require.Equal(t, int64(1002), w.Streams[DefaultStreamName].Get(ctx).State.GetLastTxnId(),
		"the second publish must commit under the id it was given")
}

type allowAnySize struct{}

func (allowAnySize) IsValidPayloadSize(int) bool { return true }
