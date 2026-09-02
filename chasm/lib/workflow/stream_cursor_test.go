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
	_, err := s.AddMessages(ctx, stream.AddMessagesRequest{Messages: messages})
	require.NoError(t, err)

	return s
}

// Subscribing registers the consumer on the stream, which is what decides
// whether an append is worth waking it for. It no longer holds the stream's
// floor: a floor held by a consumer was never released when that consumer
// finished, so it turned any cap into a no-op.
func TestSubscribeRegistersTheConsumer(t *testing.T) {
	ctx := newStreamCursorTestContext()
	w := &Workflow{}
	owned := newAttachedStream(t, ctx, 4)
	w.Streams = chasm.Map[string, *stream.Stream]{
		DefaultStreamName: chasm.NewComponentField(ctx, owned),
	}

	start, err := w.SubscribeToOwnedStream(ctx, DefaultStreamName, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), start)

	consumer := owned.State.GetConsumers()[streamConsumerID(DefaultStreamName)]
	require.NotNil(t, consumer, "the stream has to know who is reading it")
	require.Equal(t, int64(0), consumer.GetOffset())
	require.True(t, consumer.GetActive())

	// And it does not hold the floor.
	_, err = owned.Truncate(ctx, 1)
	require.NoError(t, err)
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

// Committing a delivered range moves the consumer's cursor on the stream, so
// the stream knows how far this reader has got.
func TestCommitStreamCursorsAdvancesTheConsumer(t *testing.T) {
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

	require.Equal(t, int64(3),
		owned.State.GetConsumers()[streamConsumerID(DefaultStreamName)].GetOffset(),
		"the stream must see how far the consumer has read")
}

// An idle task records an empty range, which must leave the consumer alone.
func TestCommitStreamCursorsWithAnEmptyRangeHoldsTheConsumer(t *testing.T) {
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

	require.Equal(t, int64(0),
		owned.State.GetConsumers()[streamConsumerID(DefaultStreamName)].GetOffset(),
		"consuming nothing must not move the consumer")
}

// Two publishes in one workflow task each stage their own batch, at the offsets
// they landed at.
//
// The offset a batch starts at is the key its row is written under, so two
// publishes must not collide and a retry of either must address the same row
// it wrote before. There is no transaction id involved any more: the store
// resolves a rewrite by replacing, and the frontier decides what a reader sees.
func TestPublishStagesEachBatchAtItsOwnOffset(t *testing.T) {
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
	opts := CommandHandlerOptions{WorkflowTaskCompletedEventID: 10}

	publish := &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
		Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
			AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
				Messages: []*apistreampb.StreamMessage{
					{Body: &commonpb.Payload{Data: []byte("x")}},
					{Body: &commonpb.Payload{Data: []byte("y")}},
				},
			},
		},
	}

	require.NoError(t, handleAddStreamMessagesCommand(ctx, w, allowAnySize{}, publish, opts))
	require.NoError(t, handleAddStreamMessagesCommand(ctx, w, allowAnySize{}, publish, opts))

	staged := w.DrainStreamAppends()
	require.Len(t, staged, 2)
	require.Equal(t, int64(0), staged[0].Append.StartOffset)
	require.Equal(t, int64(2), staged[0].Append.NextOffset)
	require.Equal(t, int64(2), staged[1].Append.StartOffset)
	require.Equal(t, int64(4), staged[1].Append.NextOffset)
	require.Equal(t, int64(4), w.Streams[DefaultStreamName].Get(ctx).State.GetHeadOffset())
}

type allowAnySize struct{}

func (allowAnySize) IsValidPayloadSize(int) bool { return true }

// A consumer that falls behind a truncating stream must be told, not handed
// what is left with a hole in it.
//
// Nothing holds the floor for a consumer any more. The floor that used to wait
// for the slowest reader was never released when that reader finished, so a
// capped stream kept everything for as long as a consumer had ever existed.
// The trade is that a consumer can now be outrun, and the whole point of the
// trade is that being outrun is loud.
func TestConsumerOutrunByTruncationIsToldSo(t *testing.T) {
	ctx := newStreamCursorTestContext()
	w := &Workflow{}
	owned := newAttachedStream(t, ctx, 4)
	w.Streams = chasm.Map[string, *stream.Stream]{
		DefaultStreamName: chasm.NewComponentField(ctx, owned),
	}

	_, err := w.SubscribeToOwnedStream(ctx, DefaultStreamName, 0)
	require.NoError(t, err)

	// The stream moves past where this consumer is sitting.
	_, err = owned.Truncate(ctx, 3)
	require.NoError(t, err, "a consumer must not hold the floor")

	cursor := w.StreamCursors[DefaultStreamName].Get(ctx)
	require.Less(t, cursor.Offset(), owned.State.GetBaseOffset())

	state, err := owned.Snapshot(ctx, struct{}{})
	require.NoError(t, err)
	require.Equal(t, int64(3), state.GetBaseOffset(),
		"the floor moved, which is what the consumer has to find out about")
}
