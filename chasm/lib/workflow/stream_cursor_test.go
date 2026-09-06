package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
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
		State: &streamlib.StreamState{
			CollectionId: "col-1",
			BucketSize:   stream.DefaultBucketSize,
			Producers:    make(map[string]*streamlib.ProducerCursor),
			Consumers:    make(map[string]*streamlib.ConsumerCursor),
		},
	}

	messages := make([]*streamlib.StreamMessage, count)
	for i := range messages {
		messages[i] = &streamlib.StreamMessage{Kind: streamlib.STREAM_MESSAGE_KIND_DATA}
	}
	_, err := s.AddMessages(ctx, stream.AddMessagesRequest{Messages: messages})
	require.NoError(t, err)

	return s
}

// Subscribing registers the consumer on the stream, which decides both whether
// an append is worth waking it for and what retention may not take. A range
// this workflow consumes goes into its History, and a replay is asked to
// reproduce it, so the stream holds those messages while the subscription is
// active and releases them when it is not.
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

	require.Equal(t, int64(0), consumer.GetReplayFloor())

	// And it holds the floor for as long as it is subscribed.
	require.ErrorContains(t, owned.Truncate(ctx, 1), "still depends on offset 0")
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
				Messages: []*streampb.StreamMessage{
					{Body: &commonpb.Payload{Data: []byte("x")}},
					{Body: &commonpb.Payload{Data: []byte("y")}},
				},
			},
		},
	}

	require.NoError(t, handleAddStreamMessagesCommand(ctx, w, allowAnySize{}, publish, opts))
	require.NoError(t, handleAddStreamMessagesCommand(ctx, w, allowAnySize{}, publish, opts))

	// Both publishes committed with the workflow task, so the batches are on
	// the component keyed by the offsets they start at.
	owned := w.Streams[DefaultStreamName].Get(ctx)
	require.Len(t, owned.Batches, 2)
	_, ok := owned.Batches[0]
	require.True(t, ok)
	_, ok = owned.Batches[2]
	require.True(t, ok)
	require.Equal(t, int64(4), owned.State.GetHeadOffset())
}

type allowAnySize struct{}

func (allowAnySize) IsValidPayloadSize(int) bool { return true }

// A consumer that falls behind a truncating stream must be told, not handed
// what is left with a hole in it.
//
// Being outrun is possible again once the subscription is released, which is
// deliberate: a floor that nothing ever gave up would keep every message for
// as long as a consumer had ever existed. The trade is that being outrun has
// to be loud.
func TestConsumerOutrunByTruncationIsToldSo(t *testing.T) {
	ctx := newStreamCursorTestContext()
	w := &Workflow{}
	owned := newAttachedStream(t, ctx, 4)
	w.Streams = chasm.Map[string, *stream.Stream]{
		DefaultStreamName: chasm.NewComponentField(ctx, owned),
	}

	_, err := w.SubscribeToOwnedStream(ctx, DefaultStreamName, 0)
	require.NoError(t, err)

	// Someone decides these messages are no longer needed, and only then can
	// the stream move past where this consumer is sitting.
	owned.DeregisterConsumer(ctx, streamConsumerID(DefaultStreamName))
	err = owned.Truncate(ctx, 3)
	require.NoError(t, err, "a released floor must not keep holding")

	cursor := w.StreamCursors[DefaultStreamName].Get(ctx)
	require.Less(t, cursor.Offset(), owned.State.GetBaseOffset())

	state, err := owned.Snapshot(ctx, struct{}{})
	require.NoError(t, err)
	require.Equal(t, int64(3), state.GetBaseOffset(),
		"the floor moved, which is what the consumer has to find out about")
}
