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
			Producers: make(map[string]*streamlib.ProducerCursor),
			Consumers: make(map[string]*streamlib.ConsumerCursor),
		},
	}

	messages := make([]*streamlib.StreamRecord, count)
	for i := range messages {
		messages[i] = &streamlib.StreamRecord{Kind: streampb.STREAM_RECORD_KIND_DATA}
	}
	_, err := s.AddMessages(ctx, stream.AddMessagesRequest{Records: messages})
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

	start, err := w.SubscribeToOwnedStream(
		ctx, DefaultStreamName, stream.AtOffset(0), stream.DefaultLimits())
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

	start, err := w.SubscribeToOwnedStream(
		ctx, DefaultStreamName, stream.Tail(), stream.DefaultLimits())
	require.NoError(t, err)
	require.Equal(t, int64(4), start, "tail means from wherever the stream is now")
}

// A workflow reads a topic by name before anything has been written to it, so
// subscribing has to bring the stream into being the way a first write does.
func TestSubscribeCreatesTheStreamItNames(t *testing.T) {
	ctx := newStreamCursorTestContext()
	w := &Workflow{}

	start, err := w.SubscribeToOwnedStream(ctx, "inputs", stream.AtOffset(0), stream.DefaultLimits())
	require.NoError(t, err)
	require.Equal(t, int64(0), start)

	field, ok := w.Streams["inputs"]
	require.True(t, ok, "the subscribed name is now an owned stream")
	state, err := field.Get(ctx).Snapshot(ctx, struct{}{})
	require.NoError(t, err)
	require.Equal(t, int64(0), state.GetHeadOffset())
	require.Len(t, state.GetConsumers(), 1, "the subscription pinned the new stream")
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

	_, err := w.SubscribeToOwnedStream(
		ctx, DefaultStreamName, stream.AtOffset(0), stream.DefaultLimits())
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

	_, err := w.SubscribeToOwnedStream(
		ctx, DefaultStreamName, stream.AtOffset(0), stream.DefaultLimits())
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

// Two publishes in one workflow task each write their own batch, keyed by the
// offset it starts at, so neither collides with the other and a retry of
// either addresses the same key it wrote before.
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
		CommandType: enumspb.COMMAND_TYPE_APPEND_STREAM_RECORDS,
		Attributes: &commandpb.Command_AppendStreamRecordsCommandAttributes{
			AppendStreamRecordsCommandAttributes: &commandpb.AppendStreamRecordsCommandAttributes{
				Records: []*streampb.StreamRecord{
					{Body: &commonpb.Payload{Data: []byte("x")}},
					{Body: &commonpb.Payload{Data: []byte("y")}},
				},
			},
		},
	}

	limits := stream.DefaultLimits()
	require.NoError(t, handleAppendStreamRecordsCommand(ctx, w, allowAnySize{}, publish, opts, limits))
	require.NoError(t, handleAppendStreamRecordsCommand(ctx, w, allowAnySize{}, publish, opts, limits))

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

	_, err := w.SubscribeToOwnedStream(
		ctx, DefaultStreamName, stream.AtOffset(0), stream.DefaultLimits())
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

func newStreamCursorTestContextForRun(runID string) chasm.MutableContext {
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{NamespaceID: "ns-1", BusinessID: "wf-1", RunID: runID}
			},
		},
	}
}

func subscribedEvent(streamID string, startOffset int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_STREAM_SUBSCRIBED,
		Attributes: &historypb.HistoryEvent_WorkflowStreamSubscribedEventAttributes{
			WorkflowStreamSubscribedEventAttributes: &historypb.WorkflowStreamSubscribedEventAttributes{
				StreamId:    streamID,
				StartOffset: startOffset,
			},
		},
	}
}

// A run rebuilt from its history, which is what a reset produces, gets its
// cursors back from the events alone: the subscribe event places the cursor
// and each completed task's recorded range moves it.
func TestRebuildRecreatesTheCursorFromItsEvents(t *testing.T) {
	ctx := newStreamCursorTestContext()
	w := &Workflow{}

	require.NoError(t, streamSubscribedEvent{}.Apply(ctx, w, subscribedEvent("inputs", 1)))
	cursor := w.StreamCursors["inputs"].Get(ctx)
	require.Equal(t, int64(1), cursor.StartOffset())
	require.Equal(t, int64(1), cursor.Offset())
	require.False(t, cursor.IsExternal(), "the event cannot say where the stream lives")

	require.NoError(t, w.ApplyConsumedStreamRanges(ctx, []*streampb.StreamRange{
		{StreamId: "inputs", FromOffset: 1, ToOffset: 4},
		{StreamId: "out-of-band", FromOffset: 3, ToOffset: 9},
	}))
	require.Equal(t, int64(4), cursor.Offset())
	require.Equal(t, int64(1), cursor.StartOffset(), "where reading began does not move")
	outOfBand, ok := w.StreamCursors["out-of-band"]
	require.True(t, ok, "a recorded range proves a subscription the events never mentioned")
	require.Equal(t, int64(3), outOfBand.Get(ctx).StartOffset())
	require.Equal(t, int64(9), outOfBand.Get(ctx).Offset())

	// The same subscribe event applied twice, as a resubscribe would leave in
	// History, must not rewind the cursor.
	require.NoError(t, streamSubscribedEvent{}.Apply(ctx, w, subscribedEvent("inputs", 1)))
	require.Equal(t, int64(4), w.StreamCursors["inputs"].Get(ctx).Offset())
}

// newAttachedStreamWithBodies is newAttachedStream with one record per body,
// for a test that reads the records back.
func newAttachedStreamWithBodies(
	t *testing.T, ctx chasm.MutableContext, bodies ...string,
) *stream.Stream {
	t.Helper()
	s := &stream.Stream{
		State: &streamlib.StreamState{
			Producers: make(map[string]*streamlib.ProducerCursor),
			Consumers: make(map[string]*streamlib.ConsumerCursor),
		},
	}
	// Two appends, so the copy has a batch to trim and a batch to take whole.
	for _, half := range [][]string{bodies[:len(bodies)/2], bodies[len(bodies)/2:]} {
		records := make([]*streamlib.StreamRecord, len(half))
		for i, body := range half {
			records[i] = &streamlib.StreamRecord{
				Kind: streampb.STREAM_RECORD_KIND_DATA,
				Body: &commonpb.Payload{Data: []byte(body)},
			}
		}
		_, err := s.AddMessages(ctx, stream.AddMessagesRequest{Records: records})
		require.NoError(t, err)
	}
	return s
}

// A reset run's cursor on a stream the base run owned gets a stream of the
// reset run's own, starting where the cursor stands, so the ranges below it
// stay in the base run and everything from here on is the reset run's.
func TestResetRunInheritsAnOwnedStreamAtItsCursor(t *testing.T) {
	baseCtx := newStreamCursorTestContextForRun("base-run")
	base := &Workflow{}
	base.Streams = chasm.Map[string, *stream.Stream]{
		DefaultStreamName: chasm.NewComponentField(baseCtx,
			newAttachedStreamWithBodies(t, baseCtx, "base-0", "base-1", "base-2", "base-3")),
	}
	_, err := base.SubscribeToOwnedStream(
		baseCtx, DefaultStreamName, stream.AtOffset(0), stream.DefaultLimits())
	require.NoError(t, err)

	resetCtx := newStreamCursorTestContextForRun("reset-run")
	reset := &Workflow{}
	require.NoError(t,
		streamSubscribedEvent{}.Apply(resetCtx, reset, subscribedEvent(DefaultStreamName, 0)))
	// The cursor stands at 1, inside the base run's first batch, so the copy
	// has to trim that batch rather than take it whole. The reset-point task
	// had been given [1,3); the base run's last record, past that range,
	// belongs to the timeline the reset leaves behind.
	require.NoError(t, reset.ApplyConsumedStreamRanges(resetCtx, []*streampb.StreamRange{
		{StreamId: DefaultStreamName, FromOffset: 0, ToOffset: 1},
	}))
	resetPoint := []*streampb.StreamRange{{StreamId: DefaultStreamName, FromOffset: 1, ToOffset: 3}}

	require.NoError(t, reset.InheritStreamsOnReset(
		resetCtx, base, baseCtx, stream.DefaultLimits(), resetPoint))

	cursor := reset.StreamCursors[DefaultStreamName].Get(resetCtx)
	require.False(t, cursor.IsExternal())
	require.Equal(t, int64(1), cursor.Offset())

	own := reset.OwnedStream(resetCtx, DefaultStreamName)
	require.NotNil(t, own, "the reset run reads and writes a stream of its own")
	state, err := own.Snapshot(resetCtx, struct{}{})
	require.NoError(t, err)
	require.Equal(t, int64(1), state.GetBaseOffset(), "the stream continues the offset space")
	require.Equal(t, int64(3), state.GetHeadOffset(),
		"the range the reset-point task consumed is carried, so it gets it again")
	require.NotNil(t, state.GetBudget(), "an owned stream is budgeted like one created by a publish")
	require.Empty(t, state.GetProducers(), "a copy is the stream's own past, not a producer's append")
	pin := state.GetConsumers()[streamConsumerID(DefaultStreamName)]
	require.NotNil(t, pin, "the reset run pins its own stream")
	require.Equal(t, "reset-run", pin.GetRunId())
	require.Equal(t, int64(1), pin.GetReplayFloor())

	// The carried records read back at the offsets the base run gave them;
	// nothing below the cursor or past the range came along.
	window, err := own.ReadWindow(resetCtx, stream.WindowRequest{From: 1})
	require.NoError(t, err)
	carried, next, err := stream.CollectRecords(window.Blobs, window.Starts, 1, window.To, 10, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), next)
	require.Equal(t, []string{"base-1", "base-2"}, testBodies(carried))
	require.Equal(t, []int64{1, 2}, []int64{carried[0].GetOffset(), carried[1].GetOffset()})
	_, err = own.ReadWindow(resetCtx, stream.WindowRequest{From: 0})
	require.Equal(t, stream.ReasonCursorBelowFloor, stream.ReasonOf(err.Error()))

	// The base run's stream is untouched: it still holds what the reset run's
	// history refers to.
	baseState, err := base.OwnedStream(baseCtx, DefaultStreamName).Snapshot(baseCtx, struct{}{})
	require.NoError(t, err)
	require.Equal(t, int64(0), baseState.GetBaseOffset())
	require.Equal(t, int64(4), baseState.GetHeadOffset())

	// A publish on the reset run lands after the carried records.
	result, err := own.AddMessages(resetCtx, stream.AddMessagesRequest{
		Records: []*streamlib.StreamRecord{{Kind: streampb.STREAM_RECORD_KIND_DATA}},
		Limits:  stream.DefaultLimits(),
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), result.FirstOffset)
}

// A reset whose reset-point task recorded nothing for the stream, because it
// consumed nothing or never completed, carries nothing: the reset run's stream
// starts empty at the cursor.
func TestResetRunCarriesNothingWithoutAResetPointRange(t *testing.T) {
	baseCtx := newStreamCursorTestContextForRun("base-run")
	base := &Workflow{}
	base.Streams = chasm.Map[string, *stream.Stream]{
		DefaultStreamName: chasm.NewComponentField(baseCtx, newAttachedStream(t, baseCtx, 4)),
	}
	_, err := base.SubscribeToOwnedStream(
		baseCtx, DefaultStreamName, stream.AtOffset(0), stream.DefaultLimits())
	require.NoError(t, err)

	resetCtx := newStreamCursorTestContextForRun("reset-run")
	reset := &Workflow{}
	require.NoError(t,
		streamSubscribedEvent{}.Apply(resetCtx, reset, subscribedEvent(DefaultStreamName, 0)))
	require.NoError(t, reset.ApplyConsumedStreamRanges(resetCtx, []*streampb.StreamRange{
		{StreamId: DefaultStreamName, FromOffset: 0, ToOffset: 2},
	}))
	empty := []*streampb.StreamRange{{StreamId: DefaultStreamName, FromOffset: 2, ToOffset: 2}}
	require.NoError(t, reset.InheritStreamsOnReset(
		resetCtx, base, baseCtx, stream.DefaultLimits(), empty))

	state, err := reset.OwnedStream(resetCtx, DefaultStreamName).Snapshot(resetCtx, struct{}{})
	require.NoError(t, err)
	require.Equal(t, int64(2), state.GetBaseOffset())
	require.Equal(t, int64(2), state.GetHeadOffset(),
		"the base run's records past the cursor stay with the base run")
}

func testBodies(records []*streamlib.StreamRecord) []string {
	out := make([]string, len(records))
	for i, r := range records {
		out[i] = string(r.GetBody().GetData())
	}
	return out
}

// A reset run's cursor on a stream in another execution stays on that stream
// and learns what the base run knew of its frontier.
func TestResetRunInheritsAnExternalCursorAsExternal(t *testing.T) {
	baseCtx := newStreamCursorTestContextForRun("base-run")
	base := &Workflow{}
	_, err := base.SubscribeToExternalStream(baseCtx, ExternalStreamSubscription{
		StreamID: "shared", StartOffset: 0, KnownHead: 7,
	})
	require.NoError(t, err)

	resetCtx := newStreamCursorTestContextForRun("reset-run")
	reset := &Workflow{}
	require.NoError(t, streamSubscribedEvent{}.Apply(resetCtx, reset, subscribedEvent("shared", 0)))
	require.NoError(t, reset.ApplyConsumedStreamRanges(resetCtx, []*streampb.StreamRange{
		{StreamId: "shared", FromOffset: 0, ToOffset: 3},
	}))

	require.NoError(t, reset.InheritStreamsOnReset(resetCtx, base, baseCtx, stream.DefaultLimits(), nil))

	cursor := reset.StreamCursors["shared"].Get(resetCtx)
	require.True(t, cursor.IsExternal())
	require.Equal(t, int64(3), cursor.Offset())
	require.Equal(t, int64(7), cursor.KnownHead(), "the frontier the base run last knew carries over")
	require.Nil(t, reset.OwnedStream(resetCtx, "shared"), "no stream of its own for an external one")
}

// A subscription made through the service leaves no event, so a reset run whose
// history records nothing for it is given the cursor from the base run, where
// that subscription began.
func TestResetRunCarriesASubscriptionTheEventsNeverMentioned(t *testing.T) {
	baseCtx := newStreamCursorTestContextForRun("base-run")
	base := &Workflow{}
	base.Streams = chasm.Map[string, *stream.Stream]{
		"inputs": chasm.NewComponentField(baseCtx, newAttachedStream(t, baseCtx, 4)),
	}
	_, err := base.SubscribeToOwnedStream(
		baseCtx, "inputs", stream.AtOffset(1), stream.DefaultLimits())
	require.NoError(t, err)

	resetCtx := newStreamCursorTestContextForRun("reset-run")
	reset := &Workflow{}
	require.NoError(t, reset.InheritStreamsOnReset(resetCtx, base, baseCtx, stream.DefaultLimits(), nil))

	cursor := reset.StreamCursors["inputs"].Get(resetCtx)
	require.Equal(t, int64(1), cursor.StartOffset())
	require.Equal(t, int64(1), cursor.Offset())
	state, err := reset.OwnedStream(resetCtx, "inputs").Snapshot(resetCtx, struct{}{})
	require.NoError(t, err)
	require.Equal(t, int64(1), state.GetBaseOffset())
}

// A publish command naming the part of the map activities use fails the task
// with a cause, instead of writing into a stream an activity owns.
func TestPublishCommandCannotReachAnActivityStream(t *testing.T) {
	ctx := newStreamBudgetTestContext()
	w := &Workflow{}
	publish := &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_APPEND_STREAM_RECORDS,
		Attributes: &commandpb.Command_AppendStreamRecordsCommandAttributes{
			AppendStreamRecordsCommandAttributes: &commandpb.AppendStreamRecordsCommandAttributes{
				StreamName: ActivityStreamKey("act", DefaultStreamName),
				Records:    []*streampb.StreamRecord{{Body: &commonpb.Payload{Data: []byte("x")}}},
			},
		},
	}
	err := handleAppendStreamRecordsCommand(ctx, w, allowAnySize{}, publish,
		CommandHandlerOptions{}, stream.DefaultLimits())
	var failTask FailWorkflowTaskError
	require.ErrorAs(t, err, &failTask)
	require.Equal(t,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_APPEND_STREAM_RECORDS_ATTRIBUTES, failTask.Cause)
	require.Empty(t, w.Streams)
}
