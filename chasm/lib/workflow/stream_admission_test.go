package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

func newStreamBudgetTestContext() chasm.MutableContext {
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

func budgetTestRecords(bytes int) []*streamlib.StreamRecord {
	return []*streamlib.StreamRecord{{
		Body: &commonpb.Payload{Data: make([]byte, bytes)},
		Kind: streampb.STREAM_RECORD_KIND_DATA,
	}}
}

// The per-stream budget bounds one stream, and an outside writer can name as
// many as the stream count allows. Multiplied out they come to far more than
// the execution size limit, which terminates the workflow rather than refusing
// an append, so the sum is what the append is measured against.
func TestOwnedStreamsShareOneByteBudget(t *testing.T) {
	ctx := newStreamBudgetTestContext()
	w := &Workflow{}
	limits := stream.Limits{
		MaxOwnedStreamsPerWorkflow:      10,
		OwnedStreamMaxItems:             100,
		OwnedStreamMaxBytes:             4096,
		OwnedStreamsMaxBytesPerWorkflow: 4096,
	}

	// Three streams of a thousand bytes each sit inside every per-stream
	// budget and inside the shared one.
	for _, name := range []string{"a", "b", "c"} {
		_, err := w.AppendToOwnedStream(ctx, name, stream.AddMessagesRequest{
			Records: budgetTestRecords(1000),
			Limits:  limits,
		})
		require.NoError(t, err, "stream %q", name)
	}

	// The fourth still fits its own budget and no longer fits the shared one.
	_, err := w.AppendToOwnedStream(ctx, "d", stream.AddMessagesRequest{
		Records: budgetTestRecords(1500),
		Limits:  limits,
	})
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)

	// The refusal is about the shared budget, so a small append still lands.
	_, err = w.AppendToOwnedStream(ctx, "d", stream.AddMessagesRequest{
		Records: budgetTestRecords(100),
		Limits:  limits,
	})
	require.NoError(t, err)
}

// A stream the workflow owns is keyed by its name and one in another execution
// by its id, in one map. Where the two collide the subscription is refused:
// handing back the owned cursor reports a subscription that never delivers,
// and the pin taken on the standalone stream would be held by nothing.
func TestSubscriptionsOfBothOriginsCannotShareOneKey(t *testing.T) {
	ctx := newStreamBudgetTestContext()
	limits := stream.Limits{MaxOwnedStreamsPerWorkflow: 10, OwnedStreamMaxItems: 100}

	owned := &Workflow{}
	_, err := owned.SubscribeToOwnedStream(ctx, "x", 0, limits)
	require.NoError(t, err)
	_, err = owned.SubscribeToExternalStream(ctx, ExternalStreamSubscription{
		StreamID: "x", StartOffset: 7, KnownHead: 9,
	})
	var refused *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &refused)

	external := &Workflow{}
	_, err = external.SubscribeToExternalStream(ctx, ExternalStreamSubscription{
		StreamID: "x", StartOffset: 7, KnownHead: 9,
	})
	require.NoError(t, err)
	_, err = external.SubscribeToOwnedStream(ctx, "x", 0, limits)
	require.ErrorAs(t, err, &refused)
}

// A push carries the frontier of a stream in another execution. A cursor of
// the same key on a stream the workflow owns is a different stream that
// happens to share the name, and moving its frontier would answer for data it
// is not reading.
func TestKnownHeadIsOnlyPushedIntoAnExternalCursor(t *testing.T) {
	ctx := newStreamBudgetTestContext()
	w := &Workflow{}
	_, err := w.SubscribeToOwnedStream(
		ctx, "x", 0, stream.Limits{MaxOwnedStreamsPerWorkflow: 10, OwnedStreamMaxItems: 100})
	require.NoError(t, err)

	err = w.AdvanceKnownHead(ctx, "x", 42)
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
}

// Each subscription costs a routed call on the completion path that made it,
// with this execution's lock held, and another on every task start. Nothing
// else bounds how many a workflow may hold or how many one task may carry.
func TestSubscriptionsPerWorkflowAreBounded(t *testing.T) {
	ctx := newStreamBudgetTestContext()
	backend := &chasm.MockNodeBackend{
		HandleAddHistoryEvent: func(
			eventType enumspb.EventType, set func(*historypb.HistoryEvent),
		) *historypb.HistoryEvent {
			e := &historypb.HistoryEvent{EventType: eventType}
			set(e)
			return e
		},
	}
	w := &Workflow{MSPointer: chasm.NewMSPointer(backend)}
	opts := CommandHandlerOptions{WorkflowTaskCompletedEventID: 10}
	limits := stream.Limits{MaxSubscriptionsPerWorkflow: 2}

	subscribe := func(id string) error {
		return handleSubscribeStreamCommand(ctx, w, nil, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SUBSCRIBE_STREAM,
			Attributes: &commandpb.Command_SubscribeStreamCommandAttributes{
				SubscribeStreamCommandAttributes: &commandpb.SubscribeStreamCommandAttributes{
					StreamId: id,
				},
			},
		}, opts, limits)
	}

	require.NoError(t, subscribe("a"))
	require.NoError(t, subscribe("b"))

	// Counted against what this task has already staged, so one task cannot
	// carry an unbounded set of them either.
	err := subscribe("c")
	var failTask FailWorkflowTaskError
	require.ErrorAs(t, err, &failTask)
	require.Equal(t,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SUBSCRIBE_STREAM_ATTRIBUTES, failTask.Cause)

	// Subscribing again to one already staged registers nothing new, so it is
	// not refused for room.
	require.NoError(t, subscribe("a"))
}
