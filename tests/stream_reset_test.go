package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// nthCompletedEvent returns the id of the n-th WorkflowTaskCompleted event,
// counting from one.
func nthCompletedEvent(t *testing.T, events []*historypb.HistoryEvent, n int) int64 {
	t.Helper()
	seen := 0
	for _, e := range events {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			seen++
			if seen == n {
				return e.GetEventId()
			}
		}
	}
	t.Fatalf("history has %d completed tasks, wanted %d", seen, n)
	return 0
}

func resetTo(
	t *testing.T, s *streamTestEnv, execution *commonpb.WorkflowExecution, eventID int64,
) string {
	t.Helper()
	resp, err := s.env.FrontendClient().ResetWorkflowExecution(s.ctx(),
		&workflowservice.ResetWorkflowExecutionRequest{
			Namespace:                 s.ns,
			WorkflowExecution:         execution,
			Reason:                    "stream reset test",
			WorkflowTaskFinishEventId: eventID,
			RequestId:                 uuid.NewString(),
		})
	require.NoError(t, err)
	return resp.GetRunId()
}

// A reset copies the base run's history into a new run, and that history
// records ranges the base run consumed from a stream it owned. The reset run
// replays them from the base run's stream, with each slice naming that run,
// and from the reset point on it consumes and publishes on a stream of its own
// that continues the offset space where the inherited cursor stood.
func TestResetReplaysTheBaseRunsRangesAndGoesOnWithItsOwnStream(t *testing.T) {
	// Dedicated, because the cold replay at the end evicts the cached
	// workflow context through CloseShard.
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
	s := newStreamTestEnvFrom(t, env)
	execution, tq := startConsumer(t, s, "stream-wf-reset-")

	var delivered [][]*streampb.StreamSlice
	var commands [][]*commandpb.Command
	//nolint:staticcheck // SA1019: only the deprecated poller can emit this command type.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			resp *workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			delivered = append(delivered, resp.GetStreamSlices())
			next := commands[0]
			commands = commands[1:]
			return next, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	runTask := func(cmds []*commandpb.Command) []*streampb.StreamSlice {
		t.Helper()
		commands = append(commands, cmds)
		_, err := poller.PollAndProcessWorkflowTask()
		require.NoError(t, err)
		return delivered[len(delivered)-1]
	}

	// Base run: publish two, subscribe, consume them, publish a third, consume it.
	runTask(publishCommand("before-1", "before-2"))
	_, err := s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(),
			StreamName: chasmworkflow.DefaultStreamName, StartOffset: 0,
		},
	})
	require.NoError(t, err)
	consumed := currentSlice(t, runTask(nil))
	require.Equal(t, []string{"before-1", "before-2"}, apiBodies(consumed.GetRecords()))

	signalWorkflow(t, s, execution.GetWorkflowId(), execution.GetRunId())
	runTask(publishCommand("before-3"))
	third := currentSlice(t, runTask(nil))
	require.Equal(t, []string{"before-3"}, apiBodies(third.GetRecords()))

	baseEvents := env.GetHistory(s.ns, execution)
	consumedAt := completedEventWithCursors(t, baseEvents)

	// Reset to the third task: the reset run keeps the first two completions,
	// so its cursor stands at offset 2 and nothing of the third publish is its.
	resetRunID := resetTo(t, s, execution, nthCompletedEvent(t, baseEvents, 3))
	resetRun := &commonpb.WorkflowExecution{WorkflowId: execution.GetWorkflowId(), RunId: resetRunID}

	// The reset run's first task replays the range recorded before the reset
	// point from the base run's stream, and consumes nothing new of its own.
	first := runTask(nil)
	replayed := sliceForEvent(first, consumedAt)
	require.NotNil(t, replayed, "the copied completion at event %d is re-supplied", consumedAt)
	require.Equal(t, execution.GetRunId(), replayed.GetRunId(),
		"a range recorded before the reset point is read from the base run")
	require.Equal(t, int64(0), replayed.GetFromOffset())
	require.Equal(t, int64(2), replayed.GetToOffset())
	require.Equal(t, []string{"before-1", "before-2"}, apiBodies(replayed.GetRecords()))
	live := currentSlice(t, first)
	require.Equal(t, resetRunID, live.GetRunId(),
		"from the reset point on the stream is the reset run's")
	require.Equal(t, int64(2), live.GetFromOffset(), "the inherited cursor keeps its position")
	require.Equal(t, int64(2), live.GetToOffset())

	// The reset run publishes and consumes on its own stream, whose offsets
	// continue from where the cursor stood.
	signalWorkflow(t, s, execution.GetWorkflowId(), resetRunID)
	runTask(publishCommand("after-1"))
	own := currentSlice(t, runTask(nil))
	require.Equal(t, resetRunID, own.GetRunId())
	require.Equal(t, int64(2), own.GetFromOffset())
	require.Equal(t, int64(3), own.GetToOffset())
	require.Equal(t, []string{"after-1"}, apiBodies(own.GetRecords()))

	// Seen from outside: the base run still holds every record its history
	// refers to, and the reset run's stream starts at the inherited offset.
	basePoll, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(),
			OwnerRunId: execution.GetRunId(), FromOffset: 0,
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"before-1", "before-2", "before-3"},
		bodies(basePoll.GetFrontendResponse().GetRecords()))

	resetPoll, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(),
			OwnerRunId: resetRunID, FromOffset: 2,
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"after-1"}, bodies(resetPoll.GetFrontendResponse().GetRecords()))
	require.Equal(t, []int64{2}, offsets(resetPoll.GetFrontendResponse().GetRecords()))

	_, err = s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(),
			OwnerRunId: resetRunID, FromOffset: 0,
		},
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err),
		"a reader following the chain into the reset run has to start at the offset it begins at")

	// A cold replay of the reset run re-supplies both eras from the runs that
	// hold them, tagged with the completions that recorded them.
	env.CloseShard(env.NamespaceID().String(), execution.GetWorkflowId())
	signalWorkflow(t, s, execution.GetWorkflowId(), resetRunID)
	cold := runTask(nil)
	fromBase := sliceForEvent(cold, consumedAt)
	require.NotNil(t, fromBase)
	require.Equal(t, execution.GetRunId(), fromBase.GetRunId())
	require.Equal(t, []string{"before-1", "before-2"}, apiBodies(fromBase.GetRecords()))
	ownAt := completedEventWithCursorsAfter(t, env.GetHistory(s.ns, resetRun), consumedAt)
	fromReset := sliceForEvent(cold, ownAt)
	require.NotNil(t, fromReset)
	require.Equal(t, resetRunID, fromReset.GetRunId())
	require.Equal(t, int64(2), fromReset.GetFromOffset())
	require.Equal(t, int64(3), fromReset.GetToOffset())
	require.Equal(t, []string{"after-1"}, apiBodies(fromReset.GetRecords()))

	// The base run names its successor, which is how a client following the
	// chain finds the reset run.
	described, err := env.FrontendClient().DescribeWorkflowExecution(s.ctx(),
		&workflowservice.DescribeWorkflowExecutionRequest{Namespace: s.ns, Execution: execution})
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		described.GetWorkflowExecutionInfo().GetStatus())
	require.Equal(t, resetRunID, described.GetWorkflowExtendedInfo().GetResetRunId())
}

// completedEventWithCursorsAfter returns the id of the first WorkflowTaskCompleted
// event after the given one that recorded a non-empty consumed range.
func completedEventWithCursorsAfter(
	t *testing.T, events []*historypb.HistoryEvent, after int64,
) int64 {
	t.Helper()
	for _, e := range events {
		if e.GetEventId() <= after {
			continue
		}
		for _, c := range e.GetWorkflowTaskCompletedEventAttributes().GetConsumedStreamRanges() {
			if c.GetToOffset() > c.GetFromOffset() {
				return e.GetEventId()
			}
		}
	}
	t.Fatal("no completed event after the given one recorded a consumed range")
	return 0
}

// A reset terminates the base run. Its pin on a stream in another execution is
// released the way a closed run's is, when the stream next tells its consumers
// the frontier moved and finds the run over, and the reset run, which carries
// the subscription on, takes the pin over with the floor its cursor began at.
func TestResetHandsTheExternalPinToTheResetRun(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	streamID := "reset-shared-stream-" + uuid.NewString()
	s.create(s.ctx(), t, streamID)
	execution, tq := startConsumer(t, s, "stream-wf-reset-ext-")

	var delivered [][]*streampb.StreamSlice
	//nolint:staticcheck // SA1019: consistent with the other stream tests.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			resp *workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			delivered = append(delivered, resp.GetStreamSlices())
			return nil, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	runTask := func() []*streampb.StreamSlice {
		t.Helper()
		_, err := poller.PollAndProcessWorkflowTask()
		require.NoError(t, err)
		return delivered[len(delivered)-1]
	}

	runTask()
	_, err := s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(), StreamId: streamID, StartOffset: 0,
		},
	})
	require.NoError(t, err)
	_, err = s.add(s.ctx(), t, streamID,
		&streamlib.AddMessagesInput{Records: streamMsgs("t", "one", "two")})
	require.NoError(t, err)
	consumed := currentSlice(t, runTask())
	require.Equal(t, int64(2), consumed.GetToOffset())

	signalWorkflow(t, s, execution.GetWorkflowId(), execution.GetRunId())
	runTask()
	baseEvents := env.GetHistory(s.ns, execution)
	consumedAt := completedEventWithCursors(t, baseEvents)

	resetRunID := resetTo(t, s, execution, nthCompletedEvent(t, baseEvents, 3))

	// The reset run replays the base run's range from the standalone stream
	// and stands at the same offset.
	first := runTask()
	replayed := sliceForEvent(first, consumedAt)
	require.NotNil(t, replayed)
	require.Equal(t, []string{"one", "two"}, apiBodies(replayed.GetRecords()))
	require.Equal(t, int64(2), currentSlice(t, first).GetFromOffset())

	// The pin is still the base run's until the stream next notifies.
	before := describeStream(t, s, streamID)
	require.Len(t, before.GetConsumers(), 1)
	for _, consumer := range before.GetConsumers() {
		require.Equal(t, execution.GetRunId(), consumer.GetRunId())
	}

	// An append pushes the frontier at the pinned run, finds it terminated, and
	// hands the pin to the run that now carries the subscription.
	_, err = s.add(s.ctx(), t, streamID,
		&streamlib.AddMessagesInput{Records: streamMsgs("t", "three")})
	require.NoError(t, err)
	afterReset := currentSlice(t, runTask())
	require.Equal(t, int64(2), afterReset.GetFromOffset(),
		"the reset run resumes where the base stopped")
	require.Equal(t, int64(3), afterReset.GetToOffset())
	require.Equal(t, []string{"three"}, apiBodies(afterReset.GetRecords()))

	after := describeStream(t, s, streamID)
	require.Len(t, after.GetConsumers(), 1, "one pin, keyed to the run that consumes")
	for _, consumer := range after.GetConsumers() {
		require.Equal(t, resetRunID, consumer.GetRunId())
		require.Equal(t, int64(0), consumer.GetReplayFloor(),
			"the floor is where the inherited subscription began, which its replay depends on")
	}
}
