package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// startConsumer starts a workflow on its own task queue and returns the
// execution and the queue.
func startConsumer(
	t *testing.T, s *streamTestEnv, prefix string,
) (*commonpb.WorkflowExecution, *taskqueuepb.TaskQueue) {
	t.Helper()
	id := prefix + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	we, err := s.env.FrontendClient().StartWorkflowExecution(
		s.ctx(),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.ns,
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: "stream-consumer"},
			TaskQueue:           tq,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            "tester",
		},
	)
	require.NoError(t, err)
	return &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.GetRunId()}, tq
}

func publishCommand(bodies ...string) []*commandpb.Command {
	messages := make([]*streampb.StreamMessage, len(bodies))
	for i, b := range bodies {
		messages[i] = &streampb.StreamMessage{Body: &commonpb.Payload{Data: []byte(b)}, Topic: "tokens"}
	}
	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
		Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
			AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
				Messages: messages,
			},
		},
	}}
}

func apiBodies(msgs []*streampb.StreamMessage) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.GetBody().GetData())
	}
	return out
}

// completedEventsWithCursors returns the ids of every WorkflowTaskCompleted
// event that recorded a consumed range, empty ones included.
func completedEventsWithCursors(events []*historypb.HistoryEvent) []int64 {
	var out []int64
	for _, e := range events {
		if len(e.GetWorkflowTaskCompletedEventAttributes().GetStreamCursors()) > 0 {
			out = append(out, e.GetEventId())
		}
	}
	return out
}

// The response to a started task carries one page of history. A consumer that
// has run longer than a page recorded its ranges on the pages after it, and a
// cold replay has to find every one of them or the worker replays against less
// than the first run saw.
func TestReplayFollowsHistoryPastTheFirstPage(t *testing.T) {
	env := testcore.NewEnv(t,
		testcore.WithDedicatedCluster(),
		testcore.WithDynamicConfig(dynamicconfig.HistoryMaxPageSize, 4),
	)
	s := newStreamTestEnvFrom(t, env)
	execution, tq := startConsumer(t, s, "stream-wf-paged-replay-")

	var delivered [][]*streampb.StreamSlice
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
			if len(delivered) == 1 {
				return publishCommand("page-1", "page-2"), nil
			}
			return nil, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err := poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	_, err = s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(),
			StreamName: chasmworkflow.DefaultStreamName, StartOffset: 0,
		},
	})
	require.NoError(t, err)

	// One task consumes the range, then several idle tasks push the event that
	// recorded it well behind the first page of history.
	for range 4 {
		signalWorkflow(t, s, execution.GetWorkflowId(), execution.GetRunId())
		_, err = poller.PollAndProcessWorkflowTask()
		require.NoError(t, err)
	}
	require.Len(t, currentSlice(t, delivered[1]).GetMessages(), 2)

	events := env.GetHistory(s.ns, execution)
	recordedAt := completedEventsWithCursors(events)
	require.Len(t, recordedAt, 4, "every task since the subscription recorded a range")
	consumedAt := completedEventWithCursors(t, events)
	require.Greater(t, len(events), 8,
		"the history has to span more than one page for this to mean anything")

	env.CloseShard(env.NamespaceID().String(), execution.GetWorkflowId())
	signalWorkflow(t, s, execution.GetWorkflowId(), execution.GetRunId())
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	replay := delivered[len(delivered)-1]
	replayed := sliceForEvent(replay, consumedAt)
	require.NotNil(t, replayed, "the range recorded on a later page must be re-supplied")
	require.Equal(t, []string{"page-1", "page-2"}, apiBodies(replayed.GetMessages()))
	require.Equal(t, execution.GetRunId(), replayed.GetRunId(),
		"an owned stream's slice names the run that holds it")
	for _, eventID := range recordedAt {
		require.NotNil(t, sliceForEvent(replay, eventID),
			"every recorded range, empty ones included, has to travel with its event")
	}
}

// pollInBackground keeps a poller on the queue until the returned stop
// function is called. Used where a task is expected never to reach the worker,
// because matching only asks history to start a task while someone polls.
func pollInBackground(
	t *testing.T, env *testcore.TestEnv, ns string, tq *taskqueuepb.TaskQueue,
) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
			_, _ = env.FrontendClient().PollWorkflowTaskQueue(
				pollCtx,
				&workflowservice.PollWorkflowTaskQueueRequest{
					Namespace: ns, TaskQueue: tq, Identity: "tester",
				},
			)
			pollCancel()
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func workflowTaskFailedWith(
	events []*historypb.HistoryEvent,
	cause enumspb.WorkflowTaskFailedCause,
) *historypb.WorkflowTaskFailedEventAttributes {
	for _, e := range events {
		if attrs := e.GetWorkflowTaskFailedEventAttributes(); attrs != nil && attrs.GetCause() == cause {
			return attrs
		}
	}
	return nil
}

// deleteStreamAndWait removes a standalone stream and waits until reads of it
// fail, since deletion completes in a task after the call returns.
func deleteStreamAndWait(t *testing.T, s *streamTestEnv, streamID string) {
	t.Helper()
	_, err := s.client.DeleteStream(s.ctx(), &streamlib.DeleteStreamRequest{
		FrontendRequest: &streamlib.DeleteStreamInput{
			Namespace: s.ns, StreamId: streamID, Force: true,
		},
	})
	require.NoError(t, err)
	await.RequireTrue(t, func() bool {
		_, err := s.client.DescribeStream(s.ctx(), &streamlib.DescribeStreamRequest{
			FrontendRequest: &streamlib.DescribeStreamInput{Namespace: s.ns, StreamId: streamID},
		})
		return err != nil
	}, 20*time.Second, 100*time.Millisecond)
}

// A consumer whose stream is gone before its next task starts cannot be handed
// the range it is due. That has to land in History as a failed task with a
// cause, not as a request matching retries forever.
func TestConsumerOfADeletedStreamFailsItsTaskLoudly(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	streamID := "gone-live-" + uuid.NewString()
	s.create(s.ctx(), t, streamID)
	execution, tq := startConsumer(t, s, "stream-wf-gone-live-")

	//nolint:staticcheck // SA1019: consistent with the other stream tests.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			return nil, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err := poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	_, err = s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(), StreamId: streamID, StartOffset: 0,
		},
	})
	require.NoError(t, err)

	// The append schedules the task that is owed the range.
	_, err = s.client.AddMessages(s.ctx(), &streamlib.AddMessagesRequest{
		FrontendRequest: &streamlib.AddMessagesInput{
			Namespace: s.ns, StreamId: streamID, Messages: streamMsgs("tokens", "never-delivered"),
		},
	})
	require.NoError(t, err)
	await.RequireTrue(t, func() bool {
		scheduled := 0
		for _, e := range env.GetHistory(s.ns, execution) {
			if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				scheduled++
			}
		}
		return scheduled >= 2
	}, 20*time.Second, 100*time.Millisecond)

	deleteStreamAndWait(t, s, streamID)

	stop := pollInBackground(t, env, s.ns, tq)
	defer stop()
	await.RequireTrue(t, func() bool {
		return workflowTaskFailedWith(env.GetHistory(s.ns, execution),
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_STREAM_RANGE_UNAVAILABLE) != nil
	}, 30*time.Second, 200*time.Millisecond)

	failed := workflowTaskFailedWith(env.GetHistory(s.ns, execution),
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_STREAM_RANGE_UNAVAILABLE)
	require.Contains(t, failed.GetFailure().GetMessage(), streamID,
		"the failure names the stream so an operator knows where to look")
}

// A consumer whose recorded range can no longer be re-read cannot replay. The
// replay runs after the task started, so the failure is a transaction of its
// own, and it still has to end up in History with the cause.
func TestReplayOfADeletedStreamFailsTheTaskLoudly(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
	s := newStreamTestEnvFrom(t, env)

	streamID := "gone-replay-" + uuid.NewString()
	s.create(s.ctx(), t, streamID)
	execution, tq := startConsumer(t, s, "stream-wf-gone-replay-")

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
	_, err := poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	_, err = s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(), StreamId: streamID, StartOffset: 0,
		},
	})
	require.NoError(t, err)
	_, err = s.client.AddMessages(s.ctx(), &streamlib.AddMessagesRequest{
		FrontendRequest: &streamlib.AddMessagesInput{
			Namespace: s.ns, StreamId: streamID, Messages: streamMsgs("tokens", "consumed-once"),
		},
	})
	require.NoError(t, err)

	// Consumed and recorded, which is what a replay will have to reproduce.
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	require.Len(t, currentSlice(t, delivered[1]).GetMessages(), 1)

	deleteStreamAndWait(t, s, streamID)

	env.CloseShard(env.NamespaceID().String(), execution.GetWorkflowId())
	signalWorkflow(t, s, execution.GetWorkflowId(), execution.GetRunId())

	stop := pollInBackground(t, env, s.ns, tq)
	defer stop()
	await.RequireTrue(t, func() bool {
		return workflowTaskFailedWith(env.GetHistory(s.ns, execution),
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_STREAM_RANGE_UNAVAILABLE) != nil
	}, 30*time.Second, 200*time.Millisecond)

	failed := workflowTaskFailedWith(env.GetHistory(s.ns, execution),
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_STREAM_RANGE_UNAVAILABLE)
	require.Contains(t, failed.GetFailure().GetMessage(), "cannot replay")
	require.Contains(t, failed.GetFailure().GetMessage(), streamID)
}
