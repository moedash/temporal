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
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Path C: a workflow consuming a stream. The slice rides the workflow task and
// only the offsets it covered are written to History, so consumption costs no
// event of its own no matter how many messages it carried.
func TestStreamWorkflowConsumesWithoutHistoryPayloads(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	id := "stream-wf-consume-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	we, err := env.FrontendClient().StartWorkflowExecution(s.ctx(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.ns,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "stream-consumer"},
		TaskQueue:           tq,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            "tester",
	})
	require.NoError(t, err)

	// What each task was handed, in the order the tasks ran.
	var delivered [][]*streampb.StreamSlice
	task := 0

	//nolint:staticcheck // SA1019: only the deprecated poller can emit this command type.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(resp *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			delivered = append(delivered, resp.GetStreamSlices())
			task++
			if task > 1 {
				return nil, nil
			}
			// First task publishes; nothing is subscribed yet, so it consumes
			// nothing.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
				Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
					AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
						Messages: []*streampb.StreamMessage{
							{Body: &commonpb.Payload{Data: []byte("first-token")}, Topic: "tokens"},
							{Body: &commonpb.Payload{Data: []byte("second-token")}, Topic: "tokens"},
							{Body: &commonpb.Payload{Data: []byte("third-token")}, Topic: "tokens"},
						},
					},
				},
			}}, nil
		},
		Logger: env.Logger,
		T:      t,
	}

	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	require.Empty(t, delivered[0], "nothing is subscribed on the first task")

	// Subscribe from the start of the stream.
	sub, err := s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace:   s.ns,
			WorkflowId:  id,
			StreamName:  chasmworkflow.DefaultStreamName,
			StartOffset: 0,
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), sub.GetFrontendResponse().GetStartOffset())

	// Resubscribing must not rewind. Ranges below the cursor are already in
	// History, and moving back would replay them as if they were new.
	sub2, err := s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: id,
			StreamName: chasmworkflow.DefaultStreamName, StartOffset: 2,
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), sub2.GetFrontendResponse().GetStartOffset(),
		"a second subscribe must report the cursor already registered")

	// Second task: the published range should arrive on the task itself.
	signalWorkflow(t, s, id, we.GetRunId())
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	require.Len(t, delivered[1], 1, "the subscribed stream must be attached to the task")
	slice := delivered[1][0]
	require.Equal(t, int64(0), slice.GetFromOffset())
	require.Equal(t, int64(3), slice.GetToOffset())
	require.Len(t, slice.GetMessages(), 3)
	require.Equal(t, "first-token", string(slice.GetMessages()[0].GetBody().GetData()))
	require.Equal(t, "third-token", string(slice.GetMessages()[2].GetBody().GetData()))

	// Third task: the cursor is caught up, so the range is empty. An empty
	// range is still delivered and still recorded, because a task that saw
	// nothing is a fact replay has to reproduce.
	signalWorkflow(t, s, id, we.GetRunId())
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	require.Len(t, delivered[2], 1, "a caught-up subscription is still attached")
	require.Equal(t, int64(3), delivered[2][0].GetFromOffset())
	require.Equal(t, int64(3), delivered[2][0].GetToOffset())
	require.Empty(t, delivered[2][0].GetMessages())

	events := env.GetHistory(s.ns, &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.GetRunId()})
	recorded := recordedCursors(events)

	// One record per completed task from the subscription onward, including the
	// one that consumed nothing.
	require.Len(t, recorded, 2)
	require.Equal(t, int64(0), recorded[0].GetFromOffset())
	require.Equal(t, int64(3), recorded[0].GetToOffset())
	require.Equal(t, int64(3), recorded[1].GetFromOffset())
	require.Equal(t, int64(3), recorded[1].GetToOffset(),
		"the idle task must record the empty range rather than omit it")

	// The point of the design: offsets are in History, payloads are not.
	for _, e := range events {
		require.NotContains(t, e.String(), "first-token",
			"a consumed payload must never reach History, found in %v", e.GetEventType())
		require.NotContains(t, e.String(), "third-token",
			"a consumed payload must never reach History, found in %v", e.GetEventType())
	}
}

func recordedCursors(events []*historypb.HistoryEvent) []*streampb.StreamCursor {
	var out []*streampb.StreamCursor
	for _, e := range events {
		attrs := e.GetWorkflowTaskCompletedEventAttributes()
		out = append(out, attrs.GetStreamCursors()...)
	}
	return out
}

func signalWorkflow(t *testing.T, s *streamTestEnv, workflowID, runID string) {
	t.Helper()
	_, err := s.env.FrontendClient().SignalWorkflowExecution(s.ctx(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.ns,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		SignalName:        "wake",
		Identity:          "tester",
		RequestId:         uuid.NewString(),
	})
	require.NoError(t, err)
}

// Publishing never wakes a workflow, but an active subscription does. Without
// this the rest of a stream arrives only when something unrelated happens to
// schedule a task.
func TestStreamSubscriptionSchedulesItsOwnWorkflowTask(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	id := "stream-wf-wake-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	_, err := env.FrontendClient().StartWorkflowExecution(s.ctx(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.ns,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "stream-consumer"},
		TaskQueue:           tq,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            "tester",
	})
	require.NoError(t, err)

	publish := func(bodies ...string) []*commandpb.Command {
		messages := make([]*streampb.StreamMessage, len(bodies))
		for i, b := range bodies {
			messages[i] = &streampb.StreamMessage{Body: &commonpb.Payload{Data: []byte(b)}, Topic: "tokens"}
		}
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
			Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
				AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{Messages: messages},
			},
		}}
	}

	var delivered [][]*streampb.StreamSlice
	task := 0

	//nolint:staticcheck // SA1019: only the deprecated poller can emit this command type.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(resp *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			delivered = append(delivered, resp.GetStreamSlices())
			task++
			switch task {
			case 1:
				return publish("warmup"), nil
			case 2:
				// Published while subscribed and caught up, so completing this
				// task leaves the cursor behind head.
				return publish("alpha", "beta"), nil
			default:
				return nil, nil
			}
		},
		Logger: env.Logger,
		T:      t,
	}

	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	// Subscribe at the tail, so nothing is owed yet.
	sub, err := s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: id,
			StreamName: chasmworkflow.DefaultStreamName, StartOffset: -1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), sub.GetFrontendResponse().GetStartOffset())

	signalWorkflow(t, s, id, "")
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	require.Equal(t, int64(1), delivered[1][0].GetToOffset(), "nothing new at the tail yet")

	// No signal this time. The subscription owes two offsets, so completing the
	// previous task has to have scheduled this one.
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	require.Len(t, delivered, 3)
	require.Equal(t, int64(1), delivered[2][0].GetFromOffset())
	require.Equal(t, int64(3), delivered[2][0].GetToOffset())
	require.Len(t, delivered[2][0].GetMessages(), 2)
	require.Equal(t, "alpha", string(delivered[2][0].GetMessages()[0].GetBody().GetData()))

	// And it has to stop. A wake condition that stayed true would spin the
	// workflow on empty tasks forever.
	pollCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	idle, err := env.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
	})
	if err == nil {
		require.Empty(t, idle.GetTaskToken(),
			"a caught-up subscription must not keep scheduling tasks")
	}
}

// Subscribing to a stream that already has data is the other case where the
// workflow is owed a task it did not ask for by any other means.
func TestSubscribingToABacklogSchedulesAWorkflowTask(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	id := "stream-wf-backlog-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	_, err := env.FrontendClient().StartWorkflowExecution(s.ctx(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.ns,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "stream-consumer"},
		TaskQueue:           tq,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            "tester",
	})
	require.NoError(t, err)

	var delivered [][]*streampb.StreamSlice
	task := 0

	//nolint:staticcheck // SA1019: only the deprecated poller can emit this command type.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(resp *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			delivered = append(delivered, resp.GetStreamSlices())
			task++
			if task > 1 {
				return nil, nil
			}
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
				Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
					AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
						Messages: []*streampb.StreamMessage{
							{Body: &commonpb.Payload{Data: []byte("backlog-1")}, Topic: "tokens"},
							{Body: &commonpb.Payload{Data: []byte("backlog-2")}, Topic: "tokens"},
						},
					},
				},
			}}, nil
		},
		Logger: env.Logger,
		T:      t,
	}

	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	_, err = s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: id,
			StreamName: chasmworkflow.DefaultStreamName, StartOffset: 0,
		},
	})
	require.NoError(t, err)

	// No signal. Subscribing behind the head is itself a reason to run.
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	require.Len(t, delivered, 2)
	require.Equal(t, int64(0), delivered[1][0].GetFromOffset())
	require.Equal(t, int64(2), delivered[1][0].GetToOffset())
}
