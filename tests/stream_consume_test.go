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
	"go.temporal.io/server/common/testing/await"
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

	slice := currentSlice(t, delivered[1])
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

	idleSlice := currentSlice(t, delivered[2])
	require.Equal(t, int64(3), idleSlice.GetFromOffset(), "a caught-up subscription is still attached")
	require.Equal(t, int64(3), idleSlice.GetToOffset())
	require.Empty(t, idleSlice.GetMessages())

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
	require.Equal(t, int64(1), currentSlice(t, delivered[1]).GetToOffset(), "nothing new at the tail yet")

	// No signal this time. The subscription owes two offsets, so completing the
	// previous task has to have scheduled this one.
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	require.Len(t, delivered, 3)
	woken := currentSlice(t, delivered[2])
	require.Equal(t, int64(1), woken.GetFromOffset())
	require.Equal(t, int64(3), woken.GetToOffset())
	require.Len(t, woken.GetMessages(), 2)
	require.Equal(t, "alpha", string(woken.GetMessages()[0].GetBody().GetData()))

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
	backlog := currentSlice(t, delivered[1])
	require.Equal(t, int64(0), backlog.GetFromOffset())
	require.Equal(t, int64(2), backlog.GetToOffset())
}

// History records the offsets a task consumed and never the payloads, so a
// worker replaying that task has to be handed the bytes again. The response
// field alone cannot do it: it is built once per delivery while a cache miss
// replays every prior task, so each re-supplied range travels with the id of
// the event that recorded it.
func TestReplayGetsTheConsumedRangesBackFromTheStream(t *testing.T) {
	// Dedicated, because forcing the replay path means evicting the cached
	// workflow context, and CloseShard is not allowed on a shared cluster.
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
	s := newStreamTestEnvFrom(t, env)

	id := "stream-wf-replay-" + uuid.NewString()
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
							{Body: &commonpb.Payload{Data: []byte("replay-me-1")}, Topic: "tokens"},
							{Body: &commonpb.Payload{Data: []byte("replay-me-2")}, Topic: "tokens"},
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

	// Consume the range. This is the task replay will have to reproduce.
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	require.Len(t, currentSlice(t, delivered[1]).GetMessages(), 2)

	consumedAt := completedEventWithCursors(t, env.GetHistory(s.ns,
		&commonpb.WorkflowExecution{WorkflowId: id, RunId: we.GetRunId()}))

	// Drop the cached context so the next task is served with full history,
	// which is the replay path.
	env.CloseShard(env.NamespaceID().String(), id)

	signalWorkflow(t, s, id, we.GetRunId())
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	replayed := sliceForEvent(delivered[2], consumedAt)
	require.NotNil(t, replayed, "the replayed task must carry the range recorded at event %d", consumedAt)
	require.Equal(t, int64(0), replayed.GetFromOffset())
	require.Equal(t, int64(2), replayed.GetToOffset())
	require.Len(t, replayed.GetMessages(), 2, "the payloads have to come back from the stream")
	require.Equal(t, "replay-me-1", string(replayed.GetMessages()[0].GetBody().GetData()))
	require.Equal(t, "replay-me-2", string(replayed.GetMessages()[1].GetBody().GetData()))
}

// completedEventWithCursors returns the id of the first WorkflowTaskCompleted
// event that recorded a non-empty consumed range.
func completedEventWithCursors(t *testing.T, events []*historypb.HistoryEvent) int64 {
	t.Helper()
	for _, e := range events {
		for _, c := range e.GetWorkflowTaskCompletedEventAttributes().GetStreamCursors() {
			if c.GetToOffset() > c.GetFromOffset() {
				return e.GetEventId()
			}
		}
	}
	t.Fatal("no completed event recorded a consumed range")
	return 0
}

// currentSlice picks the slice for the task about to run. Slices carrying an
// event id belong to tasks being replayed, and a response can hold both.
func currentSlice(t *testing.T, slices []*streampb.StreamSlice) *streampb.StreamSlice {
	t.Helper()
	for _, s := range slices {
		if s.GetWorkflowTaskCompletedEventId() == 0 {
			return s
		}
	}
	t.Fatal("no slice for the current task")
	return nil
}

func sliceForEvent(slices []*streampb.StreamSlice, eventID int64) *streampb.StreamSlice {
	for _, s := range slices {
		if s.GetWorkflowTaskCompletedEventId() == eventID {
			return s
		}
	}
	return nil
}

// The shape the feature exists for: a producer writing off-shard and a workflow
// consuming it, with no relationship between them beyond the stream.
//
// A consuming workflow cannot read another execution's frontier while closing
// its own transaction, so the stream pushes it. The push dirties the consumer,
// whose transaction close then sees it is behind and schedules a workflow task.
//
// The log is read from the stream's own shard rather than the consumer's.
// History nodes are stored per shard, so reading an external stream from the
// consumer's shard finds nothing, and the workflow task then fails to start
// and is dropped rather than reporting anything useful.
func TestWorkflowConsumesAStreamItDoesNotOwn(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	streamID := "external-stream-" + uuid.NewString()
	s.create(s.ctx(), t, streamID)

	id := "stream-wf-external-" + uuid.NewString()
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

	var delivered [][]*streampb.StreamSlice

	//nolint:staticcheck // SA1019: consistent with the other stream tests.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(resp *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			delivered = append(delivered, resp.GetStreamSlices())
			return nil, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	_, err = s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: id,
			StreamId: streamID, StartOffset: 0,
		},
	})
	require.NoError(t, err)

	// The subscription has to reach the stream, or truncation could take a
	// range the cursor still points at.
	desc, err := s.client.DescribeStream(s.ctx(), &streamlib.DescribeStreamRequest{
		FrontendRequest: &streamlib.DescribeStreamInput{Namespace: s.ns, StreamId: streamID},
	})
	require.NoError(t, err)
	consumers := desc.GetFrontendResponse().GetState().GetConsumers()
	require.Len(t, consumers, 1)
	for _, c := range consumers {
		require.True(t, c.GetExternal())
		require.True(t, c.GetActive())
		require.Equal(t, id, c.GetWorkflowId())
	}

	// An off-shard producer, unrelated to the workflow.
	_, err = s.client.AddMessages(s.ctx(), &streamlib.AddMessagesRequest{
		FrontendRequest: &streamlib.AddMessagesInput{
			Namespace: s.ns, StreamId: streamID,
			Messages: []*streamlib.StreamMessage{
				{Body: &commonpb.Payload{Data: []byte("from-outside-1")}, Kind: streamlib.STREAM_MESSAGE_KIND_DATA},
				{Body: &commonpb.Payload{Data: []byte("from-outside-2")}, Kind: streamlib.STREAM_MESSAGE_KIND_DATA},
			},
		},
	})
	require.NoError(t, err)

	// The append alone has to produce a workflow task, with no signal and no
	// other traffic against the workflow.
	await.RequireTruef(t, func() bool {
		events := env.GetHistory(s.ns, &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.GetRunId()})
		scheduled := 0
		for _, e := range events {
			if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				scheduled++
			}
		}
		return scheduled >= 2
	}, 20*time.Second, 200*time.Millisecond, "the append must schedule a workflow task on its own")

	// And it must reach a worker.
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	got := currentSlice(t, delivered[1])
	require.Equal(t, streamID, got.GetStreamId())
	require.Equal(t, int64(0), got.GetFromOffset())
	require.Equal(t, int64(2), got.GetToOffset())
	require.Len(t, got.GetMessages(), 2)
	require.Equal(t, "from-outside-1", string(got.GetMessages()[0].GetBody().GetData()))
}
