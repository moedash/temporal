package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	chasmstream "go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Streams owned by activities. A standalone activity is an execution of its
// own and owns a map of named streams the way a workflow does; an activity a
// workflow scheduled keeps its streams in the workflow's map and is addressed
// through the workflow. Both are written and read from outside over the
// owned-stream calls, with an owner reference naming which execution.

func newStreamActivityEnv(t *testing.T) (*testcore.TestEnv, *streamTestEnv) {
	env := testcore.NewEnv(t)
	on := []dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
		Value:       true,
	}}
	cluster := env.GetTestCluster()
	cluster.OverrideDynamicConfig(t, dynamicconfig.EnableChasm, on)
	cluster.OverrideDynamicConfig(t, activity.Enabled, on)
	return env, newStreamTestEnvFrom(t, env)
}

func (s *streamTestEnv) addOwned(
	t *testing.T, owner *streamlib.StreamOwner, name string, in *streamlib.AddWorkflowMessagesInput,
) (*streamlib.AddMessagesOutput, error) {
	t.Helper()
	in.Namespace, in.Owner, in.StreamName = s.ns, owner, name
	resp, err := s.client.AddWorkflowMessages(s.ctx(), &streamlib.AddWorkflowMessagesRequest{
		FrontendRequest: in,
	})
	return resp.GetFrontendResponse(), err
}

func (s *streamTestEnv) pollOwned(
	t *testing.T, owner *streamlib.StreamOwner, name string, from int64, wait bool,
) *streamlib.PollMessagesOutput {
	t.Helper()
	resp, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, Owner: owner, StreamName: name, FromOffset: from,
			WaitNewMessages: wait,
		},
	})
	require.NoError(t, err)
	return resp.GetFrontendResponse()
}

func (s *streamTestEnv) describeOwned(
	t *testing.T, owner *streamlib.StreamOwner, name string,
) *streamlib.StreamState {
	t.Helper()
	resp, err := s.client.DescribeWorkflowStream(s.ctx(), &streamlib.DescribeWorkflowStreamRequest{
		FrontendRequest: &streamlib.DescribeWorkflowStreamInput{
			Namespace: s.ns, Owner: owner, StreamName: name,
		},
	})
	require.NoError(t, err)
	return resp.GetFrontendResponse().GetState()
}

// attemptRecords is what a producer writing for one activity attempt sends:
// every record says which attempt wrote it, which is what lets a reader see
// where a retry took over.
func attemptRecords(attempt int64, bodies ...string) []*streamlib.StreamRecord {
	out := streamMsgs("", bodies...)
	for _, r := range out {
		r.Attempt = attempt
	}
	return out
}

func pollActivityTask(
	t *testing.T, env *testcore.TestEnv, s *streamTestEnv, taskQueue string,
) *workflowservice.PollActivityTaskQueueResponse {
	t.Helper()
	task, err := env.FrontendClient().PollActivityTaskQueue(s.ctx(),
		&workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.ns,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "tester",
		})
	require.NoError(t, err)
	require.NotEmpty(t, task.GetTaskToken(), "no activity task arrived")
	return task
}

func failActivityAttempt(
	t *testing.T, env *testcore.TestEnv, s *streamTestEnv,
	task *workflowservice.PollActivityTaskQueueResponse,
) {
	t.Helper()
	_, err := env.FrontendClient().RespondActivityTaskFailed(s.ctx(),
		&workflowservice.RespondActivityTaskFailedRequest{
			Namespace: s.ns,
			TaskToken: task.GetTaskToken(),
			Failure: &failurepb.Failure{
				Message: "attempt failed",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "Retryable"},
				},
			},
			Identity: "tester",
		})
	require.NoError(t, err)
}

func completeActivityAttempt(
	t *testing.T, env *testcore.TestEnv, s *streamTestEnv,
	task *workflowservice.PollActivityTaskQueueResponse,
) {
	t.Helper()
	_, err := env.FrontendClient().RespondActivityTaskCompleted(s.ctx(),
		&workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: s.ns, TaskToken: task.GetTaskToken(), Identity: "tester",
		})
	require.NoError(t, err)
}

var streamActivityRetryPolicy = &commonpb.RetryPolicy{
	InitialInterval:    durationpb.New(time.Second),
	BackoffCoefficient: 1,
	MaximumAttempts:    3,
}

// A standalone activity owns a default stream and named ones, written and read
// from outside by activity id. A retry inherits the stream, so a reader sees
// one log with the attempt on every record, and the stream ends when the
// activity reaches a terminal status rather than when an attempt fails.
func TestStreamStandaloneActivityOwnsStreams(t *testing.T) {
	env, s := newStreamActivityEnv(t)

	activityID := "stream-saa-" + uuid.NewString()
	taskQueue := activityID + "-tq"
	started, err := env.FrontendClient().StartActivityExecution(s.ctx(),
		&workflowservice.StartActivityExecutionRequest{
			Namespace:           s.ns,
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "streaming-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(time.Minute),
			RetryPolicy:         streamActivityRetryPolicy,
			RequestId:           uuid.NewString(),
		})
	require.NoError(t, err)

	owner := &streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_ACTIVITY, Id: activityID,
	}
	pinned := &streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_ACTIVITY, Id: activityID, RunId: started.GetRunId(),
	}

	// A reader may attach before anything is written, the same as on a
	// workflow's stream.
	empty := s.pollOwned(t, owner, "", 0, false)
	require.Empty(t, empty.GetRecords())
	require.False(t, empty.GetClosed())

	first := pollActivityTask(t, env, s, taskQueue)
	require.EqualValues(t, 1, first.GetAttempt())

	out, err := s.addOwned(t, owner, "", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(1, "token one", "token two"), ProducerId: "worker#1", Sequence: 1,
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), out.GetFirstOffset())
	_, err = s.addOwned(t, pinned, "progress", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(1, "step 1"),
	})
	require.NoError(t, err, "a named stream, addressed with the run pinned")

	// The default stream and the named one are separate logs.
	require.Equal(t, []string{"token one", "token two"},
		bodies(s.pollOwned(t, owner, chasmstream.DefaultStreamName, 0, false).GetRecords()),
		"an empty name and the default name reach the same stream")
	require.Equal(t, []string{"step 1"},
		bodies(s.pollOwned(t, pinned, "progress", 0, false).GetRecords()))

	// A failed attempt with a retry to come is not the end of the activity.
	failActivityAttempt(t, env, s, first)
	require.False(t, s.describeOwned(t, owner, "").GetClosed(),
		"the stream outlives an attempt that will be retried")

	second := pollActivityTask(t, env, s, taskQueue)
	require.EqualValues(t, 2, second.GetAttempt())
	out, err = s.addOwned(t, owner, "", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(2, "token one again"), ProducerId: "worker#2", Sequence: 1,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), out.GetFirstOffset(), "the retry continues the same log")

	read := s.pollOwned(t, owner, "", 0, false)
	require.Equal(t, []string{"token one", "token two", "token one again"}, bodies(read.GetRecords()))
	var attempts []int64
	for _, r := range read.GetRecords() {
		attempts = append(attempts, r.GetAttempt())
	}
	require.Equal(t, []int64{1, 1, 2}, attempts,
		"each record names its attempt, which is where a reader sees the retry take over")

	// A reader tailing the stream is released when the activity ends.
	type result struct {
		out *streamlib.PollMessagesOutput
		err error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
			FrontendRequest: &streamlib.PollWorkflowMessagesInput{
				Namespace: s.ns, Owner: owner, FromOffset: read.GetNextOffset(), WaitNewMessages: true,
			},
		})
		done <- result{resp.GetFrontendResponse(), err}
	}()
	// Give the reader time to park, so the wake is what releases it. Nothing
	// observable marks a parked reader, so a wait is the only option.
	time.Sleep(500 * time.Millisecond) //nolint:forbidigo
	completeActivityAttempt(t, env, s, second)

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.True(t, r.out.GetClosed(), "the parked reader learns the activity ended")
		require.Empty(t, r.out.GetRecords())
	case <-time.After(25 * time.Second):
		t.Fatal("the parked reader was not released when the activity completed")
	}

	// Ended, not gone: what was written stays readable, and nothing more can be
	// added.
	require.True(t, s.describeOwned(t, owner, "progress").GetClosed())
	require.Len(t, s.pollOwned(t, owner, "", 0, false).GetRecords(), 3)
	_, err = s.addOwned(t, owner, "", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(2, "too late"),
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)

	// A run id that is not this activity's names nothing.
	_, err = s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns,
			Owner: &streamlib.StreamOwner{
				Kind: streamlib.STREAM_OWNER_KIND_ACTIVITY, Id: activityID, RunId: uuid.NewString(),
			},
		},
	})
	require.Equal(t, codes.NotFound, status.Code(err), "%v", err)
}

// scheduleStreamingActivity starts a workflow whose first task schedules one
// activity, and returns the workflow's run id.
func scheduleStreamingActivity(
	t *testing.T, env *testcore.TestEnv, s *streamTestEnv, workflowID, activityID string,
	retry *commonpb.RetryPolicy,
) string {
	t.Helper()
	tq := &taskqueuepb.TaskQueue{Name: workflowID + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	we, err := env.FrontendClient().StartWorkflowExecution(s.ctx(),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.ns,
			WorkflowId:          workflowID,
			WorkflowType:        &commonpb.WorkflowType{Name: "activity-streams"},
			TaskQueue:           tq,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            "tester",
		})
	require.NoError(t, err)

	//nolint:staticcheck // SA1019: the raw poller is enough to schedule one activity.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:          activityID,
						ActivityType:        &commonpb.ActivityType{Name: "streaming-activity"},
						TaskQueue:           tq,
						StartToCloseTimeout: durationpb.New(time.Minute),
						RetryPolicy:         retry,
					},
				},
			}}, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	return we.GetRunId()
}

// An activity a workflow scheduled owns streams of its own, apart from the
// workflow's. They are addressed as the workflow, the activity and a name, so
// an outside producer and reader never see where the server keeps them, and
// the workflow cannot reach them under its own names.
func TestStreamWorkflowActivityOwnsStreams(t *testing.T) {
	env, s := newStreamActivityEnv(t)

	workflowID := "stream-wfa-" + uuid.NewString()
	runID := scheduleStreamingActivity(t, env, s, workflowID, "model-call", nil)
	pollActivityTask(t, env, s, workflowID+"-tq")

	owner := &streamlib.StreamOwner{
		Kind:       streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY,
		Id:         workflowID,
		RunId:      runID,
		ActivityId: "model-call",
	}
	_, err := s.addOwned(t, owner, "", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(1, "token"), ProducerId: "worker#1", Sequence: 1,
	})
	require.NoError(t, err)
	_, err = s.addOwned(t, owner, "reasoning", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(1, "thinking"),
	})
	require.NoError(t, err)

	require.Equal(t, []string{"token"}, bodies(s.pollOwned(t, owner, "", 0, false).GetRecords()))
	require.Equal(t, []string{"thinking"},
		bodies(s.pollOwned(t, owner, "reasoning", 0, false).GetRecords()))
	require.False(t, s.describeOwned(t, owner, "").GetClosed())

	// The workflow's own default stream is a different stream.
	workflowOwner := &streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW, Id: workflowID,
	}
	require.Empty(t, s.pollOwned(t, workflowOwner, "", 0, false).GetRecords(),
		"an activity's stream is not the workflow's")

	// Another activity of the same workflow has streams of its own too.
	other := &streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY, Id: workflowID, ActivityId: "other",
	}
	require.Empty(t, s.pollOwned(t, other, "", 0, false).GetRecords())

	// The workflow kind may not name the part of the map its activities use.
	_, err = s.addOwned(t, workflowOwner, "activity/model-call/output",
		&streamlib.AddWorkflowMessagesInput{Records: streamMsgs("", "forged")})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)

	// The workflow fields and an owner together are ambiguous.
	_, err = s.client.AddWorkflowMessages(s.ctx(), &streamlib.AddWorkflowMessagesRequest{
		FrontendRequest: &streamlib.AddWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: workflowID, Owner: owner,
			Records: streamMsgs("", "which one"),
		},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
}

// The owned-stream calls still accept a workflow named by its id alone, which
// is the WORKFLOW owner spelled without one.
func TestStreamOwnerDefaultsToTheNamedWorkflow(t *testing.T) {
	env, s := newStreamActivityEnv(t)

	workflowID := "stream-owner-default-" + uuid.NewString()
	scheduleStreamingActivity(t, env, s, workflowID, "unused", nil)

	_, err := s.client.AddWorkflowMessages(s.ctx(), &streamlib.AddWorkflowMessagesRequest{
		FrontendRequest: &streamlib.AddWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: workflowID,
			Records: []*streamlib.StreamRecord{{
				Body: &commonpb.Payload{Data: []byte("by id")},
				Kind: streampb.STREAM_RECORD_KIND_DATA,
			}},
		},
	})
	require.NoError(t, err)
	owner := &streamlib.StreamOwner{Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW, Id: workflowID}
	require.Equal(t, []string{"by id"}, bodies(s.pollOwned(t, owner, "", 0, false).GetRecords()))
}

// A workflow's activity keeps its streams across a retry and they end when the
// activity reaches a terminal status, while the workflow is still running. A
// reader tailing the activity is released then, not at workflow close.
func TestStreamWorkflowActivityStreamsEndWithTheActivity(t *testing.T) {
	env, s := newStreamActivityEnv(t)

	workflowID := "stream-wfa-end-" + uuid.NewString()
	scheduleStreamingActivity(t, env, s, workflowID, "model-call", streamActivityRetryPolicy)
	owner := &streamlib.StreamOwner{
		Kind:       streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY,
		Id:         workflowID,
		ActivityId: "model-call",
	}

	first := pollActivityTask(t, env, s, workflowID+"-tq")
	_, err := s.addOwned(t, owner, "", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(1, "partial answer"), ProducerId: "worker#1", Sequence: 1,
	})
	require.NoError(t, err)
	failActivityAttempt(t, env, s, first)
	require.False(t, s.describeOwned(t, owner, "").GetClosed(),
		"a failed attempt with a retry to come does not end the activity's stream")

	second := pollActivityTask(t, env, s, workflowID+"-tq")
	require.EqualValues(t, 2, second.GetAttempt())
	_, err = s.addOwned(t, owner, "", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(2, "full answer"), ProducerId: "worker#2", Sequence: 1,
	})
	require.NoError(t, err)
	read := s.pollOwned(t, owner, "", 0, false)
	require.Equal(t, []string{"partial answer", "full answer"}, bodies(read.GetRecords()),
		"the retry inherits the stream")

	type result struct {
		out *streamlib.PollMessagesOutput
		err error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
			FrontendRequest: &streamlib.PollWorkflowMessagesInput{
				Namespace: s.ns, Owner: owner, FromOffset: read.GetNextOffset(), WaitNewMessages: true,
			},
		})
		done <- result{resp.GetFrontendResponse(), err}
	}()
	// Give the reader time to park, so the wake is what releases it. Nothing
	// observable marks a parked reader, so a wait is the only option.
	time.Sleep(500 * time.Millisecond) //nolint:forbidigo
	completeActivityAttempt(t, env, s, second)

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.True(t, r.out.GetClosed(), "the parked reader learns the activity ended")
	case <-time.After(25 * time.Second):
		t.Fatal("the parked reader was not released when the activity completed")
	}

	require.True(t, s.describeOwned(t, owner, "").GetClosed())
	_, err = s.addOwned(t, owner, "", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(2, "too late"),
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)

	// The workflow is still running, so its own stream is not ended.
	workflowOwner := &streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW, Id: workflowID,
	}
	require.False(t, s.describeOwned(t, workflowOwner, "").GetClosed())
}
