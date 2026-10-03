package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	chasmstream "go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/tests/testcore"
)

// A refused publish has to fail the workflow task with a cause, not the
// completion call. Failing the call leaves the worker retrying the same command
// against the same limit until the task times out, with nothing in History
// saying why.
func TestOverLimitPublishFailsTheWorkflowTask(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)
	execution, tq := startConsumer(t, s, "stream-wf-reject-publish-")

	tooMany := make([]string, chasmstream.MaxRecordsPerBatch+1)
	for i := range tooMany {
		tooMany[i] = "x"
	}
	task := 0
	//nolint:staticcheck // SA1019: only the deprecated poller can emit this command type.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			task++
			if task == 1 {
				return publishCommand(tooMany...), nil
			}
			return publishCommand("fits"), nil
		},
		Logger: env.Logger,
		T:      t,
	}

	_, err := poller.PollAndProcessWorkflowTask()
	require.Error(t, err, "the completion is rejected")
	require.ErrorContains(t, err, "exceeds the limit")

	events := env.GetHistory(s.ns, execution)
	failed := workflowTaskFailedWith(events,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_APPEND_STREAM_RECORDS_ATTRIBUTES)
	require.NotNil(t, failed, "the refusal is a workflow task failure with its own cause")
	require.ErrorContains(t, err, failed.GetFailure().GetMessage())

	desc, err := s.client.DescribeWorkflowStream(s.ctx(), &streamlib.DescribeWorkflowStreamRequest{
		FrontendRequest: &streamlib.DescribeWorkflowStreamInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(),
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), desc.GetFrontendResponse().GetState().GetHeadOffset(),
		"a refused publish appends nothing")

	// The next attempt is a fresh task, and a publish that fits lands at the
	// start of the stream.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithExpectedAttemptCount(2))
	require.NoError(t, err)
	poll, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(), FromOffset: 0,
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"fits"}, bodies(poll.GetFrontendResponse().GetRecords()))
	require.Equal(t, []int64{0}, offsets(poll.GetFrontendResponse().GetRecords()))
}

// The guarantee the design is sold on: a publish commits with the workflow
// task, so a task that fails after publishing publishes nothing.
func TestPublishOnAFailedTaskLeavesNoTrace(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)
	execution, tq := startConsumer(t, s, "stream-wf-reject-atomic-")

	// A ScheduleActivityTask with no task queue fails command validation, so the
	// whole task is failed after the publish command already ran.
	invalidTrailer := &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
			ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:   "no-queue",
				ActivityType: &commonpb.ActivityType{Name: "noop"},
			},
		},
	}
	task := 0
	//nolint:staticcheck // SA1019: only the deprecated poller can emit this command type.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			task++
			if task == 1 {
				return append(publishCommand("lost-with-the-task"), invalidTrailer), nil
			}
			return publishCommand("first-to-land"), nil
		},
		Logger: env.Logger,
		T:      t,
	}

	_, err := poller.PollAndProcessWorkflowTask()
	require.Error(t, err)

	events := env.GetHistory(s.ns, execution)
	require.NotNil(t, workflowTaskFailedWith(events,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES))
	for _, e := range events {
		require.Nil(t, e.GetWorkflowStreamRecordsAppendedEventAttributes(),
			"a publish on a failed task leaves no event")
	}
	desc, err := s.client.DescribeWorkflowStream(s.ctx(), &streamlib.DescribeWorkflowStreamRequest{
		FrontendRequest: &streamlib.DescribeWorkflowStreamInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(),
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), desc.GetFrontendResponse().GetState().GetHeadOffset())
	poll, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(), FromOffset: 0,
		},
	})
	require.NoError(t, err)
	require.Empty(t, poll.GetFrontendResponse().GetRecords())

	// The retry's publish is the first thing the stream ever holds.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithExpectedAttemptCount(2))
	require.NoError(t, err)
	poll, err = s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(), FromOffset: 0,
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"first-to-land"}, bodies(poll.GetFrontendResponse().GetRecords()))
	require.Equal(t, []int64{0}, offsets(poll.GetFrontendResponse().GetRecords()))
}
