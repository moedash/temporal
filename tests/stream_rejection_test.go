package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	streampb "go.temporal.io/api/stream/v1"
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

// A workflow reads a topic by name before anything has been written to it, so
// the subscribe has to bring the owned stream into being rather than fail the
// task. An outside producer that arrives later appends to that same stream,
// and its records reach the workflow carrying the identity it wrote.
func TestSubscribeToAnUnwrittenNameCreatesTheOwnedStream(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)
	execution, tq := startConsumer(t, s, "stream-wf-subscribe-first-")

	name := "inputs-" + uuid.NewString()
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
				return subscribeCommand(name), nil
			}
			return nil, nil
		},
		Logger: env.Logger,
		T:      t,
	}

	_, err := poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	events := env.GetHistory(s.ns, execution)
	require.Nil(t, workflowTaskFailedWith(events,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SUBSCRIBE_STREAM_ATTRIBUTES))
	subscribed := subscribedEvents(events)
	require.Len(t, subscribed, 1)
	require.Equal(t, name, subscribed[0].GetStreamId())
	require.Equal(t, int64(0), subscribed[0].GetStartOffset(), "the new stream starts empty")

	_, err = s.client.AddWorkflowMessages(s.ctx(), &streamlib.AddWorkflowMessagesRequest{
		FrontendRequest: &streamlib.AddWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(), StreamName: name,
			Records: []*streamlib.StreamRecord{
				{
					Body: &commonpb.Payload{Data: []byte("hello")}, Topic: name,
					ProducerId: "model", Attempt: 2, Sequence: 0,
				},
				{
					Kind: streampb.STREAM_RECORD_KIND_FINISH, Topic: name,
					ProducerId: "model", Attempt: 2, Sequence: 1,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	got := currentSlice(t, delivered[1])
	require.Equal(t, name, got.GetStreamId())
	require.Equal(t, int64(2), got.GetToOffset())

	records := got.GetRecords()
	require.Len(t, records, 2)
	require.Equal(t, "hello", string(records[0].GetBody().GetData()))
	require.Equal(t, streampb.STREAM_RECORD_KIND_DATA, records[0].GetKind(),
		"a record with no kind is data")
	require.Equal(t, "model", records[0].GetProducerId())
	require.Equal(t, int64(2), records[0].GetAttempt())
	require.Equal(t, int64(0), records[0].GetSequence())
	require.Equal(t, streampb.STREAM_RECORD_KIND_FINISH, records[1].GetKind(),
		"a finish record is delivered like any other")
	require.Equal(t, "model", records[1].GetProducerId())
	require.Equal(t, int64(1), records[1].GetSequence())
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
