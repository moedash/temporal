package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func completeWorkflowCommand() []*commandpb.Command {
	attrs := &commandpb.CompleteWorkflowExecutionCommandAttributes{}
	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: attrs,
		},
	}}
}

func describeStream(t *testing.T, s *streamTestEnv, streamID string) *streamlib.StreamState {
	t.Helper()
	desc, err := s.client.DescribeStream(s.ctx(), &streamlib.DescribeStreamRequest{
		FrontendRequest: &streamlib.DescribeStreamInput{Namespace: s.ns, StreamId: streamID},
	})
	require.NoError(t, err)
	return desc.GetFrontendResponse().GetState()
}

// subscribeConsumeAndComplete runs a consumer through one delivered range and
// then completes it, leaving its pin on the stream.
func subscribeConsumeAndComplete(
	t *testing.T, s *streamTestEnv, streamID string, prefix string,
) *commonpb.WorkflowExecution {
	t.Helper()
	execution, tq := startConsumer(t, s, prefix)
	task := 0
	//nolint:staticcheck // SA1019: consistent with the other stream tests.
	poller := &testcore.TaskPoller{
		Client:    s.env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			task++
			if task == 3 {
				return completeWorkflowCommand(), nil
			}
			return nil, nil
		},
		Logger: s.env.Logger,
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
			Namespace: s.ns, StreamId: streamID, Messages: streamMsgs("tokens", "one", "two"),
		},
	})
	require.NoError(t, err)

	// Consumes the range, then completes on the task after.
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	signalWorkflow(t, s, execution.GetWorkflowId(), execution.GetRunId())
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	state := describeStream(t, s, streamID)
	require.Len(t, state.GetConsumers(), 1, "the completed run still holds its pin for now")
	return execution
}

// A consumer that completed can never replay, so the floor it held is a leak.
// The stream learns a consumer is gone when it next tries to tell it the
// frontier moved, and releases the pin then.
func TestACompletedConsumerReleasesItsFloor(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)
	streamID := "released-stream-" + uuid.NewString()
	s.create(s.ctx(), t, streamID)

	subscribeConsumeAndComplete(t, s, streamID, "stream-wf-released-")

	_, err := s.client.TruncateStream(s.ctx(), &streamlib.TruncateStreamRequest{
		FrontendRequest: &streamlib.TruncateStreamInput{
			Namespace: s.ns, StreamId: streamID, NewBaseOffset: 2,
		},
	})
	require.ErrorContains(t, err, "still depends on offset 0",
		"the pin holds until the stream finds out")

	// The append is what makes the stream go and ask.
	_, err = s.client.AddMessages(s.ctx(), &streamlib.AddMessagesRequest{
		FrontendRequest: &streamlib.AddMessagesInput{
			Namespace: s.ns, StreamId: streamID, Messages: streamMsgs("tokens", "three"),
		},
	})
	require.NoError(t, err)
	await.RequireTrue(t, func() bool {
		return len(describeStream(t, s, streamID).GetConsumers()) == 0
	}, 20*time.Second, 100*time.Millisecond)

	_, err = s.client.TruncateStream(s.ctx(), &streamlib.TruncateStreamRequest{
		FrontendRequest: &streamlib.TruncateStreamInput{
			Namespace: s.ns, StreamId: streamID, NewBaseOffset: 2,
		},
	})
	require.NoError(t, err, "nothing holds the floor once the consumer is gone")
}

// A new run of the same workflow id is a new consumer. It subscribes at its
// own offset and takes over the pin, rather than inheriting a floor from a run
// that finished, which could be below the stream's base and refuse it.
func TestANewRunSubscribesFreshAfterThePreviousCompleted(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)
	streamID := "rerun-stream-" + uuid.NewString()
	s.create(s.ctx(), t, streamID)

	first := subscribeConsumeAndComplete(t, s, streamID, "stream-wf-rerun-")

	// The same workflow id, started again after the first run completed.
	second, err := env.FrontendClient().StartWorkflowExecution(
		s.ctx(),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.ns,
			WorkflowId:          first.GetWorkflowId(),
			WorkflowType:        &commonpb.WorkflowType{Name: "stream-consumer"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: first.GetWorkflowId() + "-tq2"},
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            "tester",
		},
	)
	require.NoError(t, err)
	require.NotEqual(t, first.GetRunId(), second.GetRunId())

	sub, err := s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: first.GetWorkflowId(), StreamId: streamID, StartOffset: 2,
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), sub.GetFrontendResponse().GetStartOffset(),
		"the new run starts where it asked, not where the old run did")

	state := describeStream(t, s, streamID)
	require.Len(t, state.GetConsumers(), 1,
		"the old run's pin is replaced, not kept beside the new one")
	for _, consumer := range state.GetConsumers() {
		require.Equal(t, second.GetRunId(), consumer.GetRunId())
		require.Equal(t, int64(2), consumer.GetReplayFloor())
	}

	_, err = s.client.TruncateStream(s.ctx(), &streamlib.TruncateStreamRequest{
		FrontendRequest: &streamlib.TruncateStreamInput{
			Namespace: s.ns, StreamId: streamID, NewBaseOffset: 2,
		},
	})
	require.NoError(t, err, "only the new run's floor holds")
}
