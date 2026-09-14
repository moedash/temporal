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
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A range a workflow consumed is part of that workflow's recovery: the offsets
// are in its History and a replay is asked to reproduce them from the stream.
// Deleting those messages succeeds immediately and fails much later, during a
// replay nobody is watching, so the stream refuses instead.
func TestTruncationRefusesToDropWhatAConsumerNeedsToReplay(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	streamID := "retained-stream-" + uuid.NewString()
	s.create(s.ctx(), t, streamID)
	_, err := s.client.AddMessages(s.ctx(), &streamlib.AddMessagesRequest{
		FrontendRequest: &streamlib.AddMessagesInput{
			Namespace: s.ns, StreamId: streamID,
			Messages: []*streamlib.StreamMessage{
				{Body: &commonpb.Payload{Data: []byte("one")}, Kind: streamlib.STREAM_MESSAGE_KIND_DATA},
				{Body: &commonpb.Payload{Data: []byte("two")}, Kind: streamlib.STREAM_MESSAGE_KIND_DATA},
				{Body: &commonpb.Payload{Data: []byte("three")}, Kind: streamlib.STREAM_MESSAGE_KIND_DATA},
			},
		},
	})
	require.NoError(t, err)

	id := "stream-retention-wf-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	_, err = env.FrontendClient().StartWorkflowExecution(s.ctx(), &workflowservice.StartWorkflowExecutionRequest{
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

	//nolint:staticcheck // SA1019: consistent with the other stream tests.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(_ *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			return nil, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	_, err = s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: id, StreamId: streamID, StartOffset: 0,
		},
	})
	require.NoError(t, err)

	// Delivered and recorded, which is what makes these three messages part of
	// the workflow's recovery rather than spare capacity.
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	_, err = s.client.TruncateStream(s.ctx(), &streamlib.TruncateStreamRequest{
		FrontendRequest: &streamlib.TruncateStreamInput{
			Namespace: s.ns, StreamId: streamID, NewBaseOffset: 2,
		},
	})
	require.ErrorContains(t, err, "still depends on offset 0")

	desc, err := s.client.DescribeStream(s.ctx(), &streamlib.DescribeStreamRequest{
		FrontendRequest: &streamlib.DescribeStreamInput{Namespace: s.ns, StreamId: streamID},
	})
	require.NoError(t, err)
	state := desc.GetFrontendResponse().GetState()
	require.Equal(t, int64(0), state.GetBaseOffset(), "the messages must still be there")
	for _, consumer := range state.GetConsumers() {
		require.Equal(t, int64(0), consumer.GetReplayFloor(),
			"the floor is where the subscription started, not where it has read to")
	}
}

// The refusal is about a consumer's recovery, so a stream nobody is consuming
// truncates exactly as before. Without this, the fix would be a storage leak
// wearing a correctness argument.
func TestTruncationStillWorksWithNoConsumer(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	streamID := "unwatched-stream-" + uuid.NewString()
	s.create(s.ctx(), t, streamID)
	_, err := s.client.AddMessages(s.ctx(), &streamlib.AddMessagesRequest{
		FrontendRequest: &streamlib.AddMessagesInput{
			Namespace: s.ns, StreamId: streamID,
			Messages: []*streamlib.StreamMessage{
				{Body: &commonpb.Payload{Data: []byte("one")}, Kind: streamlib.STREAM_MESSAGE_KIND_DATA},
				{Body: &commonpb.Payload{Data: []byte("two")}, Kind: streamlib.STREAM_MESSAGE_KIND_DATA},
			},
		},
	})
	require.NoError(t, err)

	_, err = s.client.TruncateStream(s.ctx(), &streamlib.TruncateStreamRequest{
		FrontendRequest: &streamlib.TruncateStreamInput{
			Namespace: s.ns, StreamId: streamID, NewBaseOffset: 2,
		},
	})
	require.NoError(t, err)

	desc, err := s.client.DescribeStream(s.ctx(), &streamlib.DescribeStreamRequest{
		FrontendRequest: &streamlib.DescribeStreamInput{Namespace: s.ns, StreamId: streamID},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), desc.GetFrontendResponse().GetState().GetBaseOffset())
}
