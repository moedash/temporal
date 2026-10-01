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
	chasmstream "go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Retention is an age on an open stream: a record older than it is reclaimed
// behind the floor on the recheck interval, and never past an active
// consumer's floor.
func TestStreamAgesRecordsOutOnAnOpenStream(t *testing.T) {
	// Dedicated, because the recheck interval is a global setting.
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
	env.OverrideDynamicConfig(chasmstream.RetentionRecheckIntervalSetting, time.Second)
	s := newStreamTestEnvFrom(t, env)
	ctx := streamCtx(t)

	const retention = 2 * time.Second
	const aging, pinned = "stream-aging", "stream-aging-pinned"
	for _, id := range []string{aging, pinned} {
		_, err := s.client.CreateStream(ctx, &streamlib.CreateStreamRequest{
			FrontendRequest: &streamlib.CreateStreamInput{
				Namespace: s.ns, StreamId: id,
				Lifecycle: &streamlib.StreamLifecycle{Retention: durationpb.New(retention)},
			},
		})
		require.NoError(t, err)
	}
	describe := func(id string) *streamlib.StreamState {
		t.Helper()
		resp, err := s.client.DescribeStream(ctx, &streamlib.DescribeStreamRequest{
			FrontendRequest: &streamlib.DescribeStreamInput{Namespace: s.ns, StreamId: id},
		})
		require.NoError(t, err)
		return resp.GetFrontendResponse().GetState()
	}

	// A workflow pins the second stream at offset 0 before anything is written.
	workflowID := "stream-age-consumer-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: workflowID + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	_, err := env.FrontendClient().StartWorkflowExecution(ctx,
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.ns,
			WorkflowId:          workflowID,
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
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			return nil, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	_, err = s.client.SubscribeWorkflow(ctx, &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: workflowID, StreamId: pinned, StartOffset: 0,
		},
	})
	require.NoError(t, err)

	for _, id := range []string{aging, pinned} {
		_, err := s.add(ctx, t, id, &streamlib.AddMessagesInput{Records: streamMsgs("t", "old")})
		require.NoError(t, err)
	}
	require.Equal(t, []string{"old"}, bodies(s.poll(ctx, t, aging, 0).GetRecords()))

	// A retention and a recheck or two later the record is gone from the open
	// stream, and the floor says so.
	await.RequireTrue(t, func() bool {
		return describe(aging).GetBaseOffset() == 1
	}, 30*time.Second, 200*time.Millisecond)
	require.Equal(t, int64(0), describe(aging).GetHeldBytes())
	_, err = s.client.PollMessages(ctx, &streamlib.PollMessagesRequest{
		FrontendRequest: &streamlib.PollMessagesInput{Namespace: s.ns, StreamId: aging, FromOffset: 0},
	})
	requireReason(t, err, codes.FailedPrecondition, chasmstream.ReasonCursorBelowFloor)

	// The stream stays open: a new record lands at the next offset and reads
	// back from the floor.
	_, err = s.add(ctx, t, aging, &streamlib.AddMessagesInput{Records: streamMsgs("t", "new")})
	require.NoError(t, err)
	require.Equal(t, []string{"new"}, bodies(s.poll(ctx, t, aging, 1).GetRecords()))

	// The pinned stream's record is just as old, and stays: a workflow's
	// History depends on it.
	held := describe(pinned)
	require.Equal(t, int64(0), held.GetBaseOffset(), "the age never crosses an active consumer's floor")
	require.Equal(t, []string{"old"}, bodies(s.poll(ctx, t, pinned, 0).GetRecords()))
}
