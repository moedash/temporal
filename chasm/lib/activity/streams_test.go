package activity

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

func streamRecords(bodies ...string) []*streamlib.StreamRecord {
	out := make([]*streamlib.StreamRecord, len(bodies))
	for i, b := range bodies {
		out[i] = &streamlib.StreamRecord{
			Body: &commonpb.Payload{Data: []byte(b)},
			Kind: streampb.STREAM_RECORD_KIND_DATA,
		}
	}
	return out
}

func activityInStatus(status activitypb.ActivityExecutionStatus) *Activity {
	return &Activity{ActivityState: &activitypb.ActivityState{Status: status}}
}

// A standalone activity owns a map of named streams, created on first write,
// and a retry scheduled after a failed attempt keeps writing to the same one.
func TestActivityOwnsStreamsAcrossAttempts(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	a := activityInStatus(activitypb.ACTIVITY_EXECUTION_STATUS_STARTED)

	first, err := a.AppendToOwnedStream(ctx, stream.DefaultStreamName, stream.AddMessagesRequest{
		Records: streamRecords("attempt one"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), first.FirstOffset)
	require.NotNil(t, a.OwnedStream(ctx, stream.DefaultStreamName))
	require.Nil(t, a.OwnedStream(ctx, "progress"), "a name nothing wrote to has no stream")

	// The attempt failed and a retry is scheduled. That is not the end of the
	// activity, so the stream is not ended either.
	a.Status = activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED
	require.False(t, a.OwnedStreamEnded(ctx, stream.DefaultStreamName))

	a.Status = activitypb.ACTIVITY_EXECUTION_STATUS_STARTED
	second, err := a.AppendToOwnedStream(ctx, stream.DefaultStreamName, stream.AddMessagesRequest{
		Records: streamRecords("attempt two"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), second.FirstOffset, "the retry continues the same log")
}

// Once the activity is terminal its streams are ended for readers, and an
// append from a worker still running an attempt the server gave up on is
// refused rather than added after readers were told the stream was over.
func TestActivityStreamsEndAtTerminalStatus(t *testing.T) {
	for _, status := range []activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
	} {
		t.Run(status.String(), func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			a := activityInStatus(status)
			require.True(t, a.OwnedStreamEnded(ctx, stream.DefaultStreamName))

			_, err := a.AppendToOwnedStream(ctx, stream.DefaultStreamName, stream.AddMessagesRequest{
				Records: streamRecords("too late"),
			})
			var precondition *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &precondition)
			require.Nil(t, a.OwnedStream(ctx, stream.DefaultStreamName),
				"a refused append must not leave a stream behind")
		})
	}
}

// The count bound and the shared byte budget are the same code a workflow
// uses, so an activity cannot grow its state past what a workflow may.
func TestActivityStreamsAreBoundedLikeAWorkflowsAre(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	a := activityInStatus(activitypb.ACTIVITY_EXECUTION_STATUS_STARTED)
	limits := stream.Limits{MaxOwnedStreamsPerWorkflow: 2}

	for _, name := range []string{"a", "b"} {
		_, err := a.AppendToOwnedStream(ctx, name, stream.AddMessagesRequest{
			Records: streamRecords("x"), Limits: limits,
		})
		require.NoError(t, err)
	}
	_, err := a.AppendToOwnedStream(ctx, "c", stream.AddMessagesRequest{
		Records: streamRecords("x"), Limits: limits,
	})
	require.ErrorContains(t, err, "activity already owns 2 streams")
}
