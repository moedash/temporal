package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The creator's byte cap is a ceiling on what the stream holds: an append that
// would cross it is refused, describe reports the cap and the held bytes, and
// truncating behind the floor gives the room back.
func TestStreamByteCapRefusesAndDescribeReportsIt(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-byte-cap"
	const maxBytes = 120

	_, err := s.client.CreateStream(ctx, &streamlib.CreateStreamRequest{
		FrontendRequest: &streamlib.CreateStreamInput{
			Namespace: s.ns, StreamId: id,
			Lifecycle: &streamlib.StreamLifecycle{MaxBytes: maxBytes},
		},
	})
	require.NoError(t, err)

	describe := func() *streamlib.StreamState {
		t.Helper()
		resp, err := s.client.DescribeStream(ctx, &streamlib.DescribeStreamRequest{
			FrontendRequest: &streamlib.DescribeStreamInput{Namespace: s.ns, StreamId: id},
		})
		require.NoError(t, err)
		return resp.GetFrontendResponse().GetState()
	}
	require.Equal(t, int64(maxBytes), describe().GetLifecycle().GetMaxBytes())

	appended := 0
	for appended < 100 {
		_, err := s.add(ctx, t, id, &streamlib.AddMessagesInput{Records: streamMsgs("t", "0123456789")})
		if err != nil {
			require.Equal(t, codes.ResourceExhausted, status.Code(err), "%v", err)
			break
		}
		appended++
	}
	require.Positive(t, appended, "a cap of a few batches admits at least one")
	require.Less(t, appended, 100, "and refuses eventually")

	state := describe()
	require.Positive(t, state.GetHeldBytes())
	require.LessOrEqual(t, state.GetHeldBytes(), int64(maxBytes))
	require.Equal(t, int64(appended), state.GetHeadOffset(), "the refused append wrote nothing")
	require.Equal(t, int64(0), state.GetBaseOffset(), "the byte cap reclaims nothing on its own")

	// Truncating behind the floor frees the room.
	_, err = s.client.TruncateStream(ctx, &streamlib.TruncateStreamRequest{
		FrontendRequest: &streamlib.TruncateStreamInput{
			Namespace: s.ns, StreamId: id, NewBaseOffset: state.GetHeadOffset(),
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), describe().GetHeldBytes())
	_, err = s.add(ctx, t, id, &streamlib.AddMessagesInput{Records: streamMsgs("t", "0123456789")})
	require.NoError(t, err)
}

func TestStreamLifecycleRefusesANegativeByteCap(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	_, err := s.client.CreateStream(ctx, &streamlib.CreateStreamRequest{
		FrontendRequest: &streamlib.CreateStreamInput{
			Namespace: s.ns, StreamId: "stream-negative-bytes",
			Lifecycle: &streamlib.StreamLifecycle{MaxBytes: -1},
		},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
}
