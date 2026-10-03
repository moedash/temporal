package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	chasmstream "go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A create naming a stream that already exists answers in two ways a client
// tells apart without a describe: the same lifecycle is an idempotent retry,
// a different one is a policy change it has to make under a new id.
func TestStreamCreateOnAnExistingIdTellsARetryFromAPolicyChange(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-create-twice"
	create := func(lifecycle *streamlib.StreamLifecycle) error {
		_, err := s.client.CreateStream(ctx, &streamlib.CreateStreamRequest{
			FrontendRequest: &streamlib.CreateStreamInput{
				Namespace: s.ns, StreamId: id, Lifecycle: lifecycle,
			},
		})
		return err
	}
	policy := &streamlib.StreamLifecycle{MaxItems: 4, MaxBytes: 1 << 20}
	require.NoError(t, create(policy))

	err := create(policy)
	require.Equal(t, codes.AlreadyExists, status.Code(err), "%v", err)

	// The frontend settles an unset retention to the namespace's, so a repeat
	// that spells the settled value out is still the same lifecycle.
	settled, err := s.client.DescribeStream(ctx, &streamlib.DescribeStreamRequest{
		FrontendRequest: &streamlib.DescribeStreamInput{Namespace: s.ns, StreamId: id},
	})
	require.NoError(t, err)
	spelledOut := &streamlib.StreamLifecycle{
		MaxItems: 4, MaxBytes: 1 << 20,
		Retention: settled.GetFrontendResponse().GetState().GetLifecycle().GetRetention(),
	}
	err = create(spelledOut)
	require.Equal(t, codes.AlreadyExists, status.Code(err), "%v", err)

	for name, changed := range map[string]*streamlib.StreamLifecycle{
		"more items":       {MaxItems: 5, MaxBytes: 1 << 20},
		"no byte cap":      {MaxItems: 4},
		"other retention":  {MaxItems: 4, MaxBytes: 1 << 20, Retention: durationpb.New(1)},
		"no policy at all": nil,
	} {
		t.Run(name, func(t *testing.T) {
			requireReason(t, create(changed), codes.FailedPrecondition, chasmstream.ReasonPolicyMismatch)
		})
	}

	// The stream is untouched by any of it.
	described, err := s.client.DescribeStream(ctx, &streamlib.DescribeStreamRequest{
		FrontendRequest: &streamlib.DescribeStreamInput{Namespace: s.ns, StreamId: id},
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), described.GetFrontendResponse().GetState().GetLifecycle().GetMaxItems())
}
