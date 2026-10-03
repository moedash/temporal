package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	chasmstream "go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The stream service counts what a namespace appends and reads, tagged with
// the namespace, so metering has a unit to read.
func TestStreamServiceMetersAppendsAndDeliveriesPerNamespace(t *testing.T) {
	env := testcore.NewEnv(t)
	capture := env.StartNamespaceMetricCapture()
	s := newStreamTestEnvFrom(t, env)
	ctx := streamCtx(t)
	const id = "stream-metered"
	s.create(ctx, t, id)

	in := &streamlib.AddMessagesInput{
		Records: streamMsgs("t", "a", "bb", "ccc"), ProducerId: "p1", Sequence: 1,
	}
	_, err := s.add(ctx, t, id, in)
	require.NoError(t, err)
	// A deduplicated retry stored nothing and adds nothing to the meter.
	retry, err := s.add(ctx, t, id, &streamlib.AddMessagesInput{
		Records: streamMsgs("t", "a", "bb", "ccc"), ProducerId: "p1", Sequence: 1,
	})
	require.NoError(t, err)
	require.True(t, retry.GetDeduplicated())

	got := s.poll(ctx, t, id, 0)
	require.Len(t, got.GetRecords(), 3)
	empty := s.poll(ctx, t, id, 3)
	require.Empty(t, empty.GetRecords())

	sum := func(name string) int64 {
		var total int64
		for _, rec := range capture.Metric(name) {
			require.Equal(t, s.ns, rec.Tags["namespace"])
			total += rec.Value.(int64)
		}
		return total
	}
	require.Equal(t, int64(3), sum("stream_records_appended"))
	require.Equal(t, int64(3), sum("stream_records_delivered"))
	require.Equal(t, int64(2), sum("stream_polls"), "every admitted poll counts, empty or not")
	require.Positive(t, sum("stream_bytes_appended"))
	require.Positive(t, sum("stream_bytes_delivered"))
}

// A namespace over its poll rate is refused with the standard cause and the
// namespace scope, the same shape as the frontend's own rate limits, so a
// client's existing handling applies.
func TestStreamPollsAreRateLimitedPerNamespace(t *testing.T) {
	env := testcore.NewEnv(t)
	env.OverrideDynamicConfig(chasmstream.PollsPerSecondSetting, 1)
	s := newStreamTestEnvFrom(t, env)
	ctx := streamCtx(t)
	const id = "stream-poll-limited"
	s.create(ctx, t, id)
	_, err := s.add(ctx, t, id, &streamlib.AddMessagesInput{Records: streamMsgs("t", "a")})
	require.NoError(t, err)

	// One per second with a burst of one: the first poll is admitted and the
	// next, within the same second, is not.
	got := s.poll(ctx, t, id, 0)
	require.Equal(t, []string{"a"}, bodies(got.GetRecords()))

	_, err = s.client.PollMessages(ctx, &streamlib.PollMessagesRequest{
		FrontendRequest: &streamlib.PollMessagesInput{Namespace: s.ns, StreamId: id, FromOffset: 0},
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "%v", err)
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, serviceerror.FromStatus(status.Convert(err)), &exhausted)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, exhausted.Cause)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, exhausted.Scope)
}

func TestStreamAppendsAreRateLimitedPerNamespace(t *testing.T) {
	env := testcore.NewEnv(t)
	env.OverrideDynamicConfig(chasmstream.AppendRecordsPerSecondSetting, 2)
	s := newStreamTestEnvFrom(t, env)
	ctx := streamCtx(t)
	const id = "stream-append-limited"
	s.create(ctx, t, id)

	// The burst never drops below one full batch, so the first full batch is
	// admitted whatever the rate and the next record is over it.
	full := make([]string, streamMaxBatch)
	for i := range full {
		full[i] = "x"
	}
	_, err := s.add(ctx, t, id, &streamlib.AddMessagesInput{Records: streamMsgs("t", full...)})
	require.NoError(t, err)
	_, err = s.add(ctx, t, id, &streamlib.AddMessagesInput{Records: streamMsgs("t", "over")})
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "%v", err)

	// Nothing over the rate was stored.
	got := s.poll(ctx, t, id, 0)
	require.Len(t, got.GetRecords(), streamMaxBatch)
	require.Equal(t, int64(streamMaxBatch), got.GetHeadOffset())
}
