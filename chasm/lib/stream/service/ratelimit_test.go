package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/stream"
	"go.temporal.io/server/common/dynamicconfig"
)

// limitedConfig is the defaults with the three rates pinned, per namespace.
func limitedConfig(records, bytes, polls map[string]int) *stream.Config {
	cfg := stream.NewConfig(dynamicconfig.NewNoopCollection())
	pick := func(rates map[string]int, def int) dynamicconfig.IntPropertyFnWithNamespaceFilter {
		return func(ns string) int {
			if rate, ok := rates[ns]; ok {
				return rate
			}
			return def
		}
	}
	cfg.AppendRecordsPerSecond = pick(records, stream.AppendRecordsPerSecond)
	cfg.AppendBytesPerSecond = pick(bytes, stream.AppendBytesPerSecond)
	cfg.PollsPerSecond = pick(polls, stream.PollsPerSecond)
	return cfg
}

func requireOverRate(t *testing.T, err error) {
	t.Helper()
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, exhausted.Cause)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, exhausted.Scope)
}

func TestAppendRecordsRateIsPerNamespace(t *testing.T) {
	limiters := newNamespaceLimiters(limitedConfig(map[string]int{"slow": 3}, nil, nil))
	limits := stream.DefaultLimits()
	now := time.Now()

	// The burst never drops below one full batch, so the first batch goes
	// through whatever the rate; after that the bucket refills at the rate.
	require.NoError(t, limiters.allowAppend("slow", now, stream.MaxRecordsPerBatch, 10, limits))
	requireOverRate(t, limiters.allowAppend("slow", now, 1, 10, limits))
	later := now.Add(time.Second)
	require.NoError(t, limiters.allowAppend("slow", later, 3, 10, limits))
	requireOverRate(t, limiters.allowAppend("slow", later, 1, 10, limits))

	// A batch over the cap is not the limiter's to refuse: the component names
	// the cap it broke instead.
	require.NoError(t, limiters.allowAppend("slow", later, stream.MaxRecordsPerBatch+1, 10, limits))

	// Another namespace has a bucket of its own.
	require.NoError(t, limiters.allowAppend("other", now, stream.MaxRecordsPerBatch, 10, limits))
}

func TestAppendBytesRateAdmitsAFullBatchWhateverTheRate(t *testing.T) {
	limiters := newNamespaceLimiters(limitedConfig(nil, map[string]int{"slow": 1}, nil))
	limits := stream.DefaultLimits()
	now := time.Now()

	// One byte per second, and a batch of the largest admissible size still
	// goes through once: refusing it forever would make the rate a size cap.
	require.NoError(t, limiters.allowAppend("slow", now, 1, limits.MaxBatchBytes, limits))
	requireOverRate(t, limiters.allowAppend("slow", now, 1, 1, limits))

	// A batch over the cap is not the limiter's to refuse: the component names
	// the cap it broke instead.
	require.NoError(t, limiters.allowAppend("slow", now, 1, limits.MaxBatchBytes+1, limits))
}

func TestPollRateIsEnforcedAndZeroMeansUnlimited(t *testing.T) {
	limiters := newNamespaceLimiters(limitedConfig(nil, nil, map[string]int{"slow": 1, "free": 0}))
	now := time.Now()

	require.NoError(t, limiters.allowPoll("slow", now))
	requireOverRate(t, limiters.allowPoll("slow", now))
	require.NoError(t, limiters.allowPoll("slow", now.Add(time.Second)))

	for range 10_000 {
		require.NoError(t, limiters.allowPoll("free", now))
	}
}

func TestZeroAppendRatesAreNotEnforced(t *testing.T) {
	limiters := newNamespaceLimiters(limitedConfig(
		map[string]int{"free": 0}, map[string]int{"free": 0}, nil))
	limits := stream.DefaultLimits()
	now := time.Now()
	for range 100 {
		require.NoError(t, limiters.allowAppend(
			"free", now, stream.MaxRecordsPerBatch, limits.MaxBatchBytes, limits))
	}
}
