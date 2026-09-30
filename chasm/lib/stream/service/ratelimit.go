package service

import (
	"fmt"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/stream"
	"go.temporal.io/server/common/quotas"
)

// limiterRefreshInterval is how long a changed rate takes to reach a namespace
// that already has limiters.
const limiterRefreshInterval = 10 * time.Second

// namespaceLimiters enforces the per-namespace stream rates: records and bytes
// appended per second, and polls per second. One set per namespace, made on
// first use and refreshed from config on an interval, so a change lands
// without a restart. A rate of zero is not enforced at all.
type namespaceLimiters struct {
	config *stream.Config

	mu          sync.Mutex
	byNamespace map[string]*limiterSet
}

type limiterSet struct {
	appendRecords quotas.RateLimiter
	appendBytes   quotas.RateLimiter
	polls         quotas.RateLimiter
}

func newNamespaceLimiters(config *stream.Config) *namespaceLimiters {
	return &namespaceLimiters{config: config, byNamespace: make(map[string]*limiterSet)}
}

func (l *namespaceLimiters) forNamespace(ns string) *limiterSet {
	l.mu.Lock()
	defer l.mu.Unlock()
	if set, ok := l.byNamespace[ns]; ok {
		return set
	}
	set := &limiterSet{
		appendRecords: newLimiter(
			func() int { return l.config.AppendRecordsPerSecond(ns) },
			func() int { return stream.MaxRecordsPerBatch }),
		appendBytes: newLimiter(
			func() int { return l.config.AppendBytesPerSecond(ns) },
			func() int { return l.config.LimitsFor(ns).MaxBatchBytes }),
		polls: newLimiter(
			func() int { return l.config.PollsPerSecond(ns) },
			func() int { return 1 }),
	}
	l.byNamespace[ns] = set
	return set
}

// newLimiter builds a limiter whose burst is one second of its rate and never
// less than minBurst. A burst below the batch cap would refuse a full batch
// forever, whatever the rate, so a single admissible batch always fits.
func newLimiter(rateFn func() int, minBurst func() int) quotas.RateLimiter {
	return quotas.NewDynamicRateLimiter(quotas.NewRateBurst(
		func() float64 { return float64(max(rateFn(), 0)) },
		func() int { return max(rateFn(), minBurst()) },
	), limiterRefreshInterval)
}

// allowAppend admits an append of the given size or refuses it. A batch over
// the namespace's own caps is left to the component, which refuses it with
// the cap it broke rather than with a rate it never had a chance against.
func (l *namespaceLimiters) allowAppend(
	ns string, now time.Time, records int, size int, limits stream.Limits,
) error {
	set := l.forNamespace(ns)
	if rate := l.config.AppendRecordsPerSecond(ns); rate > 0 &&
		records <= stream.MaxRecordsPerBatch && !set.appendRecords.AllowN(now, records) {
		return overRate(ns, rate, "records appended")
	}
	if rate := l.config.AppendBytesPerSecond(ns); rate > 0 &&
		size <= limits.MaxBatchBytes && !set.appendBytes.AllowN(now, size) {
		return overRate(ns, rate, "bytes appended")
	}
	return nil
}

// allowPoll admits one poll, blocking or not, or refuses it.
func (l *namespaceLimiters) allowPoll(ns string, now time.Time) error {
	if rate := l.config.PollsPerSecond(ns); rate > 0 && !l.forNamespace(ns).polls.AllowN(now, 1) {
		return overRate(ns, rate, "polls")
	}
	return nil
}

func overRate(ns string, rate int, what string) error {
	return &serviceerror.ResourceExhausted{
		Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
		Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: fmt.Sprintf(
			"namespace %q is over its stream limit of %d %s per second", ns, rate, what),
	}
}
