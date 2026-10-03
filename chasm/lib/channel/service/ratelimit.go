package service

import (
	"fmt"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/channel"
	"go.temporal.io/server/common/quotas"
)

// limiterRefreshInterval is how long a changed rate takes to reach a namespace
// that already has a limiter.
const limiterRefreshInterval = 10 * time.Second

// notifyLimiters enforces the per-namespace notify rate on this host. One
// limiter per namespace, made on first use and refreshed from config, so a
// change lands without a restart. A rate of zero is not enforced.
type notifyLimiters struct {
	config *channel.Config

	mu          sync.Mutex
	byNamespace map[string]quotas.RateLimiter
}

func newNotifyLimiters(config *channel.Config) *notifyLimiters {
	return &notifyLimiters{config: config, byNamespace: make(map[string]quotas.RateLimiter)}
}

func (l *notifyLimiters) forNamespace(ns string) quotas.RateLimiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	if limiter, ok := l.byNamespace[ns]; ok {
		return limiter
	}
	rate := func() int { return max(l.config.NotifyPerSecond(ns), 0) }
	limiter := quotas.NewDynamicRateLimiter(quotas.NewRateBurst(
		func() float64 { return float64(rate()) },
		func() int { return max(rate(), 1) },
	), limiterRefreshInterval)
	l.byNamespace[ns] = limiter
	return limiter
}

func (l *notifyLimiters) allowNotify(ns string, now time.Time) error {
	rate := l.config.NotifyPerSecond(ns)
	if rate <= 0 || l.forNamespace(ns).AllowN(now, 1) {
		return nil
	}
	return &serviceerror.ResourceExhausted{
		Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
		Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: fmt.Sprintf(
			"namespace %q is over its limit of %d channel notifications per second", ns, rate),
	}
}
