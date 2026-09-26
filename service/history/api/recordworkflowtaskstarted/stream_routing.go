package recordworkflowtaskstarted

import (
	"context"

	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

type streamClientContextKey struct{}

// WithStreamClient supplies shard routing for calls that reach a stream in
// another execution from inside a workflow task's transaction.
func WithStreamClient(ctx context.Context, client streamlib.StreamServiceClient) context.Context {
	if client == nil {
		return ctx
	}
	return context.WithValue(ctx, streamClientContextKey{}, client)
}

// StreamClientFromContext returns the routed client installed by the engine.
func StreamClientFromContext(ctx context.Context) (streamlib.StreamServiceClient, bool) {
	client, ok := ctx.Value(streamClientContextKey{}).(streamlib.StreamServiceClient)
	return client, ok
}

// WithRoutedDeadline bounds a routed call made while the workflow lock is held.
func WithRoutedDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, stream.RoutedCallTimeout)
}

// WithRoutedBudget bounds a whole set of routed calls made under one workflow
// lock.
//
// Each call has a deadline of its own, but a task can carry as many of them as
// the subscription limit allows, and per-call deadlines multiplied by that
// count is how long the lock could be held. One budget over the set is what
// actually bounds it: a call that runs out of it fails, and the workflow task
// fails with it rather than the lock being held for minutes.
func WithRoutedBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, stream.RoutedSetBudget)
}
