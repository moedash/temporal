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
