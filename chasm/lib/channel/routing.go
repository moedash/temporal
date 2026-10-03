package channel

import (
	"context"

	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
)

type clientContextKey struct{}

// WithClient supplies shard routing for a call that reaches a channel from
// inside a workflow task's transaction, which runs on the workflow's shard.
func WithClient(ctx context.Context, client channelpb.ChannelServiceClient) context.Context {
	if client == nil {
		return ctx
	}
	return context.WithValue(ctx, clientContextKey{}, client)
}

// ClientFromContext returns the routed client installed by the engine.
func ClientFromContext(ctx context.Context) (channelpb.ChannelServiceClient, bool) {
	client, ok := ctx.Value(clientContextKey{}).(channelpb.ChannelServiceClient)
	return client, ok
}
