package service

import (
	"go.temporal.io/server/chasm"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"go.uber.org/fx"
)

// HistoryModule serves ChannelService on History. The routed client is here
// too: the fan-out runs on the channel's shard and has to reach each
// listener's, which this host may not own. The channel config comes from the
// workflow library module, which every service running this one has.
var HistoryModule = fx.Module(
	"channel-history",
	fx.Provide(
		channelpb.NewChannelServiceLayeredClient,
		newHandler,
		newFanOutTaskHandler,
		newCallbackTaskHandler,
		newCallbackBackoffTaskHandler,
		newIdleTaskHandler,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)

// FrontendModule gives the frontend the routed client its channel handlers
// forward to, and registers the component type.
var FrontendModule = fx.Module(
	"channel-frontend",
	fx.Provide(channelpb.NewChannelServiceLayeredClient),
	fx.Provide(newComponentOnlyLibrary),
	fx.Invoke(func(l *componentOnlyLibrary, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)
