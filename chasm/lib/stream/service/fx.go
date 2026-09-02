package service

import (
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"stream-history",
	fx.Provide(
		// Routes a call to the host owning a shard. History needs it too, not
		// just the frontend: a step that spans two executions has to reach a
		// shard this host may not own.
		streampb.NewStreamServiceLayeredClient,
		newHandler,
		newRetentionTaskHandler,
		newNotifyConsumersTaskHandler,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
	fx.Invoke(RegisterEventDefinitions),
)

var FrontendModule = fx.Module(
	"stream-frontend",
	fx.Provide(streampb.NewStreamServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
	fx.Provide(newComponentOnlyLibrary),
	fx.Invoke(func(l *componentOnlyLibrary, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)
