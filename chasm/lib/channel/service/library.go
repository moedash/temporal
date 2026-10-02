package service

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"google.golang.org/grpc"
)

const (
	libraryName   = "channel"
	componentName = "channel"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type library struct {
	chasm.UnimplementedLibrary

	handler  *handler
	fanOut   *fanOutTaskHandler
	callback *callbackTaskHandler
	backoff  *callbackBackoffTaskHandler
	idle     *idleTaskHandler
}

func newLibrary(
	h *handler,
	fanOut *fanOutTaskHandler,
	callback *callbackTaskHandler,
	backoff *callbackBackoffTaskHandler,
	idle *idleTaskHandler,
) *library {
	return &library{handler: h, fanOut: fanOut, callback: callback, backoff: backoff, idle: idle}
}

// componentOnlyLibrary registers the component without the service, for a
// process that has to know the type but serves nothing.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

func newComponentOnlyLibrary() *componentOnlyLibrary {
	return &componentOnlyLibrary{}
}

func (l *componentOnlyLibrary) Name() string {
	return libraryName
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return components()
}

func components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*channel.Channel](
			componentName,
			chasm.WithBusinessIDAlias("Channel"),
		),
	}
}

func (l *library) Name() string {
	return libraryName
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return components()
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask("fanOut", l.fanOut),
		chasm.NewRegistrableSideEffectTask("callback", l.callback),
		chasm.NewRegistrablePureTask("callbackBackoff", l.backoff),
		chasm.NewRegistrableSideEffectTask("idle", l.idle),
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	channelpb.RegisterChannelServiceServer(server, l.handler)
}
