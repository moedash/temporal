package service

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/grpc"
)

const (
	libraryName   = "stream"
	componentName = "stream"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type library struct {
	chasm.UnimplementedLibrary
	handler         *handler
	retention       *retentionTaskHandler
	notifyConsumers *notifyConsumersTaskHandler
}

func newLibrary(h *handler, retention *retentionTaskHandler, notifyConsumers *notifyConsumersTaskHandler) *library {
	return &library{handler: h, retention: retention, notifyConsumers: notifyConsumers}
}

// componentOnlyLibrary registers the component without the service, which is
// what the frontend needs in order to serialize component references.
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
		chasm.NewRegistrableComponent[*stream.Stream](
			componentName,
			chasm.WithBusinessIDAlias("StreamId"),
		),
		// Registered here rather than with the workflow library because it is
		// this package's type, even though it only ever hangs off a consuming
		// workflow.
		chasm.NewRegistrableComponent[*stream.Cursor]("streamCursor"),
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
		chasm.NewRegistrableSideEffectTask(
			"streamRetention",
			l.retention,
		),
		chasm.NewRegistrableSideEffectTask(
			"streamNotifyConsumers",
			l.notifyConsumers,
		),
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	streampb.RegisterStreamServiceServer(server, l.handler)
}
