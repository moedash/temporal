package stream

import (
	"go.temporal.io/server/chasm"
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
}

var Library = &library{}

func (l *library) Name() string {
	return libraryName
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Stream](
			componentName,
			chasm.WithBusinessIDAlias("StreamId"),
		),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return nil
}
