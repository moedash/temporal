package service

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/service/history/hsm"
)

// streamSubscribedEventDefinition tells the history service how to treat the
// event a subscribe command writes.
//
// It applies nothing. The cursor lives in CHASM state, which is persisted and
// rebuilt with the execution, so replication and reset have nothing to
// reconstruct from the event. It exists so the command has an event at all:
// every SDK matches issued commands against command-generated events in order,
// and a command producing none puts that matching out of step.
type streamSubscribedEventDefinition struct{}

func (streamSubscribedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_WORKFLOW_STREAM_SUBSCRIBED
}

// Subscribing does not itself give the workflow anything to decide on. The
// range that follows does, and that wakes the workflow through its cursor.
func (streamSubscribedEventDefinition) IsWorkflowTaskTrigger() bool { return false }

func (streamSubscribedEventDefinition) Apply(*hsm.Node, *historypb.HistoryEvent) error {
	return nil
}

// A command event, so never reapplied onto another branch: a workflow that
// still wants the subscription issues the command again on the new one.
func (streamSubscribedEventDefinition) CherryPick(
	*hsm.Node,
	*historypb.HistoryEvent,
	map[enumspb.ResetReapplyExcludeType]struct{},
) error {
	return hsm.ErrNotCherryPickable
}

// streamMessagesAddedEventDefinition tells the history service how to treat
// the event a publish command writes.
//
// Like the subscription event it applies nothing: the stream's frontier is
// CHASM state committed with the workflow task, and the bodies are in the
// stream's own log. What the event carries is the offset range, which is what
// lets anyone reading History find the batch without History having held it.
type streamMessagesAddedEventDefinition struct{}

func (streamMessagesAddedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_WORKFLOW_STREAM_MESSAGES_ADDED
}

// A workflow publishing to its own stream has nothing to be woken about.
func (streamMessagesAddedEventDefinition) IsWorkflowTaskTrigger() bool { return false }

func (streamMessagesAddedEventDefinition) Apply(*hsm.Node, *historypb.HistoryEvent) error {
	return nil
}

// A command event, so never reapplied onto another branch. Reapplying would
// claim offsets in a log the new branch never wrote to.
func (streamMessagesAddedEventDefinition) CherryPick(
	*hsm.Node,
	*historypb.HistoryEvent,
	map[enumspb.ResetReapplyExcludeType]struct{},
) error {
	return hsm.ErrNotCherryPickable
}

// RegisterEventDefinitions makes the stream events known to the history
// service.
func RegisterEventDefinitions(reg *hsm.Registry) error {
	if err := reg.RegisterEventDefinition(streamSubscribedEventDefinition{}); err != nil {
		return err
	}
	return reg.RegisterEventDefinition(streamMessagesAddedEventDefinition{})
}
