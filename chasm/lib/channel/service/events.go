package service

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/service/history/hsm"
)

// channelSubscribedEventDefinition tells the history service how to treat the
// event a subscribe command writes. The subscription itself is CHASM state on
// the Workflow component, so there is nothing here to apply.
type channelSubscribedEventDefinition struct{}

func (channelSubscribedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_SUBSCRIBED
}

// Subscribing gives the workflow nothing to decide on. A notification does,
// and it schedules its own Workflow Task.
func (channelSubscribedEventDefinition) IsWorkflowTaskTrigger() bool { return false }

func (channelSubscribedEventDefinition) Apply(*hsm.Node, *historypb.HistoryEvent) error {
	return nil
}

// A command event, so never reapplied onto another branch.
func (channelSubscribedEventDefinition) CherryPick(
	*hsm.Node,
	*historypb.HistoryEvent,
	map[enumspb.ResetReapplyExcludeType]struct{},
) error {
	return hsm.ErrNotCherryPickable
}

// RegisterEventDefinitions makes the channel event known to the history
// service.
func RegisterEventDefinitions(reg *hsm.Registry) error {
	return reg.RegisterEventDefinition(channelSubscribedEventDefinition{})
}
