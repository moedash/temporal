package workflow

import (
	"fmt"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
)

// handleSubscribeNotificationChannelCommand makes this run a listener of a
// notification channel.
//
// The event is written and the subscription recorded here, with the command,
// so the event sits in command order. The channel lives on its own shard,
// which a command handler cannot reach while it holds the state lock, so the
// registration there is staged for the completion path, which makes it before
// the commit and fails the task if the channel refuses.
func handleSubscribeNotificationChannelCommand(
	chasmCtx chasm.MutableContext,
	wf *Workflow,
	command *commandpb.Command,
	opts CommandHandlerOptions,
	maxIDLength int,
	maxSubscriptions int,
) error {
	badAttributes := enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SUBSCRIBE_NOTIFICATION_CHANNEL_ATTRIBUTES
	attrs := command.GetSubscribeNotificationChannelCommandAttributes()
	if attrs == nil {
		return FailWorkflowTaskError{
			Cause:   badAttributes,
			Message: "SubscribeNotificationChannelCommandAttributes is not set",
		}
	}
	name := attrs.GetChannel()
	if err := channel.CheckChannelName(name, maxIDLength); err != nil {
		return FailWorkflowTaskError{Cause: badAttributes, Message: err.Error()}
	}

	// A run already subscribed still gets the event, since every SDK matches
	// the commands it issued against the events they produced, in order. It
	// changes nothing else: no new listener and no latest notification.
	already := wf.SubscribedToChannel(name)

	// Refused before the event is written, so a refusal leaves nothing behind
	// even before the failed task is rolled back.
	if !already && maxSubscriptions > 0 && wf.ChannelSubscriptionCount() >= maxSubscriptions {
		return FailWorkflowTaskError{
			Cause: badAttributes,
			Message: fmt.Sprintf(
				"workflow already subscribes to %d notification channels, the limit", maxSubscriptions),
		}
	}

	event := wf.AddHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_SUBSCRIBED,
		func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_WorkflowNotificationChannelSubscribedEventAttributes{
				WorkflowNotificationChannelSubscribedEventAttributes: &historypb.
					WorkflowNotificationChannelSubscribedEventAttributes{
					WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
					Channel:                      name,
				},
			}
		})
	if already {
		return nil
	}
	if _, err := wf.RecordChannelSubscription(chasmCtx, name, event.GetEventId(), 0); err != nil {
		return err
	}
	wf.StageChannelRegistration(name)
	return nil
}

// channelSubscribedEvent is the event a subscribe command writes. It is also
// what puts the subscription back on a run rebuilt from History, which is how
// a reset run and a run replicated by events come to listen.
type channelSubscribedEvent struct{}

func (channelSubscribedEvent) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_SUBSCRIBED
}

func (channelSubscribedEvent) IsWorkflowTaskTrigger() bool { return false }

// Apply records the subscription. A second event for the same channel keeps
// the first record, so the run's first subscription is the one on record.
func (channelSubscribedEvent) Apply(
	mctx chasm.MutableContext,
	wf *Workflow,
	event *historypb.HistoryEvent,
) error {
	attrs := event.GetWorkflowNotificationChannelSubscribedEventAttributes()
	_, err := wf.RecordChannelSubscription(mctx, attrs.GetChannel(), event.GetEventId(), 0)
	return err
}

// A command event, so it is never cherry-picked: the workflow reissues the
// subscribe command on the new branch if it still wants one.
func (channelSubscribedEvent) CherryPick(
	chasm.MutableContext,
	*Workflow,
	*historypb.HistoryEvent,
	map[enumspb.ResetReapplyExcludeType]struct{},
) error {
	return ErrEventNotCherryPickable
}

// handleUnsubscribeNotificationChannelCommand ends this run's subscription to
// a notification channel. The event is written here, with the command, and
// the record and its pending notification go with it. Dropping the run from
// the channel's listeners is staged for the completion path, as the
// registration was. A notification a scheduled event already carries stays in
// History, and a delivery that reaches the run afterwards finds no
// subscription and is dropped.
func handleUnsubscribeNotificationChannelCommand(
	chasmCtx chasm.MutableContext,
	wf *Workflow,
	command *commandpb.Command,
	opts CommandHandlerOptions,
	maxIDLength int,
) error {
	badAttributes := enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UNSUBSCRIBE_NOTIFICATION_CHANNEL_ATTRIBUTES
	attrs := command.GetUnsubscribeNotificationChannelCommandAttributes()
	if attrs == nil {
		return FailWorkflowTaskError{
			Cause:   badAttributes,
			Message: "UnsubscribeNotificationChannelCommandAttributes is not set",
		}
	}
	name := attrs.GetChannel()
	if err := channel.CheckChannelName(name, maxIDLength); err != nil {
		return FailWorkflowTaskError{Cause: badAttributes, Message: err.Error()}
	}

	// A run that does not listen still gets the event, for the reason a
	// duplicate subscribe does: every SDK matches the commands it issued
	// against the events they produced, in order. The linked kind has no
	// subscription, so a command naming a linked channel is this case.
	subscribedEventID, subscribed := wf.RemoveChannelSubscription(chasmCtx, name)
	wf.AddHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_UNSUBSCRIBED,
		func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_WorkflowNotificationChannelUnsubscribedEventAttributes{
				WorkflowNotificationChannelUnsubscribedEventAttributes: &historypb.
					WorkflowNotificationChannelUnsubscribedEventAttributes{
					WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
					Channel:                      name,
					SubscribedEventId:            subscribedEventID,
				},
			}
		})
	if subscribed {
		wf.StageChannelDeregistration(name)
	}
	return nil
}

// channelUnsubscribedEvent is the event an unsubscribe command writes. On a
// run rebuilt from History it takes the subscription off again, so a reset
// run ends with the subscriptions the source had at the reset point.
type channelUnsubscribedEvent struct{}

func (channelUnsubscribedEvent) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_UNSUBSCRIBED
}

func (channelUnsubscribedEvent) IsWorkflowTaskTrigger() bool { return false }

// Apply removes the subscription. One the run does not hold is nothing to
// remove, as for the command.
func (channelUnsubscribedEvent) Apply(
	mctx chasm.MutableContext,
	wf *Workflow,
	event *historypb.HistoryEvent,
) error {
	attrs := event.GetWorkflowNotificationChannelUnsubscribedEventAttributes()
	wf.RemoveChannelSubscription(mctx, attrs.GetChannel())
	return nil
}

// A command event, so it is never cherry-picked.
func (channelUnsubscribedEvent) CherryPick(
	chasm.MutableContext,
	*Workflow,
	*historypb.HistoryEvent,
	map[enumspb.ResetReapplyExcludeType]struct{},
) error {
	return ErrEventNotCherryPickable
}

// notificationLibrary registers the subscribe and unsubscribe commands with
// the workflow registry.
type notificationLibrary struct {
	workflowConfig Config
	channelConfig  *channel.Config
}

func newNotificationLibrary(workflowConfig Config, channelConfig *channel.Config) *notificationLibrary {
	return &notificationLibrary{workflowConfig: workflowConfig, channelConfig: channelConfig}
}

func (l *notificationLibrary) CommandHandlers() map[enumspb.CommandType]CommandHandler {
	return map[enumspb.CommandType]CommandHandler{
		enumspb.COMMAND_TYPE_SUBSCRIBE_NOTIFICATION_CHANNEL: func(
			chasmCtx chasm.MutableContext,
			wf *Workflow,
			_ Validator,
			command *commandpb.Command,
			opts CommandHandlerOptions,
		) error {
			namespaceName := chasmCtx.NamespaceEntry().Name().String()
			return handleSubscribeNotificationChannelCommand(
				chasmCtx, wf, command, opts,
				l.workflowConfig.maxIDLengthLimit(),
				l.channelConfig.MaxSubscriptionsPerWorkflow(namespaceName),
			)
		},
		enumspb.COMMAND_TYPE_UNSUBSCRIBE_NOTIFICATION_CHANNEL: func(
			chasmCtx chasm.MutableContext,
			wf *Workflow,
			_ Validator,
			command *commandpb.Command,
			opts CommandHandlerOptions,
		) error {
			return handleUnsubscribeNotificationChannelCommand(
				chasmCtx, wf, command, opts, l.workflowConfig.maxIDLengthLimit())
		},
	}
}

func (l *notificationLibrary) EventDefinitions() []EventDefinition {
	return []EventDefinition{channelSubscribedEvent{}, channelUnsubscribedEvent{}}
}
