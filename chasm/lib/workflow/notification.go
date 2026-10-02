package workflow

import (
	"maps"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common"
)

// SubscribedToChannel reports whether this run subscribed to the channel.
// The channel's fan-out asks, through a probe of the run, before it delivers,
// and a run that does not answer yes is dropped from the channel.
func (w *Workflow) SubscribedToChannel(channel string) bool {
	_, ok := w.ChannelSubscriptions[channel]
	return ok
}

// ChannelSubscriptionCount is how many channels this run subscribed to,
// counting those its open Workflow Task is about to register.
func (w *Workflow) ChannelSubscriptionCount() int {
	return len(w.ChannelSubscriptions)
}

// RecordChannelSubscription records that this run subscribed to a channel,
// at the event that says so. A second subscription to the same channel keeps
// the first record. The limit bounds what one run may hold; zero means none.
//
// It reports whether the subscription is new, which is when the channel has
// to be told.
func (w *Workflow) RecordChannelSubscription(
	mctx chasm.MutableContext,
	channel string,
	eventID int64,
	limit int,
) (bool, error) {
	if _, ok := w.ChannelSubscriptions[channel]; ok {
		return false, nil
	}
	if limit > 0 && len(w.ChannelSubscriptions) >= limit {
		return false, serviceerror.NewResourceExhaustedf(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			"workflow already subscribes to %d notification channels, which is the limit",
			len(w.ChannelSubscriptions))
	}
	if w.ChannelSubscriptions == nil {
		w.ChannelSubscriptions = make(chasm.Map[string, *chasmworkflowpb.ChannelSubscription])
	}
	w.ChannelSubscriptions[channel] = chasm.NewDataField(mctx,
		&chasmworkflowpb.ChannelSubscription{EventId: eventID})
	return true, nil
}

// StageChannelRegistration queues a channel for the completion path to
// register this run on. A command handler holds the state lock and cannot
// reach the channel's shard, so the registration happens after the commands
// and before the commit.
func (w *Workflow) StageChannelRegistration(channel string) {
	w.pendingChannelRegistrations = append(w.pendingChannelRegistrations, channel)
}

// DrainChannelRegistrations returns and clears the staged registrations.
func (w *Workflow) DrainChannelRegistrations() []string {
	out := w.pendingChannelRegistrations
	w.pendingChannelRegistrations = nil
	return out
}

// ChannelNotificationIsDuplicate reports whether accepting the notification
// would change nothing: the run already accepted this counter or a higher one
// from the channel, or does not listen to it at all.
func (w *Workflow) ChannelNotificationIsDuplicate(
	ctx chasm.Context,
	channel string,
	counter int64,
) bool {
	field, ok := w.ChannelSubscriptions[channel]
	if !ok {
		return true
	}
	return counter <= field.Get(readOnly(ctx)).GetLastCounter()
}

// AcceptChannelNotification records a notification from a channel this run
// listens to, for the next WorkflowTaskScheduled event to carry. Nothing is
// written to History here; the transaction close schedules the Workflow Task
// whose scheduled event carries it.
//
// One entry per channel. A newer notification replaces one no scheduled event
// has carried yet, and the report says it folded. A counter at or below the
// highest this run accepted is dropped: a run may already have that one in
// History, and the channel redelivers after a fan-out it has to retry.
func (w *Workflow) AcceptChannelNotification(
	mctx chasm.MutableContext,
	n *channelpb.Notification,
) (bool, error) {
	channel := n.GetChannel()
	field, ok := w.ChannelSubscriptions[channel]
	if !ok {
		return false, serviceerror.NewFailedPreconditionf(
			"workflow does not listen to notification channel %q", channel)
	}
	if n.GetCounter() <= field.Get(readOnly(mctx)).GetLastCounter() {
		return false, nil
	}
	field.Get(mctx).LastCounter = n.GetCounter()

	entry := &chasmworkflowpb.ChannelNotificationEntry{
		Position: n.GetPosition(),
		Counter:  n.GetCounter(),
		Metadata: common.CloneProto(n).GetMetadata(),
	}
	if w.ChannelNotifications == nil {
		w.ChannelNotifications = make(chasm.Map[string, *chasmworkflowpb.ChannelNotificationEntry])
	}
	_, folded := w.ChannelNotifications[channel]
	w.ChannelNotifications[channel] = chasm.NewDataField(mctx, entry)
	return folded, nil
}

// HasPendingChannelNotifications reports whether a notification is waiting
// for a scheduled event. Only the keys are read, so nothing is loaded.
func (w *Workflow) HasPendingChannelNotifications() bool {
	return len(w.ChannelNotifications) > 0
}

// TakeChannelNotifications returns the pending notifications, one per
// channel and sorted by channel, and clears them. Called for a
// WorkflowTaskScheduled event that is being written to History, which is
// what acknowledges them: nothing is redelivered once an event carries it.
func (w *Workflow) TakeChannelNotifications(mctx chasm.MutableContext) []*channelpb.Notification {
	if len(w.ChannelNotifications) == 0 {
		return nil
	}
	view := readOnly(mctx)
	channels := slices.Sorted(maps.Keys(w.ChannelNotifications))
	out := make([]*channelpb.Notification, 0, len(channels))
	for _, channel := range channels {
		entry := w.ChannelNotifications[channel].Get(view)
		out = append(out, &channelpb.Notification{
			Channel:  channel,
			Position: entry.GetPosition(),
			Counter:  entry.GetCounter(),
			Metadata: entry.GetMetadata(),
		})
		delete(w.ChannelNotifications, channel)
	}
	return out
}
