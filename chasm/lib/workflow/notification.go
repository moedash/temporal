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

// readOnly hides the mutable half of a context. Reaching a data field through
// a mutable context marks it for persistence, and the subscription and
// notification tables are read on paths that must leave an execution
// untouched when nothing changes.
func readOnly(ctx chasm.Context) chasm.Context {
	return struct{ chasm.Context }{ctx}
}

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

// RemoveChannelSubscription forgets this run's subscription to the channel
// and the notification waiting on it, and reports the event that recorded
// the subscription. A channel the run does not listen to reports false and
// changes nothing. A notification a scheduled event already carries stays in
// History.
func (w *Workflow) RemoveChannelSubscription(
	mctx chasm.MutableContext,
	channel string,
) (int64, bool) {
	field, ok := w.ChannelSubscriptions[channel]
	if !ok {
		return 0, false
	}
	eventID := field.Get(readOnly(mctx)).GetEventId()
	delete(w.ChannelSubscriptions, channel)
	delete(w.ChannelNotifications, channel)
	return eventID, true
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

// StageChannelDeregistration queues a channel for the completion path to drop
// this run from, for the reason the registration is staged.
func (w *Workflow) StageChannelDeregistration(channel string) {
	w.pendingChannelDeregistrations = append(w.pendingChannelDeregistrations, channel)
}

// DrainChannelDeregistrations returns and clears the staged deregistrations.
func (w *Workflow) DrainChannelDeregistrations() []string {
	out := w.pendingChannelDeregistrations
	w.pendingChannelDeregistrations = nil
	return out
}

// ChannelNotificationIsDuplicate reports whether accepting the notification
// would change nothing: the run holds a pending notification from the channel
// at this counter or a higher one, its scheduled and not yet started task
// carries one, or it does not listen to the channel at all.
//
// A started task does not count. It may have read its source before the
// write the repeat stands for, so the same counter arriving then is a new
// reason to run: a watcher that finds a record with no task open sends the
// counter it already sent, and folding that away would leave its reader
// waiting.
func (w *Workflow) ChannelNotificationIsDuplicate(
	ctx chasm.Context,
	channel string,
	counter int64,
) bool {
	subscription, ok := w.ChannelSubscriptions[channel]
	if !ok {
		return true
	}
	view := readOnly(ctx)
	if counter <= subscription.Get(view).GetScheduledCounter() {
		return true
	}
	field, ok := w.ChannelNotifications[channel]
	return ok && counter <= field.Get(view).GetCounter()
}

// AcceptChannelNotification records a notification from a channel this run
// listens to, for the next WorkflowTaskScheduled event to carry. Nothing is
// written to History here; the transaction close schedules the Workflow Task
// whose scheduled event carries it.
//
// One entry per channel. A notification at or below the pending entry's
// counter is folded into it and changes nothing; a higher one replaces it;
// with no pending entry it starts one, whatever its counter. It reports
// whether the notification folded into a pending entry.
func (w *Workflow) AcceptChannelNotification(
	mctx chasm.MutableContext,
	n *channelpb.Notification,
) (bool, error) {
	channel := n.GetChannel()
	subscription, ok := w.ChannelSubscriptions[channel]
	if !ok {
		return false, serviceerror.NewFailedPreconditionf(
			"workflow does not listen to notification channel %q", channel)
	}
	if w.ChannelNotificationIsDuplicate(mctx, channel, n.GetCounter()) {
		return true, nil
	}
	if n.GetCounter() > subscription.Get(readOnly(mctx)).GetLastCounter() {
		subscription.Get(mctx).LastCounter = n.GetCounter()
	}

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
// for a scheduled event, from a channel the run subscribed to or one linked
// to it. The subscribed kind is answered from the keys alone; the linked
// kind keeps its pending entry on the channel, so those are read.
func (w *Workflow) HasPendingChannelNotifications(ctx chasm.Context) bool {
	if len(w.ChannelNotifications) > 0 {
		return true
	}
	view := readOnly(ctx)
	for _, field := range w.LinkedChannels {
		if field.Get(view).HasOwnerPending() {
			return true
		}
	}
	return false
}

// TakeChannelNotifications returns the pending notifications, one per
// channel and sorted by channel, and clears them. Called for a
// WorkflowTaskScheduled event that is being written to History, which is
// what acknowledges them: nothing is redelivered once an event carries it.
// Each channel's subscription remembers the counter the event carries until
// the task starts, so a repeat in that window folds into the task.
func (w *Workflow) TakeChannelNotifications(mctx chasm.MutableContext) []*channelpb.Notification {
	if len(w.ChannelNotifications) == 0 {
		return w.takeLinkedNotifications(mctx)
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
		if sub, ok := w.ChannelSubscriptions[channel]; ok {
			sub.Get(mctx).ScheduledCounter = entry.GetCounter()
		}
	}
	return append(out, w.takeLinkedNotifications(mctx)...)
}

// takeLinkedNotifications takes the pending notification of every linked
// channel that holds one, sorted by channel. Each names its owner, so a
// worker can tell it from one of a channel the run subscribed to.
func (w *Workflow) takeLinkedNotifications(mctx chasm.MutableContext) []*channelpb.Notification {
	view := readOnly(mctx)
	var out []*channelpb.Notification
	for _, name := range slices.Sorted(maps.Keys(w.LinkedChannels)) {
		field := w.LinkedChannels[name]
		if !field.Get(view).HasOwnerPending() {
			continue
		}
		out = append(out, field.Get(mctx).TakeOwnerPending())
	}
	return out
}

// HasScheduledChannelCounters reports whether any subscription remembers what
// a scheduled task carries.
func (w *Workflow) HasScheduledChannelCounters(ctx chasm.Context) bool {
	view := readOnly(ctx)
	for _, field := range w.ChannelSubscriptions {
		if field.Get(view).GetScheduledCounter() > 0 {
			return true
		}
	}
	for _, field := range w.LinkedChannels {
		if field.Get(view).HasOwnerScheduledCounter() {
			return true
		}
	}
	return false
}

// ClearScheduledChannelCounters forgets what the scheduled task carries.
// Called when the task starts, fails or times out: from then on a repeat
// cannot be assumed to reach a task that has not read yet.
func (w *Workflow) ClearScheduledChannelCounters(mctx chasm.MutableContext) {
	view := readOnly(mctx)
	for _, field := range w.ChannelSubscriptions {
		if field.Get(view).GetScheduledCounter() > 0 {
			field.Get(mctx).ScheduledCounter = 0
		}
	}
	for _, field := range w.LinkedChannels {
		if field.Get(view).HasOwnerScheduledCounter() {
			field.Get(mctx).ClearOwnerScheduledCounter()
		}
	}
}
