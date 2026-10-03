package workflow

import (
	"maps"
	"slices"
	"strings"

	notificationpb "go.temporal.io/api/notification/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common"
)

// ChannelSubscriptionInfos describes the notification channels this run
// stands on, for DescribeWorkflowExecution: the channels it subscribed to and
// the channels linked to it that hold any state, sorted by name with the
// subscribed kind first when a name is in both. A linked name nobody has
// notified, registered on or polled is not listed: it exists by construction
// and holds nothing. A subscribed channel's listener and retained counts are
// zero here, since they belong to the channel's own execution.
//
// Everything returned is a copy, as the response is marshalled after the
// run's lock is released.
func (w *Workflow) ChannelSubscriptionInfos(
	ctx chasm.Context,
) []*workflowpb.ChannelSubscriptionInfo {
	view := readOnly(ctx)
	out := make([]*workflowpb.ChannelSubscriptionInfo, 0,
		len(w.ChannelSubscriptions)+len(w.LinkedChannels))
	for _, name := range slices.Sorted(maps.Keys(w.ChannelSubscriptions)) {
		sub := w.ChannelSubscriptions[name].Get(view)
		info := &workflowpb.ChannelSubscriptionInfo{
			Channel:           name,
			Kind:              notificationpb.CHANNEL_KIND_INDEPENDENT,
			SubscribedEventId: sub.GetEventId(),
			LastCounter:       sub.GetLastCounter(),
			ScheduledCounter:  sub.GetScheduledCounter(),
		}
		if field, ok := w.ChannelNotifications[name]; ok {
			info.PendingNotification = pendingNotification(name, field.Get(view))
		}
		out = append(out, info)
	}
	for _, name := range slices.Sorted(maps.Keys(w.LinkedChannels)) {
		standing := w.LinkedChannels[name].Get(view).OwnerStanding()
		out = append(out, &workflowpb.ChannelSubscriptionInfo{
			Channel:             name,
			Kind:                notificationpb.CHANNEL_KIND_LINKED,
			LastCounter:         standing.LastCounter,
			PendingNotification: channel.ToAPINotification(standing.Pending),
			ScheduledCounter:    standing.ScheduledCounter,
			ListenerCount:       int32(standing.ListenerCount),
			RetainedCount:       int32(standing.RetainedCount),
			AcceptedCount:       standing.AcceptedCount,
		})
	}
	slices.SortStableFunc(out, func(a, b *workflowpb.ChannelSubscriptionInfo) int {
		return strings.Compare(a.GetChannel(), b.GetChannel())
	})
	return out
}

// pendingNotification is the public form of a pending entry, which is stored
// without the channel name its map key carries.
func pendingNotification(
	name string,
	entry *chasmworkflowpb.ChannelNotificationEntry,
) *notificationpb.Notification {
	entry = common.CloneProto(entry)
	return &notificationpb.Notification{
		Channel:  name,
		Position: entry.GetPosition(),
		Counter:  entry.GetCounter(),
		Metadata: entry.GetMetadata(),
	}
}
