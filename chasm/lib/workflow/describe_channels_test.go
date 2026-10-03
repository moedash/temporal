package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	notificationpb "go.temporal.io/api/notification/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm/lib/channel"
)

func channelNames(infos []*workflowpb.ChannelSubscriptionInfo) []string {
	out := make([]string, len(infos))
	for i, info := range infos {
		out[i] = info.GetChannel()
	}
	return out
}

func infoOf(
	t *testing.T,
	infos []*workflowpb.ChannelSubscriptionInfo,
	name string,
	kind notificationpb.ChannelKind,
) *workflowpb.ChannelSubscriptionInfo {
	t.Helper()
	for _, info := range infos {
		if info.GetChannel() == name && info.GetKind() == kind {
			return info
		}
	}
	require.Failf(t, "channel not described", "%s %s in %v", kind, name, channelNames(infos))
	return nil
}

// Describe lists the subscribed channels and the linked channels that hold
// state, sorted by name with the subscribed kind first, and reads each as the
// run stands on it: what it accepted, what waits for a scheduled event, what
// the unstarted task carries.
func TestChannelSubscriptionInfos(t *testing.T) {
	ctx := newOwnerContext()
	w := &Workflow{}
	require.Empty(t, w.ChannelSubscriptionInfos(ctx))

	subscribe(t, w, ctx, "orders")
	subscribe(t, w, ctx, "billing")
	infos := w.ChannelSubscriptionInfos(ctx)
	require.Equal(t, []string{"billing", "orders"}, channelNames(infos))
	for _, info := range infos {
		require.Equal(t, notificationpb.CHANNEL_KIND_INDEPENDENT, info.GetKind())
		require.Equal(t, int64(5), info.GetSubscribedEventId())
		require.Zero(t, info.GetLastCounter())
		require.Nil(t, info.GetPendingNotification())
		require.Zero(t, info.GetScheduledCounter())
		require.Zero(t, info.GetListenerCount())
		require.Zero(t, info.GetRetainedCount())
		require.Zero(t, info.GetAcceptedCount())
	}

	// Accepted and not yet carried, a notification is pending. Taken by a
	// scheduled event it becomes the scheduled counter, and a task start
	// clears that. The last counter stays at what was accepted.
	_, err := w.AcceptChannelNotification(ctx, channelNote("orders", 3))
	require.NoError(t, err)
	independent := notificationpb.CHANNEL_KIND_INDEPENDENT
	info := infoOf(t, w.ChannelSubscriptionInfos(ctx), "orders", independent)
	require.Equal(t, int64(3), info.GetLastCounter())
	require.Zero(t, info.GetScheduledCounter())
	pending := info.GetPendingNotification()
	require.Equal(t, "orders", pending.GetChannel())
	require.Equal(t, int64(3), pending.GetCounter())
	require.Equal(t, []byte{3}, pending.GetPosition())
	require.Equal(t, []byte{3}, pending.GetMetadata()["k"].GetData())
	require.Nil(t, pending.GetLinkedTo())

	w.TakeChannelNotifications(ctx)
	info = infoOf(t, w.ChannelSubscriptionInfos(ctx), "orders", independent)
	require.Equal(t, int64(3), info.GetLastCounter())
	require.Nil(t, info.GetPendingNotification())
	require.Equal(t, int64(3), info.GetScheduledCounter())

	w.ClearScheduledChannelCounters(ctx)
	info = infoOf(t, w.ChannelSubscriptionInfos(ctx), "orders", independent)
	require.Equal(t, int64(3), info.GetLastCounter())
	require.Zero(t, info.GetScheduledCounter())

	// A linked channel under a subscribed name lists after it, one under a
	// name of its own sorts among the rest, and its counts are the channel's.
	for _, n := range []struct {
		name    string
		counter int64
	}{{"orders", 1}, {"orders", 2}, {"alpha", 9}} {
		_, err = w.NotifyLinkedChannel(ctx, channelNote(n.name, n.counter), channel.Limits{})
		require.NoError(t, err)
	}
	infos = w.ChannelSubscriptionInfos(ctx)
	require.Equal(t, []string{"alpha", "billing", "orders", "orders"}, channelNames(infos))
	require.Equal(t, notificationpb.CHANNEL_KIND_LINKED, infos[0].GetKind())
	require.Equal(t, independent, infos[2].GetKind())
	linked := infos[3]
	require.Equal(t, notificationpb.CHANNEL_KIND_LINKED, linked.GetKind())
	require.Zero(t, linked.GetSubscribedEventId())
	require.Equal(t, int64(2), linked.GetLastCounter())
	require.Equal(t, int64(2), linked.GetPendingNotification().GetCounter())
	require.Equal(t, "owner", linked.GetPendingNotification().GetLinkedTo().GetBusinessId())
	require.Equal(t, "run-1", linked.GetPendingNotification().GetLinkedTo().GetRunId())
	require.Zero(t, linked.GetScheduledCounter())
	require.Zero(t, linked.GetListenerCount())
	require.Equal(t, int32(2), linked.GetRetainedCount())
	require.Equal(t, int64(2), linked.GetAcceptedCount())

	w.TakeChannelNotifications(ctx)
	linked = infoOf(t, w.ChannelSubscriptionInfos(ctx), "orders", notificationpb.CHANNEL_KIND_LINKED)
	require.Nil(t, linked.GetPendingNotification())
	require.Equal(t, int64(2), linked.GetScheduledCounter())
	require.Equal(t, int64(2), linked.GetLastCounter())
}

// What describe returns is a copy: a caller that changes it changes nothing
// the run holds.
func TestChannelSubscriptionInfosAreCopies(t *testing.T) {
	ctx := newOwnerContext()
	w := &Workflow{}
	subscribe(t, w, ctx, "orders")
	_, err := w.AcceptChannelNotification(ctx, channelNote("orders", 1))
	require.NoError(t, err)
	_, err = w.NotifyLinkedChannel(ctx, channelNote("billing", 1), channel.Limits{})
	require.NoError(t, err)

	for _, info := range w.ChannelSubscriptionInfos(ctx) {
		info.GetPendingNotification().GetMetadata()["k"].Data[0] = 42
		info.GetPendingNotification().Position[0] = 42
	}
	for _, n := range w.TakeChannelNotifications(ctx) {
		require.Equal(t, []byte{1}, n.GetPosition(), "position of %q", n.GetChannel())
		require.Equal(t, []byte{1}, n.GetMetadata()["k"].GetData(), "metadata of %q", n.GetChannel())
	}
}
