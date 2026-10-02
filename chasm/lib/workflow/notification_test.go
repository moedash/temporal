package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
)

func channelNote(channel string, counter int64) *channelpb.Notification {
	return &channelpb.Notification{
		Channel:  channel,
		Counter:  counter,
		Position: []byte{byte(counter)},
		Metadata: map[string]*commonpb.Payload{"k": {Data: []byte{byte(counter)}}},
	}
}

func subscribe(t *testing.T, w *Workflow, ctx chasm.MutableContext, channel string) {
	t.Helper()
	added, err := w.RecordChannelSubscription(ctx, channel, 5, 0)
	require.NoError(t, err)
	require.True(t, added)
}

// Notifications fold to the highest counter per channel, and a scheduled
// event takes one per channel, sorted, and clears them.
func TestChannelNotificationsFoldAndSnapshot(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	w := &Workflow{}
	subscribe(t, w, ctx, "orders")
	subscribe(t, w, ctx, "billing")

	folded, err := w.AcceptChannelNotification(ctx, channelNote("orders", 1))
	require.NoError(t, err)
	require.False(t, folded)
	folded, err = w.AcceptChannelNotification(ctx, channelNote("orders", 3))
	require.NoError(t, err)
	require.True(t, folded)
	folded, err = w.AcceptChannelNotification(ctx, channelNote("orders", 2))
	require.NoError(t, err)
	require.False(t, folded, "a lower counter is dropped, not folded")
	_, err = w.AcceptChannelNotification(ctx, channelNote("billing", 7))
	require.NoError(t, err)
	require.True(t, w.HasPendingChannelNotifications())

	taken := w.TakeChannelNotifications(ctx)
	require.Len(t, taken, 2)
	require.Equal(t, "billing", taken[0].GetChannel())
	require.Equal(t, "orders", taken[1].GetChannel())
	require.Equal(t, int64(3), taken[1].GetCounter())
	require.Equal(t, []byte{3}, taken[1].GetPosition())
	require.Equal(t, []byte{3}, taken[1].GetMetadata()["k"].GetData())
	require.False(t, w.HasPendingChannelNotifications())
	require.Empty(t, w.TakeChannelNotifications(ctx))
}

// What a scheduled event carried is in History, so a redelivery of the same
// counter after the snapshot changes nothing.
func TestChannelNotificationRedeliveryIsDuplicate(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	w := &Workflow{}
	subscribe(t, w, ctx, "orders")
	_, err := w.AcceptChannelNotification(ctx, channelNote("orders", 4))
	require.NoError(t, err)
	w.TakeChannelNotifications(ctx)

	require.True(t, w.ChannelNotificationIsDuplicate(ctx, "orders", 4))
	require.False(t, w.ChannelNotificationIsDuplicate(ctx, "orders", 5))
	_, err = w.AcceptChannelNotification(ctx, channelNote("orders", 4))
	require.NoError(t, err)
	require.False(t, w.HasPendingChannelNotifications())
}

func TestChannelSubscriptionRules(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	w := &Workflow{}

	_, err := w.AcceptChannelNotification(ctx, channelNote("orders", 1))
	var precondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &precondition, "a run that did not subscribe takes nothing")
	require.True(t, w.ChannelNotificationIsDuplicate(ctx, "orders", 1))

	added, err := w.RecordChannelSubscription(ctx, "orders", 5, 1)
	require.NoError(t, err)
	require.True(t, added)
	added, err = w.RecordChannelSubscription(ctx, "orders", 9, 1)
	require.NoError(t, err)
	require.False(t, added, "a repeat keeps the first record")
	require.Equal(t, int64(5), w.ChannelSubscriptions["orders"].Get(ctx).GetEventId())
	_, err = w.RecordChannelSubscription(ctx, "billing", 10, 1)
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.True(t, w.SubscribedToChannel("orders"))
	require.False(t, w.SubscribedToChannel("billing"))

	w.StageChannelRegistration("orders")
	require.Equal(t, []string{"orders"}, w.DrainChannelRegistrations())
	require.Empty(t, w.DrainChannelRegistrations())
}
