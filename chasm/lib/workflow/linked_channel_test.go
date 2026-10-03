package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
)

func newOwnerContext() *chasm.MockMutableContext {
	now := time.Unix(1_700_000_000, 0).UTC()
	return &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return now },
		HandleExecutionKey: func() chasm.ExecutionKey {
			return chasm.ExecutionKey{NamespaceID: "ns", BusinessID: "owner", RunID: "run-1"}
		},
	}}
}

// A linked channel is created on the run by its first notify, bounded in
// number, and its pending notification is taken by the scheduled event
// beside those of the channels the run subscribed to, naming the owner.
func TestLinkedChannelsOnTheOwner(t *testing.T) {
	ctx := newOwnerContext()
	w := &Workflow{}
	require.False(t, w.HasPendingChannelNotifications(ctx))

	result, err := w.NotifyLinkedChannel(ctx, channelNote("orders", 1), channel.Limits{})
	require.NoError(t, err)
	require.True(t, result.Advanced)
	require.Equal(t, 1, w.LinkedChannelCount())
	require.True(t, w.HasPendingChannelNotifications(ctx))
	require.True(t, w.LinkedChannelHolds(ctx, "orders", 1))
	require.False(t, w.LinkedChannelHolds(ctx, "orders", 2))
	require.False(t, w.LinkedChannelHolds(ctx, "billing", 1), "no such channel on the run")

	subscribe(t, w, ctx, "billing")
	_, err = w.AcceptChannelNotification(ctx, channelNote("billing", 7))
	require.NoError(t, err)

	taken := w.TakeChannelNotifications(ctx)
	require.Len(t, taken, 2)
	require.Equal(t, "billing", taken[0].GetChannel())
	require.Nil(t, taken[0].GetLinkedTo(), "a subscribed channel's notification names no owner")
	require.Equal(t, "orders", taken[1].GetChannel())
	require.Equal(t, int64(1), taken[1].GetCounter())
	require.Equal(t, "owner", taken[1].GetLinkedTo().GetBusinessId())
	require.Equal(t, "run-1", taken[1].GetLinkedTo().GetRunId())
	require.False(t, w.HasPendingChannelNotifications(ctx))

	require.True(t, w.HasScheduledChannelCounters(ctx))
	require.True(t, w.LinkedChannelHolds(ctx, "orders", 1), "the unstarted task carries it")
	w.ClearScheduledChannelCounters(ctx)
	require.False(t, w.HasScheduledChannelCounters(ctx))
	require.False(t, w.LinkedChannelHolds(ctx, "orders", 1))
}

// A run holds a bounded number of linked channels. The bound refuses a new
// channel and leaves the existing ones taking notifications.
func TestLinkedChannelLimitPerRun(t *testing.T) {
	ctx := newOwnerContext()
	w := &Workflow{}
	limits := channel.Limits{MaxLinkedChannelsPerWorkflow: 1}

	_, err := w.NotifyLinkedChannel(ctx, channelNote("orders", 1), limits)
	require.NoError(t, err)
	_, err = w.NotifyLinkedChannel(ctx, channelNote("billing", 1), limits)
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, 1, w.LinkedChannelCount())

	result, err := w.NotifyLinkedChannel(ctx, channelNote("orders", 2), limits)
	require.NoError(t, err)
	require.True(t, result.OwnerFolded, "replaced the pending 1")
	c, ok := w.LinkedChannel(ctx, "orders")
	require.True(t, ok)
	require.Equal(t, int64(2), c.LatestCounter())
}
