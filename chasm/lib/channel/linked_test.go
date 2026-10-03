package channel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
)

func newTestLinkedChannel(t *testing.T) (*Channel, *chasm.MockMutableContext) {
	t.Helper()
	now := time.Unix(1_700_000_000, 0).UTC()
	mctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return now },
		HandleExecutionKey: func() chasm.ExecutionKey {
			return chasm.ExecutionKey{NamespaceID: "ns", BusinessID: "owner", RunID: "run-1"}
		},
		HandleExecutionInfo: func() chasm.ExecutionInfo {
			return chasm.ExecutionInfo{ExecutionType: enumspb.EXECUTION_TYPE_WORKFLOW}
		},
	}}
	return NewLinkedChannel(mctx), mctx
}

// newTestActivityLinkedChannel is a channel held by a standalone activity.
func newTestActivityLinkedChannel(t *testing.T) (*Channel, *chasm.MockMutableContext) {
	t.Helper()
	now := time.Unix(1_700_000_000, 0).UTC()
	mctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return now },
		HandleExecutionKey: func() chasm.ExecutionKey {
			return chasm.ExecutionKey{NamespaceID: "ns", BusinessID: "act", RunID: "run-1"}
		},
		HandleExecutionInfo: func() chasm.ExecutionInfo {
			return chasm.ExecutionInfo{ExecutionType: enumspb.EXECUTION_TYPE_ACTIVITY}
		},
	}}
	return NewLinkedChannel(mctx), mctx
}

// A channel linked to a standalone activity has no owner among its listeners:
// a notify joins the ring and reaches the callbacks, nothing waits for the
// owner, the count is the callbacks alone, and describe lists no owner.
func TestLinkedActivityOwnerIsNotAListener(t *testing.T) {
	c, mctx := newTestActivityLinkedChannel(t)
	require.False(t, OwnerListens(mctx))

	result, err := c.NotifyLinked(mctx, note(1), Limits{})
	require.NoError(t, err)
	require.False(t, result.OwnerListens)
	require.Zero(t, result.ListenerCount)
	require.True(t, result.Advanced)
	require.False(t, c.HasOwnerPending(), "nothing waits for an owner that cannot be woken")
	linkedTo := c.State.GetLatest().GetLinkedTo()
	require.Equal(t, enumspb.EXECUTION_TYPE_ACTIVITY, linkedTo.GetType())
	require.Equal(t, "act", linkedTo.GetBusinessId())
	require.Equal(t, "run-1", linkedTo.GetRunId())
	require.Nil(t, c.OwnerListenerInfo(mctx))

	id, err := c.RegisterCallbackListener(mctx, CallbackRegistration{
		RequestID: "r1", Callback: testCallback(),
	})
	require.NoError(t, err)
	require.Equal(t, 1, c.LinkedListenerCount(mctx), "the callback alone")
	result, err = c.NotifyLinked(mctx, note(2), Limits{})
	require.NoError(t, err)
	require.Equal(t, 1, result.ListenerCount)
	require.Equal(t, int64(2), c.Listeners[id].Get(mctx).GetPending().GetCounter(),
		"handed the latest on registration, the next one waits behind it")
	require.False(t, c.HasOwnerPending())
}

// A notify on a linked channel names the owner, joins the ring, and waits for
// the owner's next scheduled event. No fan-out and no idle check: the owner
// is reached in this write, and the channel dies with the run.
func TestLinkedNotifyReachesTheOwnerInOneWrite(t *testing.T) {
	c, mctx := newTestLinkedChannel(t)
	require.True(t, c.Linked())

	result, err := c.NotifyLinked(mctx, note(1), Limits{})
	require.NoError(t, err)
	require.Equal(t, 1, result.ListenerCount, "the owner")
	require.True(t, result.Advanced)
	require.False(t, result.OwnerHeld)
	require.False(t, result.OwnerFolded)
	require.Empty(t, mctx.Tasks, "nothing to fan out to and nothing to expire")

	require.True(t, c.HasOwnerPending())
	pending := c.State.GetOwnerPending()
	require.Equal(t, enumspb.EXECUTION_TYPE_WORKFLOW, pending.GetLinkedTo().GetType())
	require.Equal(t, "owner", pending.GetLinkedTo().GetBusinessId())
	require.Equal(t, "run-1", pending.GetLinkedTo().GetRunId())
	polled, err := c.Poll(mctx, PollRequest{})
	require.NoError(t, err)
	require.Equal(t, []int64{1}, counters(polled))
	require.Equal(t, "owner", polled[0].GetLinkedTo().GetBusinessId())
}

// The owner folds like a subscribed listener: a repeat at or below what it
// holds pending, or what its unstarted task carries, changes nothing; a
// higher counter replaces the pending entry; and once the task has started
// the same counter is news again.
func TestLinkedOwnerFoldRules(t *testing.T) {
	c, mctx := newTestLinkedChannel(t)
	_, err := c.NotifyLinked(mctx, note(2), Limits{})
	require.NoError(t, err)

	result, err := c.NotifyLinked(mctx, note(1), Limits{})
	require.NoError(t, err)
	require.False(t, result.Advanced, "below the latest, so not retained")
	require.True(t, result.OwnerHeld)
	require.Equal(t, int64(2), c.State.GetOwnerPending().GetCounter())

	result, err = c.NotifyLinked(mctx, note(3), Limits{})
	require.NoError(t, err)
	require.True(t, result.Advanced)
	require.True(t, result.OwnerFolded, "replaced the pending 2")
	require.Equal(t, int64(3), c.State.GetOwnerPending().GetCounter())

	taken := c.TakeOwnerPending()
	require.Equal(t, int64(3), taken.GetCounter())
	require.False(t, c.HasOwnerPending())
	require.Nil(t, c.TakeOwnerPending())
	require.True(t, c.HasOwnerScheduledCounter())
	require.True(t, c.OwnerHolds(3), "the unstarted task carries it")
	require.True(t, c.OwnerHolds(2))
	require.False(t, c.OwnerHolds(4))

	result, err = c.NotifyLinked(mctx, note(3), Limits{})
	require.NoError(t, err)
	require.True(t, result.OwnerHeld)
	require.False(t, c.HasOwnerPending())

	c.ClearOwnerScheduledCounter()
	require.False(t, c.HasOwnerScheduledCounter())
	result, err = c.NotifyLinked(mctx, note(3), Limits{})
	require.NoError(t, err)
	require.False(t, result.Advanced)
	require.False(t, result.OwnerHeld, "the task started, so the repeat is a new reason to run")
	require.True(t, c.HasOwnerPending())
	require.Equal(t, int64(2), c.RetainedCount(), "2 and 3 joined the ring, the repeats did not")
}

// The linked ring has its own, smaller bound.
func TestLinkedRingBound(t *testing.T) {
	c, mctx := newTestLinkedChannel(t)
	for counter := int64(1); counter <= 5; counter++ {
		_, err := c.NotifyLinked(mctx, note(counter), Limits{LinkedRetainedNotifications: 2})
		require.NoError(t, err)
	}
	require.Equal(t, int64(2), c.RetainedCount())
	polled, err := c.Poll(mctx, PollRequest{})
	require.NoError(t, err)
	require.Equal(t, []int64{4, 5}, counters(polled))
}

// Callback listeners on a linked channel are handed each notification in the
// notify itself, folding while busy as on an independent channel, and a new
// one is posted the latest. They count as listeners beside the owner, and
// dropping the last one arms no idle check.
func TestLinkedCallbackListeners(t *testing.T) {
	c, mctx := newTestLinkedChannel(t)
	id, err := c.RegisterCallbackListener(mctx, CallbackRegistration{RequestID: "r1", Callback: testCallback()})
	require.NoError(t, err)
	require.Empty(t, tasksOf[*channelpb.ChannelCallbackTask](mctx), "nothing to hand over yet")

	result, err := c.NotifyLinked(mctx, note(1), Limits{})
	require.NoError(t, err)
	require.Equal(t, 2, result.ListenerCount)
	require.Equal(t, 1, result.CallbackStarted)
	require.Len(t, tasksOf[*channelpb.ChannelCallbackTask](mctx), 1)
	require.Empty(t, tasksOf[*channelpb.ChannelFanOutTask](mctx))
	delivery, ok := c.CallbackDeliveryFor(mctx, &channelpb.ChannelCallbackTask{ListenerId: id, Sequence: 1})
	require.True(t, ok)
	require.True(t, delivery.Linked)
	require.Equal(t, "owner", delivery.Notification.GetLinkedTo().GetBusinessId())

	result, err = c.NotifyLinked(mctx, note(2), Limits{})
	require.NoError(t, err)
	require.Equal(t, 0, result.CallbackStarted, "busy, so it folds into pending")
	require.Equal(t, int64(2), c.Listeners[id].Get(mctx).GetPending().GetCounter())

	late, err := c.RegisterCallbackListener(mctx, CallbackRegistration{RequestID: "r2", Callback: testCallback()})
	require.NoError(t, err)
	require.Equal(t, int64(2), c.Listeners[late].Get(mctx).GetInFlight().GetCounter(),
		"handed the latest on registration")
	require.Equal(t, 3, c.LinkedListenerCount(mctx))

	require.NoError(t, c.UnregisterListener(mctx, id, Limits{}))
	require.NoError(t, c.UnregisterListener(mctx, late, Limits{}))
	require.Equal(t, 1, c.LinkedListenerCount(mctx))
	require.Empty(t, tasksOf[*channelpb.ChannelIdleTask](mctx))
}
