package channel

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"go.temporal.io/server/common"
)

// A linked channel lives in the state of one execution, a workflow run. The
// run is its listener by construction: no subscription, no event and no
// registration race. A notification reaches the owner in the write that
// accepts it and rides the owner's next scheduled event, with linked_to
// naming the owner. Callback listeners and pollers attach to a linked channel as they do to an independent one. The
// channel dies with the execution and nothing of it reaches a successor: a
// continue-as-new successor starts with no linked channels, so callbacks
// register again and pollers start over on an empty ring.

// NewLinkedChannel makes an empty channel linked to the execution that holds
// it.
func NewLinkedChannel(mctx chasm.MutableContext) *Channel {
	c := NewChannel(mctx)
	c.State.Linked = true
	return c
}

// Linked reports whether the channel lives in an execution's state.
func (c *Channel) Linked() bool {
	return c.State.GetLinked()
}

// LinkedNotifyResult is what a notify on a linked channel reports.
type LinkedNotifyResult struct {
	// The owner, when it listens, and the callback listeners.
	ListenerCount int
	// The notification raised the latest counter and joined the ring.
	Advanced bool
	// The owner is among the listeners. The owner fields below mean nothing
	// when it is not.
	OwnerListens bool
	// The owner already held it, pending or on a task that has not started,
	// so nothing changed for the owner.
	OwnerHeld bool
	// It replaced a pending notification the owner had not been handed yet.
	OwnerFolded     bool
	CallbackStarted int
	CallbackFolded  int
}

// NotifyLinked accepts a notification on a linked channel. A counter above
// the latest joins the ring. Any counter reaches the owner, when it listens,
// and the callback listeners, each folding it against what it holds, since a
// repeat after the owner's task ran is a new reason to run. The owner's part
// is the pending entry here, on its own execution, which the transaction
// close turns into a Workflow Task.
func (c *Channel) NotifyLinked(
	mctx chasm.MutableContext,
	n *channelpb.Notification,
	limits Limits,
) (LinkedNotifyResult, error) {
	limits = limits.withDefaults()
	if err := CheckNotification(n, limits.MaxMetadataBytes); err != nil {
		return LinkedNotifyResult{}, err
	}
	n = common.CloneProto(n)
	n.LinkedTo = c.LinkedTo(mctx)
	out := LinkedNotifyResult{
		ListenerCount: c.LinkedListenerCount(mctx),
		OwnerListens:  OwnerListens(mctx),
	}
	if c.Advances(n.GetCounter()) {
		c.retain(mctx, n, limits.LinkedRetainedNotifications)
		c.State.AcceptedCount++
		c.State.Latest = n
		out.Advanced = true
	}
	c.touch(mctx)
	switch {
	case !out.OwnerListens:
		out.OwnerHeld = true
	case c.OwnerHolds(n.GetCounter()):
		out.OwnerHeld = true
	default:
		out.OwnerFolded = c.State.GetOwnerPending() != nil
		c.State.OwnerPending = n
	}
	started, folded, err := c.HandToCallbacks(mctx, n)
	if err != nil {
		return LinkedNotifyResult{}, err
	}
	out.CallbackStarted, out.CallbackFolded = started, folded
	return out, nil
}

// LinkedTo names the owner: the execution the channel lives in and the run
// that holds it.
func (c *Channel) LinkedTo(ctx chasm.Context) *commonpb.Execution {
	return ExecutionOf(ctx)
}

// LinkedListenerCount counts the owner, when it listens, and the callback
// listeners. The owner is never in the table, so the table holds callbacks
// only.
func (c *Channel) LinkedListenerCount(ctx chasm.Context) int {
	count := len(c.Listeners)
	if OwnerListens(ctx) {
		count++
	}
	return count
}

// OwnerHolds reports whether the owner already has a notification at this
// counter or above: pending for its next scheduled event, or on the scheduled
// event of a task that has not started. A started task does not count: it
// may have read its source before the write the repeat stands for.
func (c *Channel) OwnerHolds(counter int64) bool {
	if counter <= c.State.GetOwnerScheduledCounter() {
		return true
	}
	pending := c.State.GetOwnerPending()
	return pending != nil && counter <= pending.GetCounter()
}

// HasOwnerPending reports whether a notification waits for the owner's next
// scheduled event.
func (c *Channel) HasOwnerPending() bool {
	return c.State.GetOwnerPending() != nil
}

// TakeOwnerPending hands the pending notification to a scheduled event being
// written and clears it. The channel remembers the counter until the task
// starts, so a repeat in that window folds into the task.
func (c *Channel) TakeOwnerPending() *channelpb.Notification {
	n := c.State.GetOwnerPending()
	if n == nil {
		return nil
	}
	c.State.OwnerPending = nil
	c.State.OwnerScheduledCounter = n.GetCounter()
	return n
}

// HasOwnerScheduledCounter reports whether the channel remembers what the
// owner's scheduled task carries.
func (c *Channel) HasOwnerScheduledCounter() bool {
	return c.State.GetOwnerScheduledCounter() > 0
}

// ClearOwnerScheduledCounter forgets what the scheduled task carries, once
// that task starts, fails or times out.
func (c *Channel) ClearOwnerScheduledCounter() {
	c.State.OwnerScheduledCounter = 0
}

// OwnerStanding is a linked channel as the owner's describe reports it: the
// counters as the owner stands on them and the counts the channel keeps.
type OwnerStanding struct {
	// Highest counter the channel accepted.
	LastCounter int64
	// The notification the owner has not been handed on a scheduled event.
	Pending *channelpb.Notification
	// Counter the owner's scheduled, not yet started task carries.
	ScheduledCounter int64
	// Callback listeners. The owner is not counted among its own listeners.
	ListenerCount int
	RetainedCount int64
	AcceptedCount int64
}

// OwnerStanding reads the channel for the owner's describe. The pending
// notification is a copy, since the response is marshalled after the owner's
// lock is released.
func (c *Channel) OwnerStanding() OwnerStanding {
	return OwnerStanding{
		LastCounter:      c.LatestCounter(),
		Pending:          common.CloneProto(c.State.GetOwnerPending()),
		ScheduledCounter: c.State.GetOwnerScheduledCounter(),
		ListenerCount:    len(c.Listeners),
		RetainedCount:    c.RetainedCount(),
		AcceptedCount:    c.State.GetAcceptedCount(),
	}
}

// OwnerListenerInfo describes the owner as the linked channel's workflow
// listener, for describe. Nil for an owner that does not listen.
func (c *Channel) OwnerListenerInfo(ctx chasm.Context) *channelpb.ChannelListenerInfo {
	if !OwnerListens(ctx) {
		return nil
	}
	owner := c.LinkedTo(ctx)
	return &channelpb.ChannelListenerInfo{
		ListenerId: owner.GetBusinessId(),
		Variant: &channelpb.ChannelListenerInfo_Workflow_{
			Workflow: &channelpb.ChannelListenerInfo_Workflow{
				WorkflowId: owner.GetBusinessId(),
				RunId:      owner.GetRunId(),
			},
		},
	}
}
