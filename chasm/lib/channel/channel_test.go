package channel

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"go.temporal.io/server/common/backoff"
)

func newTestChannel(t *testing.T) (*Channel, *chasm.MockMutableContext, *time.Time) {
	t.Helper()
	now := time.Unix(1_700_000_000, 0).UTC()
	mctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return now },
	}}
	return NewChannel(mctx), mctx, &now
}

func note(counter int64) *channelpb.Notification {
	return &channelpb.Notification{Channel: "orders", Counter: counter, Position: []byte{byte(counter)}}
}

func tasksOf[T any](mctx *chasm.MockMutableContext) []chasm.MockTask {
	var out []chasm.MockTask
	for _, task := range mctx.Tasks {
		if _, ok := task.Payload.(T); ok {
			out = append(out, task)
		}
	}
	return out
}

func counters(ns []*channelpb.Notification) []int64 {
	out := make([]int64, len(ns))
	for i, n := range ns {
		out[i] = n.GetCounter()
	}
	return out
}

func testCallback() *commonpb.Callback {
	return &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
		Nexus: &commonpb.Callback_Nexus{Url: "http://localhost:7243/notify"},
	}}
}

// A channel nobody listens to still keeps what it is told, for a poller that
// arrives later, and reports that nobody was there.
func TestNotifyWithoutListenersRetains(t *testing.T) {
	c, mctx, _ := newTestChannel(t)
	result, err := c.Notify(mctx, note(1), Limits{})
	require.NoError(t, err)
	require.Zero(t, result.ListenerCount)
	require.Empty(t, tasksOf[*channelpb.ChannelFanOutTask](mctx), "nobody to fan out to")

	polled, err := c.Poll(mctx, PollRequest{})
	require.NoError(t, err)
	require.Equal(t, []int64{1}, counters(polled))
	require.Equal(t, int64(1), c.LatestCounter())
}

// The ring keeps the newest notifications up to its bound, and a poll returns
// those above the caller's counter, oldest first, up to its page size.
func TestRingDropsOldestAndPollFilters(t *testing.T) {
	c, mctx, _ := newTestChannel(t)
	limits := Limits{RetainedNotifications: 3}
	for counter := int64(1); counter <= 5; counter++ {
		_, err := c.Notify(mctx, note(counter), limits)
		require.NoError(t, err)
	}
	require.Equal(t, int64(3), c.RetainedCount())
	require.Len(t, c.Retained, 3)

	polled, err := c.Poll(mctx, PollRequest{})
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4, 5}, counters(polled))

	polled, err = c.Poll(mctx, PollRequest{AfterCounter: 3, Max: 1})
	require.NoError(t, err)
	require.Equal(t, []int64{4}, counters(polled))
}

func TestNotifyRefusesWhatItCannotAccept(t *testing.T) {
	c, mctx, _ := newTestChannel(t)
	cases := map[string]*channelpb.Notification{
		"no counter":     {Channel: "orders"},
		"no channel":     {Counter: 1},
		"long position":  {Channel: "orders", Counter: 1, Position: make([]byte, MaxPositionBytes+1)},
		"large metadata": {Channel: "orders", Counter: 1, Metadata: map[string]*commonpb.Payload{"k": {Data: make([]byte, 64)}}},
	}
	for name, n := range cases {
		_, err := c.Notify(mctx, n, Limits{MaxMetadataBytes: 32})
		var invalid *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalid, name)
	}
	require.Zero(t, c.RetainedCount())
}

// A burst schedules one fan-out, and the fan-out hands each listener only the
// highest counter.
func TestBurstCoalescesIntoOneFanOut(t *testing.T) {
	c, mctx, _ := newTestChannel(t)
	require.NoError(t, registerWorkflow(c, mctx, WorkflowRegistration{
		WorkflowID: "wf", RunID: "run-1",
	}))
	for _, counter := range []int64{1, 3, 2} {
		result, err := c.Notify(mctx, note(counter), Limits{})
		require.NoError(t, err)
		require.Equal(t, 1, result.ListenerCount)
	}
	require.Len(t, tasksOf[*channelpb.ChannelFanOutTask](mctx), 1)
	require.Equal(t, int64(3), c.LatestCounter(), "a lower counter does not replace the latest")
	require.Equal(t, int64(2), c.RetainedCount(), "nor is it retained: it changes nothing")
	require.False(t, c.Advances(3))
	require.True(t, c.Advances(4))

	fanOut, err := c.TakeFanOut(mctx, struct{}{})
	require.NoError(t, err)
	require.Equal(t, int64(3), fanOut.Latest.GetCounter())
	require.Len(t, fanOut.Workflows, 1)
	require.False(t, c.State.GetFanOutPending())

	_, err = c.Notify(mctx, note(4), Limits{})
	require.NoError(t, err)
	require.Len(t, tasksOf[*channelpb.ChannelFanOutTask](mctx), 2, "the flag came down")
}

// A callback listener has one delivery in flight. What arrives meanwhile folds
// into a single pending notification, which goes out when the in-flight one
// is done.
func TestCallbackFoldsWhileBusy(t *testing.T) {
	c, mctx, _ := newTestChannel(t)
	id, err := c.RegisterCallbackListener(mctx, CallbackRegistration{
		RequestID: "req-1", Callback: testCallback(),
	})
	require.NoError(t, err)

	notifyAndFanOut := func(counter int64) FanOut {
		_, err := c.Notify(mctx, note(counter), Limits{})
		require.NoError(t, err)
		fanOut, err := c.TakeFanOut(mctx, struct{}{})
		require.NoError(t, err)
		return fanOut
	}

	require.Equal(t, 1, notifyAndFanOut(1).CallbackStarted)
	deliveries := tasksOf[*channelpb.ChannelCallbackTask](mctx)
	require.Len(t, deliveries, 1)
	require.Equal(t, "http://localhost:7243", deliveries[0].Attributes.Destination)

	require.Zero(t, notifyAndFanOut(2).CallbackFolded, "first one waiting is not a fold")
	require.Equal(t, 1, notifyAndFanOut(3).CallbackFolded)
	require.Len(t, tasksOf[*channelpb.ChannelCallbackTask](mctx), 1, "still one in flight")

	task := deliveries[0].Payload.(*channelpb.ChannelCallbackTask)
	delivery, ok := c.CallbackDeliveryFor(mctx, task)
	require.True(t, ok)
	require.Equal(t, int64(1), delivery.Notification.GetCounter())

	delivered, err := c.CompleteCallbackDelivery(mctx, CallbackOutcome{
		ListenerID: id, Sequence: task.GetSequence(),
	})
	require.NoError(t, err)
	require.True(t, delivered)
	deliveries = tasksOf[*channelpb.ChannelCallbackTask](mctx)
	require.Len(t, deliveries, 2, "the pending one goes out next")
	next, ok := c.CallbackDeliveryFor(mctx, deliveries[1].Payload.(*channelpb.ChannelCallbackTask))
	require.True(t, ok)
	require.Equal(t, int64(3), next.Notification.GetCounter())
	_, ok = c.CallbackDeliveryFor(mctx, task)
	require.False(t, ok, "the first delivery's task is stale")

	stale, err := c.CompleteCallbackDelivery(mctx, CallbackOutcome{
		ListenerID: id, Sequence: task.GetSequence(),
	})
	require.NoError(t, err)
	require.False(t, stale, "a stale outcome changes nothing")
	delivered, err = c.CompleteCallbackDelivery(mctx, CallbackOutcome{
		ListenerID: id, Sequence: deliveries[1].Payload.(*channelpb.ChannelCallbackTask).GetSequence(),
	})
	require.NoError(t, err)
	require.True(t, delivered)
	require.Nil(t, c.Listeners[id].Get(mctx).GetInFlight())
}

// A retryable failure backs off and tries the same notification again; a
// failure that will not succeed on retry drops it.
func TestCallbackRetryBacksOff(t *testing.T) {
	c, mctx, now := newTestChannel(t)
	id, err := c.RegisterCallbackListener(mctx, CallbackRegistration{
		RequestID: "req-1", Callback: testCallback(),
	})
	require.NoError(t, err)
	_, err = c.Notify(mctx, note(1), Limits{})
	require.NoError(t, err)
	_, err = c.TakeFanOut(mctx, struct{}{})
	require.NoError(t, err)

	policy := backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(backoff.NoInterval)
	delivered, err := c.CompleteCallbackDelivery(mctx, CallbackOutcome{
		ListenerID: id, Sequence: 1, Err: errors.New("503"), Retryable: true, RetryPolicy: policy,
	})
	require.NoError(t, err)
	require.False(t, delivered)
	backoffs := tasksOf[*channelpb.ChannelCallbackBackoffTask](mctx)
	require.Len(t, backoffs, 1)
	require.True(t, backoffs[0].Attributes.ScheduledTime.After(*now))
	backoffTask := backoffs[0].Payload.(*channelpb.ChannelCallbackBackoffTask)
	require.Equal(t, int64(1), backoffTask.GetSequence())
	_, ok := c.CallbackDeliveryFor(mctx, &channelpb.ChannelCallbackTask{ListenerId: id, Sequence: 1})
	require.False(t, ok, "nothing to post while backing off")

	require.True(t, c.BackoffDone(mctx, backoffTask))
	require.NoError(t, c.ResumeCallback(mctx, backoffTask))
	retry, ok := c.CallbackDeliveryFor(mctx, &channelpb.ChannelCallbackTask{ListenerId: id, Sequence: 2})
	require.True(t, ok)
	require.Equal(t, int64(1), retry.Notification.GetCounter())
	require.Equal(t, int32(1), retry.Attempt)

	delivered, err = c.CompleteCallbackDelivery(mctx, CallbackOutcome{
		ListenerID: id, Sequence: 2, Err: errors.New("400"),
	})
	require.NoError(t, err)
	require.False(t, delivered)
	listener := c.Listeners[id].Get(mctx)
	require.Nil(t, listener.GetInFlight())
	require.True(t, listener.GetLastAttemptFailure().GetApplicationFailureInfo().GetNonRetryable())
}

// A retried registration returns the listener it made; a new listener is not
// handed what was accepted before it registered; the table is bounded.
func TestListenerTable(t *testing.T) {
	c, mctx, _ := newTestChannel(t)
	_, err := c.Notify(mctx, note(5), Limits{})
	require.NoError(t, err)

	limits := Limits{MaxListeners: 2}
	first, err := c.RegisterCallbackListener(mctx, CallbackRegistration{
		RequestID: "req-1", Callback: testCallback(), Limits: limits,
	})
	require.NoError(t, err)
	again, err := c.RegisterCallbackListener(mctx, CallbackRegistration{
		RequestID: "req-1", Callback: testCallback(), Limits: limits,
	})
	require.NoError(t, err)
	require.Equal(t, first, again)
	listener := c.Listeners[first].Get(mctx)
	require.Equal(t, int64(5), listener.GetInFlight().GetCounter(), "a new listener is posted the latest")
	require.Equal(t, int64(5), listener.GetHandedCounter())
	require.Len(t, tasksOf[*channelpb.ChannelCallbackTask](mctx), 1, "once, not per retry")

	require.NoError(t, registerWorkflow(c, mctx, WorkflowRegistration{
		WorkflowID: "wf", RunID: "run-1", Limits: limits,
	}))
	_, err = c.RegisterCallbackListener(mctx, CallbackRegistration{
		RequestID: "req-2", Callback: testCallback(), Limits: limits,
	})
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.NoError(t, registerWorkflow(c, mctx, WorkflowRegistration{
		WorkflowID: "wf", RunID: "run-2", Limits: limits,
	}), "a later run of a known workflow takes its entry, not a new one")
	require.Equal(t, "run-2", workflowRun(t, c, mctx, "wf"))

	_, err = c.RegisterCallbackListener(mctx, CallbackRegistration{
		RequestID: "req-3", Callback: &commonpb.Callback{},
	})
	require.Error(t, err, "a callback with no URL cannot be reached")

	require.Error(t, c.UnregisterListener(mctx, "wf", Limits{}), "workflows leave by ending")
	require.NoError(t, c.UnregisterListener(mctx, first, Limits{}))
	require.NoError(t, c.UnregisterListener(mctx, first, Limits{}), "a retry succeeds")
	require.Equal(t, 1, c.ListenerCount())

	snapshot, err := c.Describe(mctx, struct{}{})
	require.NoError(t, err)
	require.Len(t, snapshot.Listeners, 1)
	require.Equal(t, "run-2", snapshot.Listeners[0].GetWorkflow().GetRunId())
	require.Equal(t, int64(5), snapshot.Latest.GetCounter())
}

// The chain rule: a listener moves to the run that continued the one on
// record, and goes when no run listens. Neither touches an entry a newer run
// has already taken.
func TestWorkflowListenerRekeyAndForget(t *testing.T) {
	c, mctx, _ := newTestChannel(t)
	require.NoError(t, registerWorkflow(c, mctx, WorkflowRegistration{
		WorkflowID: "wf", RunID: "run-1",
	}))

	require.NoError(t, c.RekeyWorkflowListener(mctx, "wf", "run-1", "run-2"))
	require.Equal(t, "run-2", workflowRun(t, c, mctx, "wf"))
	require.NoError(t, c.RekeyWorkflowListener(mctx, "wf", "run-1", "run-3"))
	require.Equal(t, "run-2", workflowRun(t, c, mctx, "wf"), "stale re-key")

	c.ForgetWorkflowListener(mctx, "wf", "run-1", Limits{})
	require.Equal(t, 1, c.ListenerCount(), "stale forget")
	c.ForgetWorkflowListener(mctx, "wf", "run-2", Limits{})
	require.Zero(t, c.ListenerCount())
	require.Len(t, tasksOf[*channelpb.ChannelIdleTask](mctx), 1, "the last listener leaving arms the idle check")
}

// A channel with no listeners is closed for deletion a full retention after
// its last activity, and not before.
func TestIdleCheck(t *testing.T) {
	c, mctx, now := newTestChannel(t)
	limits := Limits{Retention: time.Hour}
	_, err := c.Notify(mctx, note(1), limits)
	require.NoError(t, err)
	idle := tasksOf[*channelpb.ChannelIdleTask](mctx)
	require.Len(t, idle, 1)
	require.Equal(t, now.Add(time.Hour), idle[0].Attributes.ScheduledTime)

	*now = now.Add(30 * time.Minute)
	_, err = c.Notify(mctx, note(2), limits)
	require.NoError(t, err)
	require.Len(t, tasksOf[*channelpb.ChannelIdleTask](mctx), 1, "one check outstanding")

	*now = now.Add(30 * time.Minute)
	expired, err := c.RunIdleCheck(mctx, limits)
	require.NoError(t, err)
	require.False(t, expired, "the second notify moved the deadline")
	idle = tasksOf[*channelpb.ChannelIdleTask](mctx)
	require.Len(t, idle, 2)
	require.Equal(t, now.Add(30*time.Minute), idle[1].Attributes.ScheduledTime)

	*now = now.Add(30 * time.Minute)
	require.True(t, c.PollNeedsTouch(*now, limits))
	expired, err = c.RunIdleCheck(mctx, limits)
	require.NoError(t, err)
	require.True(t, expired)
	require.Equal(t, chasm.LifecycleStateCompleted, c.LifecycleState(mctx))
}

// A listener holds the channel: the idle check stands down while one is
// registered.
func TestIdleCheckStandsDownWithListeners(t *testing.T) {
	c, mctx, now := newTestChannel(t)
	limits := Limits{Retention: time.Minute}
	_, err := c.Notify(mctx, note(1), limits)
	require.NoError(t, err)
	require.NoError(t, registerWorkflow(c, mctx, WorkflowRegistration{
		WorkflowID: "wf", RunID: "run-1",
	}))
	*now = now.Add(time.Hour)
	expired, err := c.RunIdleCheck(mctx, limits)
	require.NoError(t, err)
	require.False(t, expired)
	require.False(t, c.State.GetIdleCheckPending())
	require.Len(t, tasksOf[*channelpb.ChannelIdleTask](mctx), 1)
}

func registerWorkflow(c *Channel, mctx chasm.MutableContext, reg WorkflowRegistration) error {
	_, err := c.RegisterWorkflowListener(mctx, reg)
	return err
}

// workflowRun is the run the channel has on record for a workflow id.
func workflowRun(t *testing.T, c *Channel, ctx chasm.Context, workflowID string) string {
	t.Helper()
	field, ok := c.Listeners[workflowID]
	require.True(t, ok, "no listener %q", workflowID)
	target, ok := WorkflowTargetOf(field.Get(ctx))
	require.True(t, ok, "listener %q is not a workflow", workflowID)
	return target.GetRunId()
}

// A workflow listener is an internal callback whose data names the run, held
// in the one table next to the HTTP callbacks. The two kinds share the id
// space, so neither can take an id the other holds.
func TestWorkflowListenerIsAnInternalCallback(t *testing.T) {
	c, mctx, _ := newTestChannel(t)
	_, err := c.RegisterWorkflowListener(mctx, WorkflowRegistration{
		NamespaceID: "ns", WorkflowID: "wf", RunID: "run-1", FirstExecutionRunID: "run-0",
	})
	require.NoError(t, err)
	listener := c.Listeners["wf"].Get(mctx)
	require.True(t, IsWorkflowListener(listener))
	require.Equal(t, ListenerKindWorkflow, ListenerKind(listener.GetCallback()))
	require.NotEmpty(t, listener.GetCallback().GetInternal().GetData())
	require.Nil(t, listener.GetInFlight(), "a run folds on its own shard, not here")
	target, ok := WorkflowTargetOf(listener)
	require.True(t, ok)
	require.Equal(t, "ns", target.GetNamespaceId())
	require.Equal(t, "wf", target.GetWorkflowId())
	require.Equal(t, "run-1", target.GetRunId())
	require.Equal(t, "run-0", target.GetFirstExecutionRunId())

	var invalid *serviceerror.InvalidArgument
	_, err = c.RegisterCallbackListener(mctx, CallbackRegistration{
		RequestID: "wf", Callback: testCallback(),
	})
	require.ErrorAs(t, err, &invalid, "a callback cannot take a workflow's id")
	id, err := c.RegisterCallbackListener(mctx, CallbackRegistration{
		RequestID: "cb", Callback: testCallback(),
	})
	require.NoError(t, err)
	callback := c.Listeners[id].Get(mctx)
	require.False(t, IsWorkflowListener(callback))
	require.Equal(t, ListenerKindCallback, ListenerKind(callback.GetCallback()))
	_, ok = WorkflowTargetOf(callback)
	require.False(t, ok)
	_, err = c.RegisterWorkflowListener(mctx, WorkflowRegistration{WorkflowID: "cb", RunID: "run-1"})
	require.ErrorAs(t, err, &invalid, "a workflow cannot take a callback's id")

	require.Equal(t, 2, c.ListenerCount())
	targets := c.WorkflowTargets(mctx)
	require.Len(t, targets, 1)
	require.Equal(t, "run-1", targets[0].GetRunId())
	snapshot, err := c.Describe(mctx, struct{}{})
	require.NoError(t, err)
	require.Len(t, snapshot.Listeners, 2)
	require.Equal(t, "run-1", snapshot.Listeners[0].GetWorkflow().GetRunId(), "workflows first")
	require.Equal(t, testCallback().GetNexus().GetUrl(),
		snapshot.Listeners[1].GetCallback().GetNexus().GetUrl())
}

// A run new to the channel is handed the latest notification, so a write that
// landed before it subscribed still wakes it; a repeat registration and an
// empty channel hand nothing.
func TestNewWorkflowListenerGetsLatest(t *testing.T) {
	c, mctx, _ := newTestChannel(t)
	latest, err := c.RegisterWorkflowListener(mctx, WorkflowRegistration{WorkflowID: "a", RunID: "r1"})
	require.NoError(t, err)
	require.Nil(t, latest, "nothing to hand on an empty channel")

	_, err = c.Notify(mctx, note(3), Limits{})
	require.NoError(t, err)
	latest, err = c.RegisterWorkflowListener(mctx, WorkflowRegistration{WorkflowID: "b", RunID: "r1"})
	require.NoError(t, err)
	require.Equal(t, int64(3), latest.GetCounter())
	latest, err = c.RegisterWorkflowListener(mctx, WorkflowRegistration{WorkflowID: "b", RunID: "r1"})
	require.NoError(t, err)
	require.Nil(t, latest, "a repeat is not new")
	latest, err = c.RegisterWorkflowListener(mctx, WorkflowRegistration{WorkflowID: "b", RunID: "r2"})
	require.NoError(t, err)
	require.Equal(t, int64(3), latest.GetCounter(), "a successor run is new")
}

// A notification that does not advance the channel still reaches a callback
// listener that does not hold it, and changes nothing for one that does.
func TestRepeatHandsOnlyToCallbacksThatLackIt(t *testing.T) {
	c, mctx, _ := newTestChannel(t)
	id, err := c.RegisterCallbackListener(mctx, CallbackRegistration{
		RequestID: "req-1", Callback: testCallback(),
	})
	require.NoError(t, err)
	_, err = c.Notify(mctx, note(5), Limits{})
	require.NoError(t, err)
	_, err = c.TakeFanOut(mctx, struct{}{})
	require.NoError(t, err)
	require.Len(t, tasksOf[*channelpb.ChannelCallbackTask](mctx), 1)

	require.False(t, c.CallbacksNeed(mctx, 5), "in flight, so held")
	started, folded, err := c.HandToCallbacks(mctx, note(5))
	require.NoError(t, err)
	require.Zero(t, started)
	require.Equal(t, 1, folded)

	task := tasksOf[*channelpb.ChannelCallbackTask](mctx)[0].Payload.(*channelpb.ChannelCallbackTask)
	_, err = c.CompleteCallbackDelivery(mctx, CallbackOutcome{ListenerID: id, Sequence: task.GetSequence()})
	require.NoError(t, err)
	require.True(t, c.CallbacksNeed(mctx, 5), "posted, so the same counter is news again")
	started, _, err = c.HandToCallbacks(mctx, note(5))
	require.NoError(t, err)
	require.Equal(t, 1, started)
	require.Len(t, tasksOf[*channelpb.ChannelCallbackTask](mctx), 2)
	require.Empty(t, c.WorkflowTargets(mctx))
}
