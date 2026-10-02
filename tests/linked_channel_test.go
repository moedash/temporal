package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/channel"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A linked channel lives in the mutable state of the workflow named on the
// call. These cases drive it with the raw client the way the independent
// cases do, so each scheduled event can be inspected.

func linkedOwner(id, runID string) *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID}
}

func (c *channelTestEnv) notifyLinked(
	owner *commonpb.WorkflowExecution,
	name string,
	counter int64,
) (*workflowservice.NotifyChannelResponse, error) {
	return c.env.FrontendClient().NotifyChannel(c.ctx(), &workflowservice.NotifyChannelRequest{
		Namespace:         c.ns,
		Notification:      channelNotification(name, counter),
		Identity:          "tester",
		RequestId:         uuid.NewString(),
		WorkflowExecution: owner,
	})
}

func (c *channelTestEnv) mustNotifyLinked(id, name string, counter int64) int32 {
	c.t.Helper()
	resp, err := c.notifyLinked(linkedOwner(id, ""), name, counter)
	require.NoError(c.t, err)
	return resp.GetListenerCount()
}

func (c *channelTestEnv) describeLinked(
	owner *commonpb.WorkflowExecution,
	name string,
) (*workflowservice.DescribeChannelResponse, error) {
	return c.env.FrontendClient().DescribeChannel(c.ctx(), &workflowservice.DescribeChannelRequest{
		Namespace:         c.ns,
		Channel:           name,
		WorkflowExecution: owner,
	})
}

func (c *channelTestEnv) mustDescribeLinked(id, name string) *workflowservice.DescribeChannelResponse {
	c.t.Helper()
	resp, err := c.describeLinked(linkedOwner(id, ""), name)
	require.NoError(c.t, err)
	return resp
}

func (c *channelTestEnv) pollLinked(
	owner *commonpb.WorkflowExecution,
	name string,
	after int64,
	wait time.Duration,
	limit int32,
) (*workflowservice.PollChannelResponse, error) {
	return c.env.FrontendClient().PollChannel(c.ctx(), &workflowservice.PollChannelRequest{
		Namespace:         c.ns,
		Channel:           name,
		AfterCounter:      after,
		Wait:              durationpb.New(wait),
		MaxNotifications:  limit,
		WorkflowExecution: owner,
	})
}

// startIdle starts a workflow and completes its first task, so the next task
// it gets is one a notification scheduled.
func (c *channelTestEnv) startIdle(id string) string {
	c.t.Helper()
	runID := c.start(id)
	c.complete(c.poll(id), false)
	require.False(c.t, c.hasPendingTask(id))
	return runID
}

func requireLinkedTo(t *testing.T, ns []*notificationpb.Notification, id, runID string) {
	t.Helper()
	for _, n := range ns {
		require.Equal(t, id, n.GetLinkedTo().GetWorkflowId(), "linked_to of %q", n.GetChannel())
		require.Equal(t, runID, n.GetLinkedTo().GetRunId(), "linked_to run of %q", n.GetChannel())
	}
}

func requireResourceExhausted(t *testing.T, err error) {
	t.Helper()
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
}

// A notify addressed to a workflow lands on that workflow's own linked
// channel: the run is the listener with no subscription, the notification
// rides its next scheduled event naming the owner, and describe reports the
// kind. Before anyone notifies, the channel exists on a running workflow with
// nothing in it, which is what a client probes for.
func TestLinkedChannelNotifyCarriesOnScheduledEvent(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "linked-basic-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID := c.startIdle(id)

	untouched := c.mustDescribeLinked(id, name)
	require.Equal(t, notificationpb.CHANNEL_KIND_LINKED, untouched.GetKind())
	require.Equal(t, id, untouched.GetLinkedTo().GetWorkflowId())
	require.Equal(t, runID, untouched.GetLinkedTo().GetRunId())
	require.Empty(t, untouched.GetListeners())
	require.Nil(t, untouched.GetLatest())
	require.Zero(t, untouched.GetRetainedCount())

	require.Equal(t, int32(1), c.mustNotifyLinked(id, name, 1), "the owner listens")
	task := c.poll(id)
	scheduled := c.scheduledNotifications(id, task)
	requireNotifications(t, scheduled, map[string]int64{name: 1})
	requireLinkedTo(t, scheduled, id, runID)
	c.complete(task, false)
	require.False(t, c.hasPendingTask(id), "History acknowledged it, so nothing is redelivered")

	desc := c.mustDescribeLinked(id, name)
	require.Equal(t, notificationpb.CHANNEL_KIND_LINKED, desc.GetKind())
	require.Len(t, desc.GetListeners(), 1)
	require.Equal(t, id, desc.GetListeners()[0].GetWorkflow().GetWorkflowId())
	require.Equal(t, runID, desc.GetListeners()[0].GetWorkflow().GetRunId())
	require.Equal(t, int64(1), desc.GetLatest().GetCounter())
	require.Equal(t, id, desc.GetLatest().GetLinkedTo().GetWorkflowId())
	require.Equal(t, int32(1), desc.GetRetainedCount())

	// The independent channel of the same name is untouched by all of this.
	_, err := c.env.FrontendClient().DescribeChannel(c.ctx(), &workflowservice.DescribeChannelRequest{
		Namespace: c.ns, Channel: name,
	})
	requireNotFound(t, err)
}

// The owner folds as a subscribed listener does: a notification waiting for
// a scheduled event, or carried by a task that has not started, absorbs a
// repeat at or below its counter with no write, and a higher one replaces
// the pending entry.
func TestLinkedChannelFoldsWhilePendingAndIntoUnstartedTask(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "linked-fold-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	c.startIdle(id)

	c.mustNotifyLinked(id, name, 1)
	require.True(t, c.hasPendingTask(id), "the notify scheduled a task carrying 1")
	c.mustNotifyLinked(id, name, 2)
	transitions := c.stateTransitions(id)
	require.Equal(t, int32(1), c.mustNotifyLinked(id, name, 1), "the unstarted task carries it")
	require.Equal(t, transitions, c.stateTransitions(id), "a fold into the unstarted task writes nothing")

	first := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, first), map[string]int64{name: 1})
	c.complete(first, false)

	require.True(t, c.hasPendingTask(id), "2 arrived while the first task was open")
	transitions = c.stateTransitions(id)
	c.mustNotifyLinked(id, name, 2)
	require.Equal(t, transitions, c.stateTransitions(id), "held by the scheduled task, so no write")
	second := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, second), map[string]int64{name: 2})
	c.complete(second, false)
	require.False(t, c.hasPendingTask(id))

	require.Equal(t, int32(2), c.mustDescribeLinked(id, name).GetRetainedCount(), "repeats do not join the ring")
}

// Once the task carrying a counter has run, the same counter is a new reason
// to run: a watcher that finds the record with no task open notifies again,
// and the owner gets a task whose scheduled event carries it.
func TestLinkedChannelRepeatAfterTaskRunsAgain(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "linked-repeat-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	c.startIdle(id)

	c.mustNotifyLinked(id, name, 1)
	task := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 1})
	c.complete(task, false)
	require.False(t, c.hasPendingTask(id))

	c.mustNotifyLinked(id, name, 1)
	again := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, again), map[string]int64{name: 1})
	c.complete(again, false)
	require.Equal(t, int32(1), c.mustDescribeLinked(id, name).GetRetainedCount())
}

// A linked channel is addressed by workflow id, so after continue-as-new the
// same calls reach the new run. Nothing carries over: the successor starts
// with an empty channel, and the run that closed answers NotFound.
func TestLinkedChannelFollowsContinueAsNew(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "linked-can-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	firstRun := c.startIdle(id)

	c.mustNotifyLinked(id, name, 1)
	task := c.poll(id)
	c.complete(task, false, c.continueAsNewCommand(id))
	c.complete(c.poll(id), false)

	desc := c.mustDescribeLinked(id, name)
	secondRun := desc.GetLinkedTo().GetRunId()
	require.NotEqual(t, firstRun, secondRun)
	require.Empty(t, desc.GetListeners(), "the successor starts with an empty channel")
	require.Zero(t, desc.GetRetainedCount())

	require.Equal(t, int32(1), c.mustNotifyLinked(id, name, 2))
	next := c.poll(id)
	scheduled := c.scheduledNotifications(id, next)
	requireNotifications(t, scheduled, map[string]int64{name: 2})
	requireLinkedTo(t, scheduled, id, secondRun)
	c.complete(next, false)

	_, err := c.notifyLinked(linkedOwner(id, firstRun), name, 3)
	requireNotFound(t, err)
	_, err = c.describeLinked(linkedOwner(id, firstRun), name)
	requireNotFound(t, err)
}

// A poll on a linked channel reads the owner's ring: counters above the
// caller's, oldest first, up to the page, and a long poll returns one
// published while it waits. A name nobody has notified answers empty.
func TestLinkedChannelPoll(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "linked-poll-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID := c.startIdle(id)
	owner := linkedOwner(id, "")

	empty, err := c.pollLinked(owner, name, 0, 0, 0)
	require.NoError(t, err)
	require.Empty(t, empty.GetNotifications())

	c.mustNotifyLinked(id, name, 1)
	c.mustNotifyLinked(id, name, 2)
	c.mustNotifyLinked(id, name, 3)

	resp, err := c.pollLinked(owner, name, 1, 0, 0)
	require.NoError(t, err)
	requireNotifications(t, resp.GetNotifications()[:1], map[string]int64{name: 2})
	require.Len(t, resp.GetNotifications(), 2)
	requireLinkedTo(t, resp.GetNotifications(), id, runID)

	resp, err = c.pollLinked(owner, name, 1, 0, 1)
	require.NoError(t, err)
	require.Len(t, resp.GetNotifications(), 1)
	require.Equal(t, int64(2), resp.GetNotifications()[0].GetCounter())

	done := make(chan *workflowservice.PollChannelResponse, 1)
	errs := make(chan error, 1)
	go func() {
		resp, err := c.pollLinked(owner, name, 3, 10*time.Second, 0)
		if err != nil {
			errs <- err
			return
		}
		done <- resp
	}()
	// Nothing says when the poll has parked, so it is given a moment. Were the
	// notify to land first, the poll would return it all the same.
	time.Sleep(time.Second) //nolint:forbidigo // no signal says the poll is waiting
	c.mustNotifyLinked(id, name, 4)
	select {
	case resp := <-done:
		requireNotifications(t, resp.GetNotifications(), map[string]int64{name: 4})
	case err := <-errs:
		t.Fatalf("the long poll failed: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the long poll did not return on the notify")
	}
}

// A callback listener on a linked channel is posted each notification, with
// linked_to naming the owner, and one that registers after a notify is handed
// the latest once. The owner counts as a listener beside it.
func TestLinkedChannelCallbackListener(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "linked-callback-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	c.startIdle(id)
	recorder, url := newCallbackRecorder(t)

	register := func(requestID string) string {
		resp, err := c.env.FrontendClient().RegisterChannelListener(c.ctx(),
			&workflowservice.RegisterChannelListenerRequest{
				Namespace: c.ns,
				Channel:   name,
				Callback: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: url},
				}},
				RequestId:         requestID,
				Identity:          "tester",
				WorkflowExecution: linkedOwner(id, ""),
			})
		require.NoError(t, err)
		return resp.GetListenerId()
	}
	requestID := uuid.NewString()
	listenerID := register(requestID)
	require.Equal(t, listenerID, register(requestID), "a retried registration finds its listener")
	require.Len(t, c.mustDescribeLinked(id, name).GetListeners(), 2, "the owner and the callback")

	require.Equal(t, int32(2), c.mustNotifyLinked(id, name, 1))
	select {
	case <-recorder.arrived:
	case <-time.After(20 * time.Second):
		t.Fatal("the callback was not posted")
	}
	await.Require(c.ctx(), t, func(t *await.T) {
		require.Equal(t, []string{"1"}, recorder.counters())
	}, 5*time.Second, 20*time.Millisecond)
	recorder.mu.Lock()
	require.Equal(t, name, recorder.channels[0])
	linkedTo, _ := recorder.bodies[0]["linkedTo"].(map[string]any)
	require.Equal(t, id, linkedTo["workflowId"])
	recorder.mu.Unlock()
	task := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 1})
	c.complete(task, false)

	late := register(uuid.NewString())
	select {
	case <-recorder.arrived:
	case <-time.After(20 * time.Second):
		t.Fatal("the late listener was not handed the latest")
	}
	await.Require(c.ctx(), t, func(t *await.T) {
		require.Equal(t, []string{"1", "1"}, recorder.counters())
	}, 5*time.Second, 20*time.Millisecond)
	require.False(t, c.hasPendingTask(id), "the handoff is the callback's, not the owner's")

	for _, l := range []string{listenerID, late} {
		_, err := c.env.FrontendClient().UnregisterChannelListener(c.ctx(),
			&workflowservice.UnregisterChannelListenerRequest{
				Namespace: c.ns, Channel: name, ListenerId: l, Identity: "tester",
				WorkflowExecution: linkedOwner(id, ""),
			})
		require.NoError(t, err)
	}
	require.Len(t, c.mustDescribeLinked(id, name).GetListeners(), 1, "the owner remains")
	require.Equal(t, int32(1), c.mustNotifyLinked(id, name, 2))
}

// A workflow that has closed took its linked channels with it, and a
// workflow that never existed has none.
func TestLinkedChannelClosedChainIsNotFound(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "linked-closed-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID := c.start(id)
	c.complete(c.poll(id), false, completeWorkflowCommand()...)

	for _, owner := range []*commonpb.WorkflowExecution{linkedOwner(id, ""), linkedOwner(id, runID)} {
		_, err := c.describeLinked(owner, name)
		requireNotFound(t, err)
		_, err = c.notifyLinked(owner, name, 1)
		requireNotFound(t, err)
		_, err = c.pollLinked(owner, name, 0, 0, 0)
		requireNotFound(t, err)
	}
	_, err := c.describeLinked(linkedOwner("absent-"+uuid.NewString(), ""), name)
	requireNotFound(t, err)
	_, err = c.notifyLinked(linkedOwner("absent-"+uuid.NewString(), ""), name, 1)
	requireNotFound(t, err)
}

// With the linked kind switched off, the channel calls ignore the workflow
// they name and reach the independent channel of that name, as before the
// kind existed: a notify with an owner wakes the workflow that subscribed
// with the command, describe answers the independent kind, and an untouched
// name with an owner is NotFound rather than an empty linked channel.
func TestLinkedChannelKindSwitchedOff(t *testing.T) {
	c := newChannelTestEnv(t, testcore.WithDynamicConfig(channel.LinkedKindEnabledSetting, false))
	id := "linked-off-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID := c.subscribe(id, name)

	_, err := c.describeLinked(linkedOwner(id, ""), "untouched-"+uuid.NewString())
	requireNotFound(t, err)

	resp, err := c.notifyLinked(linkedOwner(id, ""), name, 1)
	require.NoError(t, err)
	require.Equal(t, int32(1), resp.GetListenerCount(), "the subscribed run, on the independent channel")
	task := c.poll(id)
	scheduled := c.scheduledNotifications(id, task)
	requireNotifications(t, scheduled, map[string]int64{name: 1})
	require.Nil(t, scheduled[0].GetLinkedTo(), "delivered by the independent channel")
	c.complete(task, false)

	desc := c.mustDescribeLinked(id, name)
	require.Equal(t, notificationpb.CHANNEL_KIND_INDEPENDENT, desc.GetKind())
	require.Nil(t, desc.GetLinkedTo())
	require.Len(t, desc.GetListeners(), 1)
	require.Equal(t, runID, desc.GetListeners()[0].GetWorkflow().GetRunId())
	require.Equal(t, int32(1), desc.GetRetainedCount())

	polled, err := c.pollLinked(linkedOwner(id, ""), name, 0, 0, 0)
	require.NoError(t, err)
	requireNotifications(t, polled.GetNotifications(), map[string]int64{name: 1})
}

// One run holds a bounded number of linked channels, and each keeps a
// bounded ring of its own.
func TestLinkedChannelLimits(t *testing.T) {
	c := newChannelTestEnv(t,
		testcore.WithDynamicConfig(channel.MaxLinkedChannelsPerWorkflowSetting, 1),
		testcore.WithDynamicConfig(channel.LinkedRetainedNotificationsSetting, 2),
	)
	id := "linked-limits-" + uuid.NewString()
	first := "orders-" + uuid.NewString()
	second := "billing-" + uuid.NewString()
	c.startIdle(id)

	require.Equal(t, int32(1), c.mustNotifyLinked(id, first, 1))
	_, err := c.notifyLinked(linkedOwner(id, ""), second, 1)
	requireResourceExhausted(t, err)
	require.Equal(t, int32(1), c.mustNotifyLinked(id, first, 2), "the first channel still takes notifications")
	c.mustNotifyLinked(id, first, 3)

	require.Equal(t, int32(2), c.mustDescribeLinked(id, first).GetRetainedCount())
	resp, err := c.pollLinked(linkedOwner(id, ""), first, 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3}, []int64{
		resp.GetNotifications()[0].GetCounter(), resp.GetNotifications()[1].GetCounter(),
	})
}
