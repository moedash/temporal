package tests

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	channelservice "go.temporal.io/server/chasm/lib/channel/service"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// channelTestEnv drives the notification channel with the raw client, playing
// the worker by hand so each scheduled event can be inspected.
type channelTestEnv struct {
	*wakeTestEnv
}

func newChannelTestEnv(t *testing.T, opts ...testcore.TestOption) *channelTestEnv {
	opts = append([]testcore.TestOption{
		testcore.WithDynamicConfig(callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 50*time.Millisecond),
	}, opts...)
	return &channelTestEnv{wakeTestEnv: newWakeTestEnv(t, opts...)}
}

func subscribeChannelCommand(name string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SUBSCRIBE_NOTIFICATION_CHANNEL,
		Attributes: &commandpb.Command_SubscribeNotificationChannelCommandAttributes{
			SubscribeNotificationChannelCommandAttributes: &commandpb.SubscribeNotificationChannelCommandAttributes{
				Channel: name,
			},
		},
	}
}

func channelNotification(name string, counter int64) *notificationpb.Notification {
	return &notificationpb.Notification{
		Channel:  name,
		Position: []byte(name + "@" + strconv.FormatInt(counter, 10)),
		Counter:  counter,
		Metadata: map[string]*commonpb.Payload{
			"topic": {Data: []byte("t" + strconv.FormatInt(counter, 10))},
		},
	}
}

func (c *channelTestEnv) notify(name string, counter int64) (*workflowservice.NotifyChannelResponse, error) {
	return c.env.FrontendClient().NotifyChannel(c.ctx(), &workflowservice.NotifyChannelRequest{
		Namespace:    c.ns,
		Notification: channelNotification(name, counter),
		Identity:     "tester",
		RequestId:    uuid.NewString(),
	})
}

func (c *channelTestEnv) mustNotify(name string, counter int64) int32 {
	c.t.Helper()
	resp, err := c.notify(name, counter)
	require.NoError(c.t, err)
	return resp.GetListenerCount()
}

func (c *channelTestEnv) describe(name string) *workflowservice.DescribeChannelResponse {
	c.t.Helper()
	resp, err := c.env.FrontendClient().DescribeChannel(c.ctx(), &workflowservice.DescribeChannelRequest{
		Namespace: c.ns,
		Channel:   name,
	})
	require.NoError(c.t, err)
	return resp
}

// workflowListenerRun is the run the channel has on record for a workflow id,
// or the empty string when it has none.
func (c *channelTestEnv) workflowListenerRun(name, workflowID string) string {
	c.t.Helper()
	for _, l := range c.describe(name).GetListeners() {
		if l.GetWorkflow().GetWorkflowId() == workflowID {
			return l.GetWorkflow().GetRunId()
		}
	}
	return ""
}

// subscribe starts a workflow and completes its first task with a subscribe
// command, returning the run id.
func (c *channelTestEnv) subscribe(id string, channels ...string) string {
	c.t.Helper()
	runID := c.start(id)
	var commands []*commandpb.Command
	for _, name := range channels {
		commands = append(commands, subscribeChannelCommand(name))
	}
	c.complete(c.poll(id), false, commands...)
	for _, name := range channels {
		require.Equal(c.t, runID, c.workflowListenerRun(name, id))
	}
	return runID
}

// scheduledNotifications reads the notifications on the scheduled event of
// the task, from History as a replaying worker would.
func (c *channelTestEnv) scheduledNotifications(
	id string,
	task *workflowservice.PollWorkflowTaskQueueResponse,
) []*notificationpb.Notification {
	c.t.Helper()
	events := c.env.GetHistory(c.ns, task.GetWorkflowExecution())
	var scheduled *historypb.HistoryEvent
	for _, e := range events {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED &&
			e.GetEventId() <= task.GetStartedEventId() {
			scheduled = e
		}
	}
	require.NotNil(c.t, scheduled, "no scheduled event for the task of %s", id)
	return scheduled.GetWorkflowTaskScheduledEventAttributes().GetNotifications()
}

// awaitPending waits until the run holds a pending notification for the
// channel at the counter, which is how a test knows a fan-out reached it
// while it had no way to schedule a task.
func (c *channelTestEnv) awaitPending(id, name string, counter int64) {
	c.t.Helper()
	await.Require(c.ctx(), c.t, func(t *await.T) {
		nodes := c.persisted(id).GetDatabaseMutableState().GetChasmNodes()
		node, ok := nodes["ChannelNotifications#"+name]
		require.True(t, ok, "no pending notification for %s yet", name)
		var entry chasmworkflowpb.ChannelNotificationEntry
		require.NoError(t, proto.Unmarshal(node.GetData().GetData(), &entry))
		require.Equal(t, counter, entry.GetCounter())
	}, 20*time.Second, 50*time.Millisecond)
}

// awaitCallbackPending waits until the callback listener holds a pending
// notification at the counter behind the one in flight.
func (c *channelTestEnv) awaitCallbackPending(name, listenerID string, counter int64) {
	c.t.Helper()
	await.Require(c.ctx(), c.t, func(t *await.T) {
		resp, err := c.env.AdminClient().DescribeMutableState(c.ctx(),
			&adminservice.DescribeMutableStateRequest{
				Namespace: c.ns,
				Execution: &commonpb.WorkflowExecution{WorkflowId: name},
				Archetype: channelservice.Archetype,
			})
		require.NoError(t, err)
		node, ok := resp.GetDatabaseMutableState().GetChasmNodes()["CallbackListeners#"+listenerID]
		require.True(t, ok)
		var listener channelpb.CallbackListener
		require.NoError(t, proto.Unmarshal(node.GetData().GetData(), &listener))
		require.Equal(t, counter, listener.GetPending().GetCounter())
	}, 20*time.Second, 50*time.Millisecond)
}

func requireNotifications(t *testing.T, got []*notificationpb.Notification, want map[string]int64) {
	t.Helper()
	require.Len(t, got, len(want), "notifications: %v", got)
	for _, n := range got {
		counter, ok := want[n.GetChannel()]
		require.True(t, ok, "unexpected notification from %q", n.GetChannel())
		require.Equal(t, counter, n.GetCounter(), "counter of %q", n.GetChannel())
		require.Equal(t, []byte(n.GetChannel()+"@"+strconv.FormatInt(counter, 10)), n.GetPosition())
		require.Equal(t, []byte("t"+strconv.FormatInt(counter, 10)), n.GetMetadata()["topic"].GetData())
	}
}

func requireInvalidArgument(t *testing.T, err error) {
	t.Helper()
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid)
}

// A workflow subscribes by completing a task with the command, the event says
// so, and a notify then schedules one task whose scheduled event carries the
// notification. History is where it lives: the poll response carries nothing
// of its own, and a replayed history shows the same event.
func TestNotificationChannelSubscribeAndNotify(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "channel-basic-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID := c.subscribe(id, name)

	events := c.env.GetHistory(c.ns, &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID})
	var subscribed *historypb.WorkflowNotificationChannelSubscribedEventAttributes
	for _, e := range events {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_SUBSCRIBED {
			subscribed = e.GetWorkflowNotificationChannelSubscribedEventAttributes()
		}
	}
	require.NotNil(t, subscribed)
	require.Equal(t, name, subscribed.GetChannel())
	require.Positive(t, subscribed.GetWorkflowTaskCompletedEventId())
	require.False(t, c.hasPendingTask(id))

	require.Equal(t, int32(1), c.mustNotify(name, 1))
	task := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 1})
	require.Empty(t, task.GetWakes())
	c.complete(task, false)
	require.False(t, c.hasPendingTask(id), "History acknowledged it, so nothing is redelivered")

	// A replayed history reads the same scheduled event.
	reread, err := c.env.FrontendClient().GetWorkflowExecutionHistory(c.ctx(),
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: c.ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID},
		})
	require.NoError(t, err)
	var carried int
	for _, e := range reread.GetHistory().GetEvents() {
		if n := e.GetWorkflowTaskScheduledEventAttributes().GetNotifications(); len(n) > 0 {
			requireNotifications(t, n, map[string]int64{name: 1})
			carried++
		}
	}
	require.Equal(t, 1, carried)

	desc := c.describe(name)
	require.Len(t, desc.GetListeners(), 1)
	require.Equal(t, id, desc.GetListeners()[0].GetWorkflow().GetWorkflowId())
	require.Equal(t, int64(1), desc.GetLatest().GetCounter())
	require.Equal(t, int32(1), desc.GetRetainedCount())
}

// Notifications that reach a workflow while it has a task open wait for the
// next scheduled event and fold there into the highest counter.
func TestNotificationChannelFoldsWhileTaskOpen(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "channel-fold-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	c.subscribe(id, name)

	c.mustNotify(name, 1)
	open := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, open), map[string]int64{name: 1})

	for counter := int64(2); counter <= 4; counter++ {
		c.mustNotify(name, counter)
	}
	c.awaitPending(id, name, 4)

	c.complete(open, false)
	next := c.poll(id)
	got := c.scheduledNotifications(id, next)
	require.Len(t, got, 1, "one per channel")
	require.Equal(t, int64(4), got[0].GetCounter())
	c.complete(next, false)
	require.False(t, c.hasPendingTask(id))
}

// Every listener is woken, and the writer learns how many there were.
func TestNotificationChannelWakesEveryListener(t *testing.T) {
	c := newChannelTestEnv(t)
	name := "orders-" + uuid.NewString()
	first := "channel-many-a-" + uuid.NewString()
	second := "channel-many-b-" + uuid.NewString()
	c.subscribe(first, name)
	c.subscribe(second, name)

	require.Equal(t, int32(2), c.mustNotify(name, 7))
	for _, id := range []string{first, second} {
		task := c.poll(id)
		requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 7})
		c.complete(task, false)
	}
}

// A subscription ends with its run. After a continue-as-new the predecessor's
// listener is dropped on the next notify unless the successor subscribes,
// and a successor that subscribes takes the entry and gets the next one.
func TestNotificationChannelContinueAsNew(t *testing.T) {
	c := newChannelTestEnv(t)
	name := "orders-" + uuid.NewString()

	id := "channel-can-" + uuid.NewString()
	predecessor := c.subscribe(id, name)
	c.mustNotify(name, 1)
	task := c.poll(id)
	c.complete(task, false, c.continueAsNewCommand(id))
	successorTask := c.poll(id)
	require.NotEqual(t, predecessor, successorTask.GetWorkflowExecution().GetRunId())
	c.complete(successorTask, false)

	c.mustNotify(name, 2)
	await.Require(c.ctx(), t, func(t *await.T) {
		require.Empty(t, c.describe(name).GetListeners(), "the closed run's listener goes")
	}, 20*time.Second, 50*time.Millisecond)
	require.False(t, c.hasPendingTask(id), "a successor that did not subscribe is not woken")

	// A new subscriber is handed the latest the channel holds, and a
	// successor that subscribes again is handed it too, then the next one.
	id2 := "channel-can2-" + uuid.NewString()
	c.subscribe(id2, name)
	task = c.poll(id2)
	requireNotifications(t, c.scheduledNotifications(id2, task), map[string]int64{name: 2})
	c.complete(task, false)
	c.mustNotify(name, 3)
	task = c.poll(id2)
	requireNotifications(t, c.scheduledNotifications(id2, task), map[string]int64{name: 3})
	c.complete(task, false, c.continueAsNewCommand(id2))
	successorTask = c.poll(id2)
	successor := successorTask.GetWorkflowExecution().GetRunId()
	c.complete(successorTask, false, subscribeChannelCommand(name))
	require.Equal(t, successor, c.workflowListenerRun(name, id2), "the successor takes the entry")
	c.mustNotify(name, 4)
	task = c.poll(id2)
	require.Equal(t, successor, task.GetWorkflowExecution().GetRunId())
	requireNotifications(t, c.scheduledNotifications(id2, task), map[string]int64{name: 3})
	c.complete(task, false)
	task = c.poll(id2)
	requireNotifications(t, c.scheduledNotifications(id2, task), map[string]int64{name: 4})
}

// A reset run is rebuilt from History, subscription event included, so it
// listens. The channel still has the reset-away run on record, and the next
// notify follows the chain to the reset run and re-keys the listener.
func TestNotificationChannelReKeysToResetRun(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "channel-reset-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	base := c.subscribe(id, name)

	c.mustNotify(name, 1)
	task := c.poll(id)
	c.complete(task, false)

	resp, err := c.env.FrontendClient().ResetWorkflowExecution(c.ctx(),
		&workflowservice.ResetWorkflowExecutionRequest{
			Namespace:                 c.ns,
			WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: id, RunId: base},
			Reason:                    "test",
			WorkflowTaskFinishEventId: task.GetStartedEventId(),
			RequestId:                 uuid.NewString(),
		})
	require.NoError(t, err)
	resetRun := resp.GetRunId()
	c.complete(c.poll(id), false)
	require.Equal(t, base, c.workflowListenerRun(name, id))

	c.mustNotify(name, 2)
	task = c.poll(id)
	require.Equal(t, resetRun, task.GetWorkflowExecution().GetRunId())
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 2})
	await.Require(c.ctx(), t, func(t *await.T) {
		require.Equal(t, resetRun, c.workflowListenerRun(name, id))
	}, 20*time.Second, 50*time.Millisecond)
}

// callbackRecorder is an HTTP endpoint that records what the channel posts,
// and can hold a request open to keep a delivery in flight.
type callbackRecorder struct {
	mu       sync.Mutex
	bodies   []map[string]any
	channels []string
	hold     chan struct{}
	arrived  chan struct{}
}

func newCallbackRecorder(t *testing.T) (*callbackRecorder, string) {
	r := &callbackRecorder{arrived: make(chan struct{}, 100)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		data, _ := io.ReadAll(req.Body)
		var body map[string]any
		_ = json.Unmarshal(data, &body)
		r.mu.Lock()
		r.bodies = append(r.bodies, body)
		r.channels = append(r.channels, req.Header.Get(channelservice.ChannelHeader))
		hold := r.hold
		r.mu.Unlock()
		r.arrived <- struct{}{}
		if hold != nil {
			<-hold
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return r, server.URL + "/notify"
}

func (r *callbackRecorder) counters() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.bodies))
	for i, b := range r.bodies {
		out[i], _ = b["counter"].(string)
	}
	return out
}

// A callback listener is posted each notification as JSON with the channel
// in a header, and while one post is in flight what arrives folds into one,
// sent when the post completes.
func TestNotificationChannelCallbackListener(t *testing.T) {
	c := newChannelTestEnv(t)
	name := "orders-" + uuid.NewString()
	recorder, url := newCallbackRecorder(t)

	register := func(requestID string) string {
		resp, err := c.env.FrontendClient().RegisterChannelListener(c.ctx(),
			&workflowservice.RegisterChannelListenerRequest{
				Namespace: c.ns,
				Channel:   name,
				Callback: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: url},
				}},
				RequestId: requestID,
				Identity:  "tester",
			})
		require.NoError(t, err)
		return resp.GetListenerId()
	}
	requestID := uuid.NewString()
	listenerID := register(requestID)
	require.Equal(t, listenerID, register(requestID), "a retried registration finds its listener")

	require.Equal(t, int32(1), c.mustNotify(name, 1))
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
	require.Equal(t, name, recorder.bodies[0]["channel"])
	recorder.hold = make(chan struct{})
	hold := recorder.hold
	recorder.mu.Unlock()

	c.mustNotify(name, 2)
	select {
	case <-recorder.arrived:
	case <-time.After(20 * time.Second):
		t.Fatal("the second notification was not posted")
	}
	c.mustNotify(name, 3)
	c.mustNotify(name, 4)
	// The fan-outs for 3 and 4 hand their notifications over while 2 is still
	// in flight, and 4 replaces 3.
	c.awaitCallbackPending(name, listenerID, 4)
	recorder.mu.Lock()
	recorder.hold = nil
	recorder.mu.Unlock()
	close(hold)

	await.Require(c.ctx(), t, func(t *await.T) {
		require.Equal(t, []string{"1", "2", "4"}, recorder.counters())
	}, 20*time.Second, 50*time.Millisecond)

	_, err := c.env.FrontendClient().UnregisterChannelListener(c.ctx(),
		&workflowservice.UnregisterChannelListenerRequest{
			Namespace: c.ns, Channel: name, ListenerId: listenerID, Identity: "tester",
		})
	require.NoError(t, err)
	require.Empty(t, c.describe(name).GetListeners())
	require.Equal(t, int32(0), c.mustNotify(name, 5))
}

// A long poll returns a notification published while it waits.
func TestNotificationChannelPollWaits(t *testing.T) {
	c := newChannelTestEnv(t)
	name := "orders-" + uuid.NewString()
	require.Equal(t, int32(0), c.mustNotify(name, 1), "nobody listens yet")

	done := make(chan *workflowservice.PollChannelResponse, 1)
	errs := make(chan error, 1)
	go func() {
		resp, err := c.env.FrontendClient().PollChannel(c.ctx(), &workflowservice.PollChannelRequest{
			Namespace:    c.ns,
			Channel:      name,
			AfterCounter: 1,
			Wait:         durationpb.New(15 * time.Second),
		})
		if err != nil {
			errs <- err
			return
		}
		done <- resp
	}()
	// Nothing says when the poll has parked, so it is given a moment. Were the
	// notify to land first, the poll would return it all the same.
	time.Sleep(time.Second) //nolint:forbidigo // no signal says the poll is waiting
	c.mustNotify(name, 2)
	select {
	case resp := <-done:
		require.Len(t, resp.GetNotifications(), 1)
		require.Equal(t, int64(2), resp.GetNotifications()[0].GetCounter())
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(20 * time.Second):
		t.Fatal("the poll did not return")
	}
}

// A notify with no listeners is retained, so a poll that arrives later
// catches up on it; describe reports what the channel holds.
func TestNotificationChannelRetainsWithoutListeners(t *testing.T) {
	c := newChannelTestEnv(t)
	name := "orders-" + uuid.NewString()
	require.Equal(t, int32(0), c.mustNotify(name, 1))
	require.Equal(t, int32(0), c.mustNotify(name, 2))

	resp, err := c.env.FrontendClient().PollChannel(c.ctx(), &workflowservice.PollChannelRequest{
		Namespace: c.ns,
		Channel:   name,
	})
	require.NoError(t, err)
	requireNotifications(t, resp.GetNotifications()[:1], map[string]int64{name: 1})
	require.Len(t, resp.GetNotifications(), 2)

	resp, err = c.env.FrontendClient().PollChannel(c.ctx(), &workflowservice.PollChannelRequest{
		Namespace: c.ns, Channel: name, AfterCounter: 1, MaxNotifications: 1,
	})
	require.NoError(t, err)
	require.Len(t, resp.GetNotifications(), 1)
	require.Equal(t, int64(2), resp.GetNotifications()[0].GetCounter())

	desc := c.describe(name)
	require.Empty(t, desc.GetListeners())
	require.Equal(t, int64(2), desc.GetLatest().GetCounter())
	require.Equal(t, int32(2), desc.GetRetainedCount())

	_, err = c.env.FrontendClient().DescribeChannel(c.ctx(), &workflowservice.DescribeChannelRequest{
		Namespace: c.ns, Channel: "absent-" + uuid.NewString(),
	})
	requireNotFound(t, err)
}

// The frontend refuses what the channel could not accept, and the limits hold.
func TestNotificationChannelValidationAndLimits(t *testing.T) {
	c := newChannelTestEnv(t,
		testcore.WithDynamicConfig(channel.MaxListenersSetting, 1),
		testcore.WithDynamicConfig(channel.MaxMetadataBytesSetting, 16),
		testcore.WithDynamicConfig(channel.MaxSubscriptionsPerWorkflowSetting, 1),
		testcore.WithDynamicConfig(channel.RetainedNotificationsSetting, 2),
	)
	name := "orders-" + uuid.NewString()
	notify := func(n *notificationpb.Notification) error {
		_, err := c.env.FrontendClient().NotifyChannel(c.ctx(), &workflowservice.NotifyChannelRequest{
			Namespace: c.ns, Notification: n,
		})
		return err
	}
	requireInvalidArgument(t, notify(nil))
	requireInvalidArgument(t, notify(&notificationpb.Notification{Counter: 1}))
	requireInvalidArgument(t, notify(&notificationpb.Notification{Channel: name}))
	requireInvalidArgument(t, notify(&notificationpb.Notification{
		Channel: name, Counter: 1, Position: make([]byte, channel.MaxPositionBytes+1),
	}))
	requireInvalidArgument(t, notify(&notificationpb.Notification{
		Channel: name, Counter: 1,
		Metadata: map[string]*commonpb.Payload{"k": {Data: make([]byte, 64)}},
	}))
	requireInvalidArgument(t, notify(&notificationpb.Notification{
		Channel: string(make([]byte, 2000)), Counter: 1,
	}))

	// The ring keeps the newest two.
	for counter := int64(1); counter <= 3; counter++ {
		c.mustNotify(name, counter)
	}
	require.Equal(t, int32(2), c.describe(name).GetRetainedCount())

	// One listener is the limit.
	id := "channel-limit-" + uuid.NewString()
	c.subscribe(id, name)
	_, err := c.env.FrontendClient().RegisterChannelListener(c.ctx(),
		&workflowservice.RegisterChannelListenerRequest{
			Namespace: c.ns,
			Channel:   name,
			Callback: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: "http://localhost:1/notify"},
			}},
			RequestId: uuid.NewString(),
		})
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)

	// One subscription per run is the limit, and the refusal fails the task
	// with the channel's cause.
	c.mustNotify(name, 4)
	task := c.poll(id)
	_, err = c.env.FrontendClient().RespondWorkflowTaskCompleted(c.ctx(),
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: c.ns,
			TaskToken: task.GetTaskToken(),
			Commands:  []*commandpb.Command{subscribeChannelCommand("other-" + uuid.NewString())},
			Identity:  "tester",
		})
	requireInvalidArgument(t, err)
	var failed *historypb.WorkflowTaskFailedEventAttributes
	for _, e := range c.env.GetHistory(c.ns, task.GetWorkflowExecution()) {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
			failed = e.GetWorkflowTaskFailedEventAttributes()
		}
	}
	require.NotNil(t, failed)
	require.Equal(t,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SUBSCRIBE_NOTIFICATION_CHANNEL_ATTRIBUTES,
		failed.GetCause())
}

// The notify rate is per namespace; past it a notify is refused with the rate
// limit cause.
func TestNotificationChannelNotifyRate(t *testing.T) {
	c := newChannelTestEnv(t, testcore.WithDynamicConfig(channel.NotifyPerSecondSetting, 1))
	name := "orders-" + uuid.NewString()
	var refused error
	for counter := int64(1); counter <= 20 && refused == nil; counter++ {
		_, refused = c.notify(name, counter)
	}
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, refused, &exhausted)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, exhausted.Cause)
}

// eventsOfType returns the run's events of one type, in order.
func (c *channelTestEnv) eventsOfType(
	execution *commonpb.WorkflowExecution,
	eventType enumspb.EventType,
) []*historypb.HistoryEvent {
	var out []*historypb.HistoryEvent
	for _, e := range c.env.GetHistory(c.ns, execution) {
		if e.GetEventType() == eventType {
			out = append(out, e)
		}
	}
	return out
}

// retryScheduled is the scheduled event History records for a retried task,
// written when the retry completes: the last one at attempt two.
func (c *channelTestEnv) retryScheduled(
	retry *workflowservice.PollWorkflowTaskQueueResponse,
) *historypb.WorkflowTaskScheduledEventAttributes {
	c.t.Helper()
	var out *historypb.WorkflowTaskScheduledEventAttributes
	for _, e := range c.eventsOfType(retry.GetWorkflowExecution(), enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED) {
		if attrs := e.GetWorkflowTaskScheduledEventAttributes(); attrs.GetAttempt() == 2 {
			out = attrs
		}
	}
	require.NotNil(c.t, out, "no scheduled event for the retry")
	return out
}

// A retry after a failed task does not carry again what the failed attempt's
// scheduled event carried: an SDK reads every scheduled event since its last
// completed task, so carrying it twice would deliver it twice. A notification
// that arrives after the failure rides the scheduled event after the retry.
func TestNotificationChannelRetryDoesNotRecarry(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "channel-retry-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	c.subscribe(id, name)

	c.mustNotify(name, 1)
	task := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 1})
	_, err := c.env.FrontendClient().RespondWorkflowTaskFailed(c.ctx(),
		&workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: c.ns,
			TaskToken: task.GetTaskToken(),
			Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
			Identity:  "tester",
		})
	require.NoError(t, err)

	retry := c.poll(id)
	require.Equal(t, int32(2), retry.GetAttempt())
	c.complete(retry, false)
	require.Empty(t, c.retryScheduled(retry).GetNotifications(), "the retry does not carry it again")
	require.False(t, c.hasPendingTask(id))

	// A notification arriving between a failure and the retry is not carried
	// by the retry, whose scheduled event the worker never saw with it; it
	// rides the next scheduled event, alone.
	c.mustNotify(name, 2)
	task = c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 2})
	_, err = c.env.FrontendClient().RespondWorkflowTaskFailed(c.ctx(),
		&workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: c.ns,
			TaskToken: task.GetTaskToken(),
			Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
			Identity:  "tester",
		})
	require.NoError(t, err)
	c.mustNotify(name, 3)
	c.awaitPending(id, name, 3)
	retry = c.poll(id)
	c.complete(retry, false)
	require.Empty(t, c.retryScheduled(retry).GetNotifications())
	next := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, next), map[string]int64{name: 3})
}

// A notify that found no listeners is handed to a workflow that subscribes
// afterwards, so a reader that checked its source, found nothing and
// subscribed is not left waiting on a write that landed in between.
func TestNotificationChannelSubscribeGetsLatest(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "channel-late-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	require.Equal(t, int32(0), c.mustNotify(name, 1))
	require.Equal(t, int32(0), c.mustNotify(name, 2))

	c.subscribe(id, name)
	task := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 2})
	c.complete(task, false)
	require.False(t, c.hasPendingTask(id))
}

// Subscribing to a channel that holds nothing schedules nothing. A second
// subscribe to the same channel records its own event, since every command
// needs one, and changes nothing else: no new listener and no delivery.
func TestNotificationChannelSubscribeOnEmptyChannel(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "channel-empty-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID := c.subscribe(id, name)
	require.False(t, c.hasPendingTask(id))

	c.mustNotify(name, 1)
	task := c.poll(id)
	c.complete(task, false, subscribeChannelCommand(name))
	execution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID}
	subscribed := c.eventsOfType(execution, enumspb.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_SUBSCRIBED)
	require.Len(t, subscribed, 2)
	require.Equal(t, name,
		subscribed[1].GetWorkflowNotificationChannelSubscribedEventAttributes().GetChannel())
	require.Len(t, c.describe(name).GetListeners(), 1)
	require.False(t, c.hasPendingTask(id), "the latest is not handed out again")

}

// channelTransitions reads the channel execution's state transition count,
// which moves on every write.
func (c *channelTestEnv) channelTransitions(name string) int64 {
	c.t.Helper()
	resp, err := c.env.AdminClient().DescribeMutableState(c.ctx(),
		&adminservice.DescribeMutableStateRequest{
			Namespace: c.ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: name},
			Archetype: channelservice.Archetype,
		})
	require.NoError(c.t, err)
	return resp.GetDatabaseMutableState().GetExecutionInfo().GetStateTransitionCount()
}

// The stall the wake round fixed: a listener's task runs to completion, and a
// watcher that later finds the same record with no task open notifies the
// same counter again. That is a new reason to run, so it schedules a task
// whose scheduled event carries the counter. It does not join the ring again.
func TestNotificationChannelRepeatAfterTaskWakesAgain(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "channel-repeat-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	c.subscribe(id, name)

	c.mustNotify(name, 1)
	task := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 1})
	c.complete(task, false)
	require.False(t, c.hasPendingTask(id))

	require.Equal(t, int32(1), c.mustNotify(name, 1))
	task = c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 1})
	require.Equal(t, int32(1), c.describe(name).GetRetainedCount())

	// While the open task keeps the repeat pending, the same counter again
	// writes neither the run nor the channel.
	c.mustNotify(name, 2)
	c.awaitPending(id, name, 2)
	runWrites, channelWrites := c.stateTransitions(id), c.channelTransitions(name)
	require.Equal(t, int32(1), c.mustNotify(name, 2))
	require.Equal(t, runWrites, c.stateTransitions(id), "folded into the pending entry")
	require.Equal(t, channelWrites, c.channelTransitions(name), "the ring and the table are unchanged")
	c.complete(task, false)
	next := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, next), map[string]int64{name: 2})
}
