package tests

import (
	"context"
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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	channelservice "go.temporal.io/server/chasm/lib/channel/service"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// channelTestEnv drives the notification channel with the raw client, playing
// the worker by hand so each scheduled event can be inspected.
type channelTestEnv struct {
	t   *testing.T
	env *testcore.TestEnv
	ns  string
}

func newChannelTestEnv(t *testing.T, opts ...testcore.TestOption) *channelTestEnv {
	opts = append([]testcore.TestOption{
		testcore.WithDynamicConfig(callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
		testcore.WithDynamicConfig(callback.RetryPolicyInitialInterval, 10*time.Millisecond),
		testcore.WithDynamicConfig(callback.RetryPolicyMaximumInterval, 50*time.Millisecond),
	}, opts...)
	env := testcore.NewEnv(t, opts...)
	return &channelTestEnv{t: t, env: env, ns: env.Namespace().String()}
}

func (c *channelTestEnv) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	c.t.Cleanup(cancel)
	return ctx
}

func requireNotFound(t *testing.T, err error) {
	t.Helper()
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
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
		node, ok := resp.GetDatabaseMutableState().GetChasmNodes()["Listeners#"+listenerID]
		require.True(t, ok)
		var listener channelpb.Listener
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

// callbackRecorder is an HTTP endpoint that records what the channel posts,
// and can hold a request open to keep a delivery in flight.
type callbackRecorder struct {
	mu       sync.Mutex
	bodies   []map[string]any
	channels []string
	hold     chan struct{}
	// How long each request takes to answer, for a slow endpoint.
	delay   time.Duration
	arrived chan struct{}
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
		hold, delay := r.hold, r.delay
		r.mu.Unlock()
		r.arrived <- struct{}{}
		if hold != nil {
			<-hold
		}
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-req.Context().Done():
			}
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

// Deliveries to a callback are in order: one post is in flight per listener,
// and what arrives during it folds into the one post sent after it returns.
// A slow endpoint under a burst of rising counters sees them rise and ends on
// the last one.
func TestNotificationChannelCallbackDeliveriesInOrder(t *testing.T) {
	c := newChannelTestEnv(t)
	name := "orders-" + uuid.NewString()
	recorder, url := newCallbackRecorder(t)
	recorder.mu.Lock()
	recorder.delay = 150 * time.Millisecond
	recorder.mu.Unlock()
	_, err := c.env.FrontendClient().RegisterChannelListener(c.ctx(),
		&workflowservice.RegisterChannelListenerRequest{
			Namespace: c.ns,
			Channel:   name,
			Callback: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: url},
			}},
			RequestId: uuid.NewString(),
			Identity:  "tester",
		})
	require.NoError(t, err)

	const last = int64(12)
	pace := time.NewTicker(40 * time.Millisecond)
	defer pace.Stop()
	for counter := int64(1); counter <= last; counter++ {
		require.Equal(t, int32(1), c.mustNotify(name, counter))
		<-pace.C
	}
	await.Require(c.ctx(), t, func(t *await.T) {
		seen := recorder.counters()
		require.NotEmpty(t, seen)
		require.Equal(t, strconv.FormatInt(last, 10), seen[len(seen)-1])
	}, 20*time.Second, 50*time.Millisecond)

	seen := recorder.counters()
	previous := int64(0)
	for _, s := range seen {
		counter, err := strconv.ParseInt(s, 10, 64)
		require.NoError(t, err)
		require.Greater(t, counter, previous, "deliveries out of order: %v", seen)
		previous = counter
	}
	require.Equal(t, last, previous)
}

// A channel with no listeners is deleted a retention after its last activity,
// ring included. A notify after that starts a fresh channel, and a poller
// holding a counter from the old one sees only what the new one has.
func TestNotificationChannelRecreatedAfterRetention(t *testing.T) {
	c := newChannelTestEnv(t, testcore.WithDynamicConfig(channel.RetentionSetting, time.Second))
	name := "orders-" + uuid.NewString()
	for counter := int64(1); counter <= 3; counter++ {
		require.Equal(t, int32(0), c.mustNotify(name, counter))
	}
	require.Equal(t, int32(3), c.describe(name).GetRetainedCount())

	await.Require(c.ctx(), t, func(t *await.T) {
		_, err := c.env.FrontendClient().DescribeChannel(c.ctx(), &workflowservice.DescribeChannelRequest{
			Namespace: c.ns, Channel: name,
		})
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound, "the idle channel is still there")
	}, 20*time.Second, 100*time.Millisecond)

	require.Equal(t, int32(0), c.mustNotify(name, 7))
	resp, err := c.env.FrontendClient().PollChannel(c.ctx(), &workflowservice.PollChannelRequest{
		Namespace: c.ns, Channel: name, AfterCounter: 2,
	})
	require.NoError(t, err)
	require.Len(t, resp.GetNotifications(), 1, "the old ring is gone with the old channel")
	require.Equal(t, int64(7), resp.GetNotifications()[0].GetCounter())
	desc := c.describe(name)
	require.Equal(t, int32(1), desc.GetRetainedCount())
	require.Equal(t, int64(7), desc.GetLatest().GetCounter())
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
