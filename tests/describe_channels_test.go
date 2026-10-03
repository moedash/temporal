package tests

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/await"
	"google.golang.org/protobuf/encoding/protojson"
)

// DescribeWorkflowExecution reports the notification channels a run stands
// on. These cases drive the run with the raw client and read the field after
// each step, so what describe shows at each point of a notification's life is
// pinned down.

const (
	kindIndependent = notificationpb.CHANNEL_KIND_INDEPENDENT
	kindLinked      = notificationpb.CHANNEL_KIND_LINKED
)

// channelSubscriptions is what describe reports of the run's channels, the
// current run when no run id is given.
func (c *channelTestEnv) channelSubscriptions(
	id, runID string,
) []*workflowpb.ChannelSubscriptionInfo {
	c.t.Helper()
	resp, err := c.env.FrontendClient().DescribeWorkflowExecution(c.ctx(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: c.ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID},
		})
	require.NoError(c.t, err)
	return resp.GetChannelSubscriptions()
}

// channelSubscription is the current run's entry for a channel of a kind.
func (c *channelTestEnv) channelSubscription(
	id, name string,
	kind notificationpb.ChannelKind,
) *workflowpb.ChannelSubscriptionInfo {
	c.t.Helper()
	infos := c.channelSubscriptions(id, "")
	for _, info := range infos {
		if info.GetChannel() == name && info.GetKind() == kind {
			return info
		}
	}
	require.Failf(c.t, "channel not described", "%s %s on %s: %v", kind, name, id, infos)
	return nil
}

// subscribedEventID is the event that recorded the run's subscription.
func (c *channelTestEnv) subscribedEventID(id, runID, name string) int64 {
	c.t.Helper()
	execution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID}
	subscribed := enumspb.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_SUBSCRIBED
	for _, e := range c.eventsOfType(execution, subscribed) {
		if e.GetWorkflowNotificationChannelSubscribedEventAttributes().GetChannel() == name {
			return e.GetEventId()
		}
	}
	require.Failf(c.t, "no subscribe event", "%s on %s", name, id)
	return 0
}

// requireStanding checks the three counters of an entry. A zero pending
// counter means no pending notification.
func requireStanding(
	t *testing.T,
	info *workflowpb.ChannelSubscriptionInfo,
	last, pending, scheduled int64,
) {
	t.Helper()
	name := info.GetChannel()
	require.Equal(t, last, info.GetLastCounter(), "last counter of %q", name)
	require.Equal(t, scheduled, info.GetScheduledCounter(), "scheduled counter of %q", name)
	if pending == 0 {
		require.Nil(t, info.GetPendingNotification(), "pending notification of %q", name)
		return
	}
	n := info.GetPendingNotification()
	require.Equal(t, pending, n.GetCounter(), "pending counter of %q", name)
	require.Equal(t, name, n.GetChannel())
	require.Equal(t, []byte(name+"@"+strconv.FormatInt(pending, 10)), n.GetPosition())
	require.Equal(t, []byte("t"+strconv.FormatInt(pending, 10)), n.GetMetadata()["topic"].GetData())
}

// awaitStanding waits for the counters of an independent channel's entry,
// since a notify on one reaches the run through the channel's fan-out task.
func (c *channelTestEnv) awaitStanding(id, name string, last, pending, scheduled int64) {
	c.t.Helper()
	await.Require(c.ctx(), c.t, func(t *await.T) {
		info := c.channelSubscription(id, name, kindIndependent)
		require.Equal(t, last, info.GetLastCounter())
		require.Equal(t, pending, info.GetPendingNotification().GetCounter())
		require.Equal(t, scheduled, info.GetScheduledCounter())
	}, 20*time.Second, 50*time.Millisecond)
	requireStanding(c.t, c.channelSubscription(id, name, kindIndependent), last, pending, scheduled)
}

// A subscribed channel is listed from its event on, with nothing accepted. A
// notify with no task open schedules one in the write that accepts it, so
// the counter shows as what the unstarted task carries, and the task's start
// clears that. A notify while a task is open waits as the pending
// notification, moves to the task the completion schedules, and once that
// task has run only the last counter remains.
func TestDescribeWorkflowSubscribedChannel(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "describe-subscribed-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID := c.subscribe(id, name)

	infos := c.channelSubscriptions(id, "")
	require.Len(t, infos, 1)
	info := infos[0]
	require.Equal(t, name, info.GetChannel())
	require.Equal(t, kindIndependent, info.GetKind())
	require.Equal(t, c.subscribedEventID(id, runID, name), info.GetSubscribedEventId())
	requireStanding(t, info, 0, 0, 0)
	require.Zero(t, info.GetListenerCount(), "the channel execution's, not the run's")
	require.Zero(t, info.GetRetainedCount())
	require.Zero(t, info.GetAcceptedCount())

	c.mustNotify(name, 1)
	c.awaitStanding(id, name, 1, 0, 1)
	open := c.poll(id)
	requireStanding(t, c.channelSubscription(id, name, kindIndependent), 1, 0, 0)

	c.mustNotify(name, 2)
	c.awaitStanding(id, name, 2, 2, 0)
	c.complete(open, false)
	requireStanding(t, c.channelSubscription(id, name, kindIndependent), 2, 0, 2)

	c.complete(c.poll(id), false)
	info = c.channelSubscription(id, name, kindIndependent)
	requireStanding(t, info, 2, 0, 0)
	require.Equal(t, c.subscribedEventID(id, runID, name), info.GetSubscribedEventId())
	require.Len(t, c.channelSubscriptions(id, ""), 1)
}

// A linked channel is listed once it holds state. The notify that creates it
// is the owner's own write, so what the unstarted task carries shows at once,
// and a second notify before the task starts waits beside it as the pending
// notification. The counts are the channel's: callback listeners without the
// owner, the ring, and every notification that raised the latest.
func TestDescribeWorkflowLinkedChannel(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "describe-linked-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID := c.startIdle(id)

	require.Equal(t, kindLinked, c.mustDescribeLinked(id, name).GetKind(), "exists by construction")
	require.Empty(t, c.channelSubscriptions(id, ""), "and holds nothing, so it is not listed")

	c.mustNotifyLinked(id, name, 1)
	info := c.channelSubscription(id, name, kindLinked)
	require.Len(t, c.channelSubscriptions(id, ""), 1)
	require.Zero(t, info.GetSubscribedEventId())
	requireStanding(t, info, 1, 0, 1)
	require.Zero(t, info.GetListenerCount())
	require.Equal(t, int32(1), info.GetRetainedCount())
	require.Equal(t, int64(1), info.GetAcceptedCount())

	c.mustNotifyLinked(id, name, 2)
	info = c.channelSubscription(id, name, kindLinked)
	requireStanding(t, info, 2, 2, 1)
	require.Equal(t, id, info.GetPendingNotification().GetLinkedTo().GetBusinessId())
	require.Equal(t, runID, info.GetPendingNotification().GetLinkedTo().GetRunId())
	require.Equal(t, int32(2), info.GetRetainedCount())
	require.Equal(t, int64(2), info.GetAcceptedCount())

	first := c.poll(id)
	requireStanding(t, c.channelSubscription(id, name, kindLinked), 2, 2, 0)
	c.complete(first, false)
	requireStanding(t, c.channelSubscription(id, name, kindLinked), 2, 0, 2)
	c.complete(c.poll(id), false)
	requireStanding(t, c.channelSubscription(id, name, kindLinked), 2, 0, 0)

	// A repeat wakes the owner again but joins no ring and counts as nothing
	// accepted.
	c.mustNotifyLinked(id, name, 2)
	info = c.channelSubscription(id, name, kindLinked)
	requireStanding(t, info, 2, 0, 2)
	require.Equal(t, int32(2), info.GetRetainedCount())
	require.Equal(t, int64(2), info.GetAcceptedCount())
	c.complete(c.poll(id), false)

	recorder, url := newCallbackRecorder(t)
	_, err := c.env.FrontendClient().RegisterChannelListener(c.ctx(),
		&workflowservice.RegisterChannelListenerRequest{
			Namespace: c.ns,
			Channel:   name,
			Callback: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: url},
			}},
			RequestId: uuid.NewString(),
			Identity:  "tester",
			Execution: linkedOwner(id, ""),
		})
	require.NoError(t, err)
	require.Equal(t, int32(1), c.channelSubscription(id, name, kindLinked).GetListenerCount())
	select {
	case <-recorder.arrived:
	case <-time.After(20 * time.Second):
		t.Fatal("the new listener was not handed the latest")
	}
}

// Entries sort by channel name, and a name the run both subscribed to and
// holds a linked channel under is listed twice, the subscribed kind first.
func TestDescribeWorkflowChannelsSortedAcrossKinds(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "describe-sorted-" + uuid.NewString()
	suffix := uuid.NewString()
	a, b, d := "a-"+suffix, "b-"+suffix, "d-"+suffix
	c.subscribe(id, b, a)
	c.mustNotifyLinked(id, d, 1)
	c.mustNotifyLinked(id, b, 1)

	infos := c.channelSubscriptions(id, "")
	require.Len(t, infos, 4)
	var names []string
	var kinds []notificationpb.ChannelKind
	for _, info := range infos {
		names = append(names, info.GetChannel())
		kinds = append(kinds, info.GetKind())
	}
	require.Equal(t, []string{a, b, b, d}, names)
	require.Equal(t,
		[]notificationpb.ChannelKind{kindIndependent, kindIndependent, kindLinked, kindLinked},
		kinds)
}

// Both kinds end with the run. A continue-as-new successor lists nothing
// until it subscribes, and then lists its own event. A reset run is rebuilt
// from History with the subscribe event, so it lists the subscription.
func TestDescribeWorkflowChannelsAcrossRuns(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "describe-runs-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	linkedName := "billing-" + uuid.NewString()
	first := c.subscribe(id, name)
	c.mustNotifyLinked(id, linkedName, 1)
	require.Len(t, c.channelSubscriptions(id, ""), 2)

	c.complete(c.poll(id), false, c.continueAsNewCommand(id))
	successorTask := c.poll(id)
	second := successorTask.GetWorkflowExecution().GetRunId()
	require.NotEqual(t, first, second)
	require.Empty(t, c.channelSubscriptions(id, second), "the successor starts with none")
	require.Len(t, c.channelSubscriptions(id, first), 2, "the closed run keeps its record")

	c.complete(successorTask, false, subscribeChannelCommand(name))
	infos := c.channelSubscriptions(id, "")
	require.Len(t, infos, 1)
	require.Equal(t, name, infos[0].GetChannel())
	require.Equal(t, c.subscribedEventID(id, second, name), infos[0].GetSubscribedEventId())
	requireStanding(t, infos[0], 0, 0, 0)

	// Reset the successor to the end of its next task. The rebuilt History
	// holds the subscribe event, so the reset run lists the subscription under
	// the same event id, and the next notify reaches it.
	c.mustNotify(name, 1)
	c.awaitStanding(id, name, 1, 0, 1)
	task := c.poll(id)
	c.complete(task, false)
	resp, err := c.env.FrontendClient().ResetWorkflowExecution(c.ctx(),
		&workflowservice.ResetWorkflowExecutionRequest{
			Namespace:                 c.ns,
			WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: id, RunId: second},
			Reason:                    "test",
			WorkflowTaskFinishEventId: task.GetStartedEventId(),
			RequestId:                 uuid.NewString(),
		})
	require.NoError(t, err)
	resetRun := resp.GetRunId()
	c.complete(c.poll(id), false)
	infos = c.channelSubscriptions(id, resetRun)
	require.Len(t, infos, 1)
	require.Equal(t, name, infos[0].GetChannel())
	require.Equal(t, kindIndependent, infos[0].GetKind())
	require.Equal(t, c.subscribedEventID(id, second, name), infos[0].GetSubscribedEventId())

	c.mustNotify(name, 2)
	c.awaitStanding(id, name, 2, 0, 2)
	require.Equal(t, resetRun, c.poll(id).GetWorkflowExecution().GetRunId())
}

// The HTTP describe route carries the field under its JSON name.
func TestDescribeWorkflowChannelsOverHTTP(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "describe-http-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	c.subscribe(id, name)
	c.mustNotify(name, 1)
	c.awaitStanding(id, name, 1, 0, 1)

	url := "http://" + c.env.HttpAPIAddress() + "/namespaces/" + c.ns + "/workflows/" + id
	req, err := http.NewRequestWithContext(c.ctx(), http.MethodGet, url, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json+no-payload-shorthand")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))
	entries, ok := raw["channelSubscriptions"].([]any)
	require.True(t, ok, "no channelSubscriptions in %s", body)
	require.Len(t, entries, 1)
	entry, _ := entries[0].(map[string]any)
	require.Equal(t, name, entry["channel"])
	require.Equal(t, "CHANNEL_KIND_INDEPENDENT", entry["kind"])
	require.Equal(t, "1", entry["lastCounter"], "int64 fields are JSON strings")
	require.Equal(t, "1", entry["scheduledCounter"])

	var described workflowservice.DescribeWorkflowExecutionResponse
	require.NoError(t, protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(body, &described))
	require.Len(t, described.GetChannelSubscriptions(), 1)
	requireStanding(t, described.GetChannelSubscriptions()[0], 1, 0, 1)
}
