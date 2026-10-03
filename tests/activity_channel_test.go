package tests

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/channel"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A channel linked to a standalone activity lives in the activity's state, as
// a workflow's linked channel lives in the run's. The activity is not among
// its listeners: nothing like a Workflow Task would carry a notification to
// it, so a notify reaches callbacks and pollers only. These cases address the
// activity as an Execution of type ACTIVITY and read the channel back.

func activityOwner(id, runID string) *commonpb.Execution {
	return &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: id, RunId: runID}
}

// startStandaloneActivity starts an activity of its own and polls its task,
// so the activity is running and can be completed by the test.
func startStandaloneActivity(
	t *testing.T,
	c *channelTestEnv,
	s *streamTestEnv,
	activityID string,
) (string, *workflowservice.PollActivityTaskQueueResponse) {
	t.Helper()
	started, err := c.env.FrontendClient().StartActivityExecution(s.ctx(),
		&workflowservice.StartActivityExecutionRequest{
			Namespace:           s.ns,
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "streaming-activity"},
			TaskQueue:           c.taskQueue(activityID),
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           uuid.NewString(),
		})
	require.NoError(t, err)
	task := pollActivityTask(t, c.env, s, activityID+"-tq")
	return started.GetRunId(), task
}

func requireActivityLinkedTo(t *testing.T, got *commonpb.Execution, id, runID string) {
	t.Helper()
	require.Equal(t, enumspb.EXECUTION_TYPE_ACTIVITY, got.GetType())
	require.Equal(t, id, got.GetBusinessId())
	require.Equal(t, runID, got.GetRunId())
}

// A notify addressed to a standalone activity lands on the activity's own
// linked channel, with or without the run named. Nobody is woken: the
// activity is not a listener, so the count is zero and describe lists none.
// Pollers read the ring with linked_to naming the activity, a wrong run is
// NotFound, and the channel goes with the activity when it completes.
func TestActivityChannelNotifyPollDescribe(t *testing.T) {
	c, s := newStreamChannelEnv(t)
	activityID := "activity-channel-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID, task := startStandaloneActivity(t, c, s, activityID)

	// Before anyone notifies, the name is a linked channel with nothing in
	// it, which is what a client probes for.
	desc, err := c.describeLinked(activityOwner(activityID, ""), "untouched-"+uuid.NewString())
	require.NoError(t, err)
	require.Equal(t, notificationpb.CHANNEL_KIND_LINKED, desc.GetKind())
	requireActivityLinkedTo(t, desc.GetLinkedTo(), activityID, runID)
	require.Empty(t, desc.GetListeners())
	require.Nil(t, desc.GetLatest())

	resp, err := c.notifyLinked(activityOwner(activityID, ""), name, 1)
	require.NoError(t, err)
	require.Equal(t, int32(0), resp.GetListenerCount(), "the activity is not a listener")
	resp, err = c.notifyLinked(activityOwner(activityID, runID), name, 2)
	require.NoError(t, err)
	require.Equal(t, int32(0), resp.GetListenerCount())

	polled, err := c.pollLinked(activityOwner(activityID, ""), name, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, polled.GetNotifications(), 2)
	for i, n := range polled.GetNotifications() {
		require.Equal(t, int64(i+1), n.GetCounter())
		requireActivityLinkedTo(t, n.GetLinkedTo(), activityID, runID)
	}
	polled, err = c.pollLinked(activityOwner(activityID, runID), name, 1, 0, 0)
	require.NoError(t, err)
	require.Len(t, polled.GetNotifications(), 1)
	require.Equal(t, int64(2), polled.GetNotifications()[0].GetCounter())

	desc, err = c.describeLinked(activityOwner(activityID, runID), name)
	require.NoError(t, err)
	require.Equal(t, notificationpb.CHANNEL_KIND_LINKED, desc.GetKind())
	requireActivityLinkedTo(t, desc.GetLinkedTo(), activityID, runID)
	require.Empty(t, desc.GetListeners(), "no owner entry for an activity")
	require.Equal(t, int64(2), desc.GetLatest().GetCounter())
	require.Equal(t, int32(2), desc.GetRetainedCount())

	_, err = c.describeLinked(activityOwner(activityID, uuid.NewString()), name)
	requireNotFound(t, err)
	_, err = c.notifyLinked(activityOwner("absent-"+uuid.NewString(), ""), name, 1)
	requireNotFound(t, err)
	_, err = c.notifyLinked(&commonpb.Execution{
		Type: enumspb.EXECUTION_TYPE_NEXUS_OPERATION, BusinessId: activityID,
	}, name, 1)
	requireInvalidArgument(t, err)

	completeActivityAttempt(t, c.env, s, task)
	await.Require(c.ctx(), t, func(t *await.T) {
		_, err := c.describeLinked(activityOwner(activityID, ""), name)
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound, "the channel went with the activity")
	}, 20*time.Second, 50*time.Millisecond)
	_, err = c.notifyLinked(activityOwner(activityID, ""), name, 3)
	requireNotFound(t, err)
}

// A URL callback on an activity's channel is posted each notification with
// linked_to naming the activity, and is the channel's only listener.
func TestActivityChannelCallbackListener(t *testing.T) {
	c, s := newStreamChannelEnv(t)
	activityID := "activity-callback-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID, _ := startStandaloneActivity(t, c, s, activityID)
	recorder, url := newCallbackRecorder(t)

	listenerID := c.registerCallback(name, url, activityOwner(activityID, ""))
	desc, err := c.describeLinked(activityOwner(activityID, ""), name)
	require.NoError(t, err)
	require.Len(t, desc.GetListeners(), 1, "the callback alone")
	require.Equal(t, listenerID, desc.GetListeners()[0].GetListenerId())

	resp, err := c.notifyLinked(activityOwner(activityID, ""), name, 1)
	require.NoError(t, err)
	require.Equal(t, int32(1), resp.GetListenerCount())
	awaitCounters(t, recorder, "1")
	recorder.mu.Lock()
	linkedTo, _ := recorder.bodies[0]["linkedTo"].(map[string]any)
	recorder.mu.Unlock()
	require.Equal(t, "EXECUTION_TYPE_ACTIVITY", linkedTo["type"])
	require.Equal(t, activityID, linkedTo["businessId"])
	require.Equal(t, runID, linkedTo["runId"])

	_, err = c.env.FrontendClient().UnregisterChannelListener(c.ctx(),
		&workflowservice.UnregisterChannelListenerRequest{
			Namespace: c.ns, Channel: name, ListenerId: listenerID, Identity: "tester",
			Execution: activityOwner(activityID, ""),
		})
	require.NoError(t, err)
	desc, err = c.describeLinked(activityOwner(activityID, ""), name)
	require.NoError(t, err)
	require.Empty(t, desc.GetListeners())
}

// The HTTP activity route binds the business id alone and cannot set the
// type, so the server infers it from the route: a notify posted to the
// activities path lands on the activity's linked channel, and the describe
// route reports the owner by type.
func TestActivityChannelOverHTTP(t *testing.T) {
	c, s := newStreamChannelEnv(t)
	activityID := "activity-http-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID, _ := startStandaloneActivity(t, c, s, activityID)
	base := "http://" + c.env.HttpAPIAddress() + "/namespaces/" + c.ns + "/activities/" + activityID

	body, err := protojson.Marshal(&workflowservice.NotifyChannelRequest{
		Notification: channelNotification(name, 1),
		Identity:     "tester",
		RequestId:    uuid.NewString(),
	})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(c.ctx(), http.MethodPost,
		base+"/channels/"+name+"/notify", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(out))

	desc, err := c.describeLinked(activityOwner(activityID, ""), name)
	require.NoError(t, err)
	require.Equal(t, int64(1), desc.GetLatest().GetCounter(), "the route named the activity")
	requireActivityLinkedTo(t, desc.GetLinkedTo(), activityID, runID)

	req, err = http.NewRequestWithContext(c.ctx(), http.MethodGet, base+"/channels/"+name, nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(out))
	var raw map[string]any
	require.NoError(t, json.Unmarshal(out, &raw))
	linkedTo, _ := raw["linkedTo"].(map[string]any)
	require.Equal(t, "EXECUTION_TYPE_ACTIVITY", linkedTo["type"], string(out))
	require.Equal(t, activityID, linkedTo["businessId"])
	require.Equal(t, "CHANNEL_KIND_LINKED", raw["kind"])
}

// With the linked kind switched off, an execution of any type is ignored and
// the call reaches the independent channel of the name.
func TestActivityChannelKindSwitchedOff(t *testing.T) {
	c, s := newStreamChannelEnv(t, testcore.WithDynamicConfig(channel.LinkedKindEnabledSetting, false))
	activityID := "activity-off-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	startStandaloneActivity(t, c, s, activityID)

	resp, err := c.notifyLinked(activityOwner(activityID, ""), name, 1)
	require.NoError(t, err)
	require.Equal(t, int32(0), resp.GetListenerCount())
	desc, err := c.describeLinked(activityOwner(activityID, ""), name)
	require.NoError(t, err)
	require.Equal(t, notificationpb.CHANNEL_KIND_INDEPENDENT, desc.GetKind())
	require.Nil(t, desc.GetLinkedTo())
	require.Equal(t, int64(1), c.describe(name).GetLatest().GetCounter(), "the independent channel")
}
