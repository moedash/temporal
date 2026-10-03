package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A change to a native stream notifies the channel named by the stream, so a
// client learns of the stream the way it learns of an external one. These
// cases write through the stream service and read the channel.

// newStreamChannelEnv serves the channel calls and the stream calls from one
// cluster, with activities on for the activity-owned cases.
func newStreamChannelEnv(
	t *testing.T,
	opts ...testcore.TestOption,
) (*channelTestEnv, *streamTestEnv) {
	c := newChannelTestEnv(t, opts...)
	on := []dynamicconfig.ConstrainedValue{{
		Constraints: dynamicconfig.Constraints{Namespace: c.ns},
		Value:       true,
	}}
	cluster := c.env.GetTestCluster()
	cluster.OverrideDynamicConfig(t, dynamicconfig.EnableChasm, on)
	cluster.OverrideDynamicConfig(t, activity.Enabled, on)
	return c, newStreamTestEnvFrom(t, c.env)
}

// requireChange checks one stream-driven notification: the change sequence
// as the counter, the head paired with the run as the position, and closed
// only on the close.
func requireChange(
	t *testing.T,
	n *notificationpb.Notification,
	channel, run string,
	counter, head int64,
	closed bool,
) {
	t.Helper()
	require.Equal(t, channel, n.GetChannel())
	require.Equal(t, counter, n.GetCounter(), "counter of %q", channel)
	require.Equal(t, string(stream.EncodePosition(run, head)), string(n.GetPosition()))
	if !closed {
		require.Empty(t, n.GetMetadata(), "metadata of change %d", counter)
		return
	}
	var isClosed bool
	require.NoError(t, payload.Decode(n.GetMetadata()[stream.ClosedMetadataKey], &isClosed))
	require.True(t, isClosed)
}

func (c *channelTestEnv) pollIndependent(name string, after int64) []*notificationpb.Notification {
	c.t.Helper()
	resp, err := c.env.FrontendClient().PollChannel(c.ctx(), &workflowservice.PollChannelRequest{
		Namespace: c.ns, Channel: name, AfterCounter: after,
	})
	require.NoError(c.t, err)
	return resp.GetNotifications()
}

// awaitIndependent waits for the channel's latest notification to reach the
// counter, since a standalone stream reaches its channel through a task, and
// returns what the channel retains above the given counter. The task reads
// the latest change when it runs, so a burst may arrive as fewer
// notifications than changes, in order.
func (c *channelTestEnv) awaitIndependent(
	name string,
	after, latest int64,
) []*notificationpb.Notification {
	c.t.Helper()
	var out []*notificationpb.Notification
	await.Require(c.ctx(), c.t, func(t *await.T) {
		resp, err := c.env.FrontendClient().PollChannel(c.ctx(), &workflowservice.PollChannelRequest{
			Namespace: c.ns, Channel: name, AfterCounter: after,
		})
		require.NoError(t, err)
		out = resp.GetNotifications()
		require.NotEmpty(t, out)
		require.Equal(t, latest, out[len(out)-1].GetCounter())
	}, 20*time.Second, 50*time.Millisecond)
	for i := 1; i < len(out); i++ {
		require.Less(c.t, out[i-1].GetCounter(), out[i].GetCounter(), "in order")
	}
	return out
}

func (c *channelTestEnv) registerCallback(
	name, url string,
	owner *commonpb.Execution,
) string {
	c.t.Helper()
	resp, err := c.env.FrontendClient().RegisterChannelListener(c.ctx(),
		&workflowservice.RegisterChannelListenerRequest{
			Namespace: c.ns,
			Channel:   name,
			Callback: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: url},
			}},
			RequestId: uuid.NewString(),
			Identity:  "tester",
			Execution: owner,
		})
	require.NoError(c.t, err)
	return resp.GetListenerId()
}

func awaitCounters(t *testing.T, recorder *callbackRecorder, want ...string) {
	t.Helper()
	await.Require(t.Context(), t, func(t *await.T) {
		require.Equal(t, want, recorder.counters())
	}, 20*time.Second, 50*time.Millisecond)
}

// A stream a workflow owns notifies the channel of the stream's name linked
// to the run: one notification per append, whatever the batch holds, with
// the head as its position. The owner is not woken by its own stream, a
// callback registered on the channel is handed the latest and then each
// change, and the stream's own readers see nothing different.
func TestStreamChannelWorkflowOwnedStream(t *testing.T) {
	c, s := newStreamChannelEnv(t)
	id := "stream-channel-wf-" + uuid.NewString()
	runID := c.startIdle(id)
	owner := &streamlib.StreamOwner{Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW, Id: id}
	name := stream.OwnedChannelName("output")
	linked := linkedOwner(id, "")

	_, err := s.addOwned(t, owner, "output", &streamlib.AddWorkflowMessagesInput{
		Records: streamMsgs("", "a", "b"),
	})
	require.NoError(t, err)
	resp, err := c.pollLinked(linked, name, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, resp.GetNotifications(), 1, "one notification per append call")
	requireChange(t, resp.GetNotifications()[0], name, runID, 1, 2, false)
	require.Equal(t, id, resp.GetNotifications()[0].GetLinkedTo().GetBusinessId())
	require.False(t, c.hasPendingTask(id), "the owner is not woken by its own stream")

	recorder, url := newCallbackRecorder(t)
	c.registerCallback(name, url, linked)
	awaitCounters(t, recorder, "1")

	_, err = s.addOwned(t, owner, "output", &streamlib.AddWorkflowMessagesInput{
		Records: streamMsgs("", "c"),
	})
	require.NoError(t, err)
	resp, err = c.pollLinked(linked, name, 1, 0, 0)
	require.NoError(t, err)
	require.Len(t, resp.GetNotifications(), 1)
	requireChange(t, resp.GetNotifications()[0], name, runID, 2, 3, false)
	awaitCounters(t, recorder, "1", "2")
	require.False(t, c.hasPendingTask(id))

	// A producer's retry appends nothing and notifies nothing.
	retry := &streamlib.AddWorkflowMessagesInput{
		Records: streamMsgs("", "d"), ProducerId: "p", Sequence: 1,
	}
	_, err = s.addOwned(t, owner, "output", retry)
	require.NoError(t, err)
	_, err = s.addOwned(t, owner, "output", retry)
	require.NoError(t, err)
	desc := c.mustDescribeLinked(id, name)
	require.Equal(t, notificationpb.CHANNEL_KIND_LINKED, desc.GetKind())
	require.Equal(t, int64(3), desc.GetLatest().GetCounter())
	require.Equal(t, int32(3), desc.GetRetainedCount())
	awaitCounters(t, recorder, "1", "2", "3")

	require.Equal(t, []string{"a", "b", "c", "d"},
		bodies(s.pollOwned(t, owner, "output", 0, false).GetRecords()))
}

// A standalone stream notifies the independent channel of its id through a
// task that carries the latest change. A callback that registers after
// three appends is handed the third once, and the close is the last change,
// marked closed.
func TestStreamChannelStandaloneStream(t *testing.T) {
	c, s := newStreamChannelEnv(t)
	ctx := s.ctx()
	streamID := "stream-channel-sa-" + uuid.NewString()
	name := stream.ChannelName(streamID)
	s.create(ctx, t, streamID)
	_, err := s.add(ctx, t, streamID, &streamlib.AddMessagesInput{Records: streamMsgs("", "a")})
	require.NoError(t, err)
	first := c.awaitIndependent(name, 0, 1)
	requireChange(t, first[0], name, streamID, 1, 1, false)
	require.Nil(t, first[0].GetLinkedTo())

	for _, body := range []string{"b", "c"} {
		_, err := s.add(ctx, t, streamID, &streamlib.AddMessagesInput{Records: streamMsgs("", body)})
		require.NoError(t, err)
	}
	changes := c.awaitIndependent(name, 1, 3)
	last := changes[len(changes)-1]
	requireChange(t, last, name, streamID, 3, 3, false)

	recorder, url := newCallbackRecorder(t)
	c.registerCallback(name, url, nil)
	awaitCounters(t, recorder, "3")

	_, err = s.client.CloseStream(ctx, &streamlib.CloseStreamRequest{
		FrontendRequest: &streamlib.CloseStreamInput{Namespace: s.ns, StreamId: streamID},
	})
	require.NoError(t, err)
	closed := c.awaitIndependent(name, 3, 4)
	require.Len(t, closed, 1)
	requireChange(t, closed[0], name, streamID, 4, 3, true)
	awaitCounters(t, recorder, "3", "4")

	// A second close changes nothing.
	_, err = s.client.CloseStream(ctx, &streamlib.CloseStreamRequest{
		FrontendRequest: &streamlib.CloseStreamInput{Namespace: s.ns, StreamId: streamID},
	})
	require.NoError(t, err)
	desc := c.describe(name)
	require.Equal(t, notificationpb.CHANNEL_KIND_INDEPENDENT, desc.GetKind())
	require.Equal(t, int64(4), desc.GetLatest().GetCounter())
	require.Len(t, desc.GetListeners(), 1)
}

// A stream an activity of a workflow owns notifies a channel named by the
// activity and the stream, linked to the workflow run, and the activity's
// completion closes the stream, which is the last change.
func TestStreamChannelWorkflowActivityStream(t *testing.T) {
	c, s := newStreamChannelEnv(t)
	id := "stream-channel-wfa-" + uuid.NewString()
	runID := scheduleStreamingActivity(t, c.env, s, id, "model-call", nil)
	task := pollActivityTask(t, c.env, s, id+"-tq")
	owner := &streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY, Id: id, ActivityId: "model-call",
	}
	name := stream.ActivityChannelName("model-call", "output")

	_, err := s.addOwned(t, owner, "output", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(1, "token"),
	})
	require.NoError(t, err)
	resp, err := c.pollLinked(linkedOwner(id, ""), name, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, resp.GetNotifications(), 1)
	requireChange(t, resp.GetNotifications()[0], name, runID, 1, 1, false)

	completeActivityAttempt(t, c.env, s, task)
	await.Require(c.ctx(), t, func(at *await.T) {
		require.True(at, s.describeOwned(t, owner, "output").GetClosed())
	}, 20*time.Second, 50*time.Millisecond)
	resp, err = c.pollLinked(linkedOwner(id, ""), name, 1, 0, 0)
	require.NoError(t, err)
	require.Len(t, resp.GetNotifications(), 1)
	requireChange(t, resp.GetNotifications()[0], name, runID, 2, 1, true)
}

// A stream a standalone activity owns has no linked channel to live in, so
// it notifies the independent channel named by the activity and the stream.
func TestStreamChannelStandaloneActivityStream(t *testing.T) {
	c, s := newStreamChannelEnv(t)
	activityID := "stream-channel-saa-" + uuid.NewString()
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
	pollActivityTask(t, c.env, s, activityID+"-tq")
	owner := &streamlib.StreamOwner{Kind: streamlib.STREAM_OWNER_KIND_ACTIVITY, Id: activityID}
	name := stream.ActivityChannelName(activityID, "output")

	_, err = s.addOwned(t, owner, "output", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(1, "token one", "token two"),
	})
	require.NoError(t, err)
	changes := c.awaitIndependent(name, 0, 1)
	require.Len(t, changes, 1)
	requireChange(t, changes[0], name, started.GetRunId(), 1, 2, false)
}

// With the switch off, streams create no channel state at all.
func TestStreamChannelSwitchOff(t *testing.T) {
	c, s := newStreamChannelEnv(t, testcore.WithDynamicConfig(stream.NotifyChannelSetting, false))
	id := "stream-channel-off-" + uuid.NewString()
	c.startIdle(id)
	owner := &streamlib.StreamOwner{Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW, Id: id}
	_, err := s.addOwned(t, owner, "output", &streamlib.AddWorkflowMessagesInput{
		Records: streamMsgs("", "a"),
	})
	require.NoError(t, err)
	desc := c.mustDescribeLinked(id, stream.OwnedChannelName("output"))
	require.Nil(t, desc.GetLatest(), "the linked name exists by construction and holds nothing")
	require.Zero(t, desc.GetRetainedCount())

	ctx := s.ctx()
	streamID := "stream-channel-off-sa-" + uuid.NewString()
	s.create(ctx, t, streamID)
	_, err = s.add(ctx, t, streamID, &streamlib.AddMessagesInput{Records: streamMsgs("", "a")})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, bodies(s.poll(ctx, t, streamID, 0).GetRecords()))
	_, err = c.env.FrontendClient().DescribeChannel(c.ctx(), &workflowservice.DescribeChannelRequest{
		Namespace: c.ns, Channel: stream.ChannelName(streamID),
	})
	requireNotFound(t, err)
}
