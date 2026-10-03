package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/testing/await"
	"google.golang.org/protobuf/proto"
)

// A notification that lands while a task runs is carried by the next
// scheduled event. That task is created in the completion's own write and
// handed back with the response when the worker asks for it, the way a task
// for a buffered Signal is, so scheduled and started are one write. Without
// the request the task still goes through matching.

// pendingTaskState is the state describe reports for the run's pending task.
func (c *channelTestEnv) pendingTaskState(id string) enumspb.PendingWorkflowTaskState {
	c.t.Helper()
	resp, err := c.env.FrontendClient().DescribeWorkflowExecution(c.ctx(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: c.ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: id},
		})
	require.NoError(c.t, err)
	return resp.GetPendingWorkflowTask().GetState()
}

// requireInlineHandoff completes the open task asking for the next one and
// checks that the response carries it, started, with the notification on its
// scheduled event, for one state transition. The returned task is the one
// handed back.
func (c *channelTestEnv) requireInlineHandoff(
	id, name string,
	open *workflowservice.PollWorkflowTaskQueueResponse,
	counter int64,
) *workflowservice.PollWorkflowTaskQueueResponse {
	c.t.Helper()
	before := c.stateTransitions(id)
	resp := c.complete(open, true)
	handed := resp.GetWorkflowTask()
	require.NotNil(c.t, handed, "the completion response carries the next task")
	require.NotEmpty(c.t, handed.GetTaskToken())
	require.Positive(c.t, handed.GetStartedEventId(), "handed back started")
	requireNotifications(c.t, c.scheduledNotifications(id, handed), map[string]int64{name: counter})
	require.Equal(c.t, before+1, c.stateTransitions(id), "completion, schedule and start in one write")
	require.Equal(c.t, enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED, c.pendingTaskState(id))
	return handed
}

// A notification from a subscribed channel, arriving while the task is open,
// comes back with the completion response on the task it scheduled.
func TestInlineHandoffSubscribedChannel(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "handoff-subscribed-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	c.subscribe(id, name)

	c.mustNotify(name, 1)
	open := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, open), map[string]int64{name: 1})
	c.mustNotify(name, 2)
	c.awaitPending(id, name, 2)

	handed := c.requireInlineHandoff(id, name, open, 2)
	c.complete(handed, false)
	require.False(t, c.hasPendingTask(id), "nothing was left for matching")

	// A completion that does not ask for the next task leaves it to matching.
	c.mustNotify(name, 3)
	open = c.poll(id)
	c.mustNotify(name, 4)
	c.awaitPending(id, name, 4)
	resp := c.complete(open, false)
	require.Nil(t, resp.GetWorkflowTask())
	require.Equal(t, enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED, c.pendingTaskState(id))
	next := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, next), map[string]int64{name: 4})
	c.complete(next, false)
}

// A notification on a linked channel is accepted in the owner's own write, so
// no wait is needed before the completion hands the task back.
func TestInlineHandoffLinkedChannel(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "handoff-linked-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID := c.startIdle(id)

	c.mustNotifyLinked(id, name, 1)
	open := c.poll(id)
	c.mustNotifyLinked(id, name, 2)

	handed := c.requireInlineHandoff(id, name, open, 2)
	requireLinkedTo(t, c.scheduledNotifications(id, handed), id, runID)
	c.complete(handed, false)
	require.False(t, c.hasPendingTask(id))

	// The handed task has started, so a repeat of what it carries is news
	// again and schedules a task of its own.
	c.mustNotifyLinked(id, name, 3)
	open = c.poll(id)
	c.mustNotifyLinked(id, name, 4)
	handed = c.requireInlineHandoff(id, name, open, 4)
	c.mustNotifyLinked(id, name, 4)
	require.Equal(t, enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED, c.pendingTaskState(id))
	c.complete(handed, false)
	require.Equal(t, enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED, c.pendingTaskState(id),
		"the repeat waited as pending and the completion scheduled its task through matching")
	next := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, next), map[string]int64{name: 4})
	c.complete(next, false)
}

// awaitKnownHead waits until the stream's push of its frontier reached the
// run's cursor, which is what makes the cursor behind and the run owe a task.
func (c *channelTestEnv) awaitKnownHead(id, streamID string, head int64) {
	c.t.Helper()
	await.Require(c.ctx(), c.t, func(t *await.T) {
		nodes := c.persisted(id).GetDatabaseMutableState().GetChasmNodes()
		node, ok := nodes["StreamCursors#"+streamID]
		require.True(t, ok, "no cursor for %s yet", streamID)
		var cursor streamlib.WorkflowStreamCursor
		require.NoError(t, proto.Unmarshal(node.GetData().GetData(), &cursor))
		require.Equal(t, head, cursor.GetKnownHead())
	}, 20*time.Second, 50*time.Millisecond)
}

// sliceBodies are the record bodies of the live slice a task carries, apart
// from the consumed ranges a non-sticky poll re-supplies for replay.
func sliceBodies(t *testing.T, slices []*streampb.StreamSlice) []string {
	t.Helper()
	var out []string
	for _, r := range currentSlice(t, slices).GetRecords() {
		out = append(out, string(r.GetBody().GetData()))
	}
	return out
}

// A native stream's frontier moving past a subscription's cursor while the
// task runs is the same case: the completion hands back the task that
// carries the slice, for one state transition, and without the request the
// task goes through matching.
func TestInlineHandoffNativeStream(t *testing.T) {
	c := newChannelTestEnv(t)
	s := newStreamTestEnvFrom(t, c.env)
	id := "handoff-native-" + uuid.NewString()
	streamID := "handoff-stream-" + uuid.NewString()
	s.create(s.ctx(), t, streamID)
	c.startIdle(id)
	_, err := s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: c.ns, WorkflowId: id, StreamId: streamID, StartOffset: 0,
		},
	})
	require.NoError(t, err)

	appendBody := func(body string) {
		t.Helper()
		_, err := s.add(s.ctx(), t, streamID, &streamlib.AddMessagesInput{Records: streamMsgs("", body)})
		require.NoError(t, err)
	}
	appendBody("a")
	c.awaitKnownHead(id, streamID, 1)
	open := c.poll(id)
	require.Equal(t, []string{"a"}, sliceBodies(t, open.GetStreamSlices()))

	appendBody("b")
	c.awaitKnownHead(id, streamID, 2)
	before := c.stateTransitions(id)
	resp := c.complete(open, true)
	handed := resp.GetWorkflowTask()
	require.NotNil(t, handed, "the completion response carries the next task")
	require.Positive(t, handed.GetStartedEventId())
	require.Equal(t, []string{"b"}, sliceBodies(t, handed.GetStreamSlices()))
	require.Equal(t, before+1, c.stateTransitions(id), "completion, schedule and start in one write")
	require.Equal(t, enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED, c.pendingTaskState(id))

	appendBody("c")
	c.awaitKnownHead(id, streamID, 3)
	resp = c.complete(handed, false)
	require.Nil(t, resp.GetWorkflowTask())
	require.Equal(t, enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED, c.pendingTaskState(id))
	next := c.poll(id)
	require.Equal(t, []string{"c"}, sliceBodies(t, next.GetStreamSlices()))
	c.complete(next, false)
	require.False(t, c.hasPendingTask(id))
}

// A subscribe command whose channel already holds a notification is handed
// the latest, and that rides back with the same completion's response.
func TestInlineHandoffSubscribeGetsLatest(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "handoff-late-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	require.Equal(t, int32(0), c.mustNotify(name, 7), "nobody listens yet")
	c.start(id)

	open := c.poll(id)
	before := c.stateTransitions(id)
	resp := c.complete(open, true, subscribeChannelCommand(name))
	handed := resp.GetWorkflowTask()
	require.NotNil(t, handed)
	requireNotifications(t, c.scheduledNotifications(id, handed), map[string]int64{name: 7})
	require.Equal(t, before+1, c.stateTransitions(id))
	c.complete(handed, false)
	require.False(t, c.hasPendingTask(id))
}
