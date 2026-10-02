package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/await"
)

// A workflow ends its subscription to a notification channel with a command.
// These cases drive it with the raw client, as the subscribe cases do.

func unsubscribeChannelCommand(name string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_UNSUBSCRIBE_NOTIFICATION_CHANNEL,
		Attributes: &commandpb.Command_UnsubscribeNotificationChannelCommandAttributes{
			UnsubscribeNotificationChannelCommandAttributes: &commandpb.
				UnsubscribeNotificationChannelCommandAttributes{Channel: name},
		},
	}
}

// nudge signals the workflow, which is how a test gets a Workflow Task to
// carry a command when nothing else would schedule one.
func (c *channelTestEnv) nudge(id string) {
	c.t.Helper()
	_, err := c.env.FrontendClient().SignalWorkflowExecution(c.ctx(),
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         c.ns,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id},
			SignalName:        "nudge",
			Identity:          "tester",
			RequestId:         uuid.NewString(),
		})
	require.NoError(c.t, err)
}

// unsubscribedEvents are the run's unsubscribe events, in order.
func (c *channelTestEnv) unsubscribedEvents(
	id, runID string,
) []*historypb.WorkflowNotificationChannelUnsubscribedEventAttributes {
	c.t.Helper()
	execution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID}
	unsubscribed := enumspb.EVENT_TYPE_WORKFLOW_NOTIFICATION_CHANNEL_UNSUBSCRIBED
	var out []*historypb.WorkflowNotificationChannelUnsubscribedEventAttributes
	for _, e := range c.eventsOfType(execution, unsubscribed) {
		out = append(out, e.GetWorkflowNotificationChannelUnsubscribedEventAttributes())
	}
	return out
}

// An unsubscribe records its event naming the subscribe event, drops the run
// from the channel and from describe, and a notify afterwards wakes nothing.
// Subscribing again in the same run starts over with a new event and the
// channel's latest.
func TestUnsubscribeChannelEndsTheSubscription(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "unsubscribe-basic-" + uuid.NewString()
	name := "orders-" + uuid.NewString()
	runID := c.subscribe(id, name)
	subscribedAt := c.subscribedEventID(id, runID, name)

	c.mustNotify(name, 1)
	task := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 1})
	c.complete(task, false, unsubscribeChannelCommand(name))

	events := c.unsubscribedEvents(id, runID)
	require.Len(t, events, 1)
	require.Equal(t, name, events[0].GetChannel())
	require.Equal(t, subscribedAt, events[0].GetSubscribedEventId())
	require.Positive(t, events[0].GetWorkflowTaskCompletedEventId())
	require.Empty(t, c.describe(name).GetListeners(), "the channel forgot the run")
	require.Empty(t, c.channelSubscriptions(id, ""), "describe drops the entry")

	require.Equal(t, int32(0), c.mustNotify(name, 2), "nobody listens")
	require.False(t, c.hasPendingTask(id), "a notify after the unsubscribe wakes nothing")
	require.Equal(t, int32(2), c.describe(name).GetRetainedCount(), "the channel retains it")

	// Subscribing again records a new event and is handed the latest.
	c.nudge(id)
	task = c.poll(id)
	c.complete(task, false, subscribeChannelCommand(name))
	require.Equal(t, runID, c.workflowListenerRun(name, id), "the run is a listener again")
	info := c.channelSubscription(id, name, kindIndependent)
	require.Greater(t, info.GetSubscribedEventId(), subscribedAt, "a new subscribe event")
	task = c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 2})
	c.complete(task, false)
	c.mustNotify(name, 3)
	task = c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{name: 3})
	c.complete(task, false)
}

// An unsubscribe for a channel the run does not listen to, or for a linked
// channel, records its event with no subscribe event and changes nothing
// else. Subscribe and unsubscribe in one task leave no listener behind, and
// the reverse keeps it.
func TestUnsubscribeChannelWithoutSubscription(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "unsubscribe-none-" + uuid.NewString()
	unknown := "unknown-" + uuid.NewString()
	linked := "linked-" + uuid.NewString()
	both := "both-" + uuid.NewString()
	runID := c.startIdle(id)
	c.mustNotifyLinked(id, linked, 1)
	task := c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{linked: 1})
	c.complete(task, false,
		unsubscribeChannelCommand(unknown),
		unsubscribeChannelCommand(linked),
		subscribeChannelCommand(both),
		unsubscribeChannelCommand(both),
	)

	events := c.unsubscribedEvents(id, runID)
	require.Len(t, events, 3)
	require.Equal(t, unknown, events[0].GetChannel())
	require.Zero(t, events[0].GetSubscribedEventId())
	require.Equal(t, linked, events[1].GetChannel())
	require.Zero(t, events[1].GetSubscribedEventId(), "the linked kind has no subscription")
	require.Equal(t, both, events[2].GetChannel())
	require.Equal(t, c.subscribedEventID(id, runID, both), events[2].GetSubscribedEventId())

	_, err := c.env.FrontendClient().DescribeChannel(c.ctx(), &workflowservice.DescribeChannelRequest{
		Namespace: c.ns, Channel: unknown,
	})
	requireNotFound(t, err)
	_, err = c.env.FrontendClient().DescribeChannel(c.ctx(), &workflowservice.DescribeChannelRequest{
		Namespace: c.ns, Channel: both,
	})
	requireNotFound(t, err)
	infos := c.channelSubscriptions(id, "")
	require.Len(t, infos, 1, "only the linked channel is listed")
	require.Equal(t, linked, infos[0].GetChannel())
	require.False(t, c.hasPendingTask(id))

	// The linked channel is untouched by the command naming it.
	c.mustNotifyLinked(id, linked, 2)
	task = c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{linked: 2})

	// Unsubscribe and subscribe again in one task: the listener stays, under
	// the new event.
	again := "again-" + uuid.NewString()
	c.complete(task, false, subscribeChannelCommand(again))
	first := c.subscribedEventID(id, runID, again)
	c.nudge(id)
	task = c.poll(id)
	c.complete(task, false, unsubscribeChannelCommand(again), subscribeChannelCommand(again))
	require.Equal(t, runID, c.workflowListenerRun(again, id))
	info := c.channelSubscription(id, again, kindIndependent)
	require.Greater(t, info.GetSubscribedEventId(), first)
	c.mustNotify(again, 1)
	task = c.poll(id)
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{again: 1})
	c.complete(task, false)
}

// An empty channel name fails the task with the unsubscribe cause.
func TestUnsubscribeChannelValidation(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "unsubscribe-invalid-" + uuid.NewString()
	c.startIdle(id)
	c.nudge(id)
	task := c.poll(id)
	_, err := c.env.FrontendClient().RespondWorkflowTaskCompleted(c.ctx(),
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: c.ns,
			TaskToken: task.GetTaskToken(),
			Commands:  []*commandpb.Command{unsubscribeChannelCommand("")},
			Identity:  "tester",
		})
	requireInvalidArgument(t, err)
	failed := c.eventsOfType(task.GetWorkflowExecution(), enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED)
	require.Len(t, failed, 1)
	require.Equal(t,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UNSUBSCRIBE_NOTIFICATION_CHANNEL_ATTRIBUTES,
		failed[0].GetWorkflowTaskFailedEventAttributes().GetCause())
	retry := c.poll(id)
	require.Equal(t, int32(2), retry.GetAttempt())
	c.complete(retry, false)
	require.Empty(t, c.unsubscribedEvents(id, task.GetWorkflowExecution().GetRunId()))
}

// A reset run is rebuilt from History, subscribe and unsubscribe events in
// order, so it ends with the subscriptions the source had at the reset point.
// The next notify on the dropped channel finds no run that listens and the
// channel forgets the listener, while the kept channel reaches the reset run.
func TestUnsubscribeChannelSurvivesReset(t *testing.T) {
	c := newChannelTestEnv(t)
	id := "unsubscribe-reset-" + uuid.NewString()
	dropped := "dropped-" + uuid.NewString()
	kept := "kept-" + uuid.NewString()
	base := c.subscribe(id, dropped, kept)

	c.mustNotify(dropped, 1)
	task := c.poll(id)
	c.complete(task, false, unsubscribeChannelCommand(dropped))
	c.nudge(id)
	task = c.poll(id)
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

	infos := c.channelSubscriptions(id, resetRun)
	require.Len(t, infos, 1)
	require.Equal(t, kept, infos[0].GetChannel())
	require.Equal(t, c.subscribedEventID(id, base, kept), infos[0].GetSubscribedEventId())

	c.mustNotify(dropped, 2)
	await.Require(c.ctx(), t, func(t *await.T) {
		require.Empty(t, c.describe(dropped).GetListeners(), "the channel forgets the reset-away run")
	}, 20*time.Second, 50*time.Millisecond)
	require.False(t, c.hasPendingTask(id), "the reset run is not woken for a channel it left")

	c.mustNotify(kept, 1)
	task = c.poll(id)
	require.Equal(t, resetRun, task.GetWorkflowExecution().GetRunId())
	requireNotifications(t, c.scheduledNotifications(id, task), map[string]int64{kept: 1})
	c.complete(task, false)
}
