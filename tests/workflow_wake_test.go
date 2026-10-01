package tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type wakeTestEnv struct {
	t   *testing.T
	env *testcore.TestEnv
	ns  string
}

func newWakeTestEnv(t *testing.T, opts ...testcore.TestOption) *wakeTestEnv {
	env := testcore.NewEnv(t, opts...)
	return &wakeTestEnv{t: t, env: env, ns: env.Namespace().String()}
}

func (w *wakeTestEnv) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	w.t.Cleanup(cancel)
	return ctx
}

func (w *wakeTestEnv) taskQueue(id string) *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
}

func (w *wakeTestEnv) start(id string) string {
	w.t.Helper()
	resp, err := w.env.FrontendClient().StartWorkflowExecution(w.ctx(),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           w.ns,
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: "wake-consumer"},
			TaskQueue:           w.taskQueue(id),
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            "tester",
		})
	require.NoError(w.t, err)
	return resp.GetRunId()
}

func (w *wakeTestEnv) poll(id string) *workflowservice.PollWorkflowTaskQueueResponse {
	w.t.Helper()
	resp, err := w.env.FrontendClient().PollWorkflowTaskQueue(w.ctx(),
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: w.ns,
			TaskQueue: w.taskQueue(id),
			Identity:  "tester",
		})
	require.NoError(w.t, err)
	require.NotEmpty(w.t, resp.GetTaskToken(), "expected a workflow task")
	return resp
}

func (w *wakeTestEnv) complete(
	task *workflowservice.PollWorkflowTaskQueueResponse,
	returnNewTask bool,
	commands ...*commandpb.Command,
) *workflowservice.RespondWorkflowTaskCompletedResponse {
	w.t.Helper()
	resp, err := w.env.FrontendClient().RespondWorkflowTaskCompleted(w.ctx(),
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace:             w.ns,
			TaskToken:             task.GetTaskToken(),
			Commands:              commands,
			Identity:              "tester",
			ReturnNewWorkflowTask: returnNewTask,
		})
	require.NoError(w.t, err)
	return resp
}

func (w *wakeTestEnv) continueAsNewCommand(id string) *commandpb.Command {
	attrs := &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
		WorkflowType:        &commonpb.WorkflowType{Name: "wake-consumer"},
		TaskQueue:           w.taskQueue(id),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	}
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
			ContinueAsNewWorkflowExecutionCommandAttributes: attrs,
		},
	}
}

func (w *wakeTestEnv) wake(
	id string,
	runID string,
	source string,
	counter int64,
) (*workflowservice.WakeWorkflowExecutionResponse, error) {
	return w.env.FrontendClient().WakeWorkflowExecution(w.ctx(),
		&workflowservice.WakeWorkflowExecutionRequest{
			Namespace:         w.ns,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID},
			Wake: &workflowpb.Wake{
				Source:   source,
				Position: []byte(source + "@" + string(rune('0'+counter))),
				Counter:  counter,
			},
			Identity: "tester",
		})
}

func (w *wakeTestEnv) mustWake(
	id string,
	runID string,
	source string,
	counter int64,
) *workflowservice.WakeWorkflowExecutionResponse {
	w.t.Helper()
	resp, err := w.wake(id, runID, source, counter)
	require.NoError(w.t, err)
	return resp
}

func (w *wakeTestEnv) hasPendingTask(id string) bool {
	w.t.Helper()
	resp, err := w.env.FrontendClient().DescribeWorkflowExecution(w.ctx(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: w.ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: id},
		})
	require.NoError(w.t, err)
	return resp.GetPendingWorkflowTask() != nil
}

func (w *wakeTestEnv) persisted(id string) *adminservice.DescribeMutableStateResponse {
	w.t.Helper()
	resp, err := w.env.AdminClient().DescribeMutableState(w.ctx(),
		&adminservice.DescribeMutableStateRequest{
			Namespace: w.ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: id},
			Archetype: chasm.WorkflowArchetype,
		})
	require.NoError(w.t, err)
	return resp
}

func (w *wakeTestEnv) stateTransitions(id string) int64 {
	w.t.Helper()
	return w.persisted(id).GetDatabaseMutableState().GetExecutionInfo().GetStateTransitionCount()
}

// persistedNextEventID reads the next event id from the database, which a
// speculative Workflow Task does not move, unlike the history the API shows.
func (w *wakeTestEnv) persistedNextEventID(id string) int64 {
	w.t.Helper()
	return w.persisted(id).GetDatabaseMutableState().GetNextEventId()
}

func (w *wakeTestEnv) eventTypes(id string, runID string) []enumspb.EventType {
	events := w.env.GetHistory(w.ns, &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID})
	out := make([]enumspb.EventType, len(events))
	for i, e := range events {
		out[i] = e.GetEventType()
	}
	return out
}

func requireWakes(t *testing.T, got []*workflowpb.Wake, want map[string]int64) {
	t.Helper()
	require.Len(t, got, len(want), "wakes: %v", got)
	for _, wake := range got {
		counter, ok := want[wake.GetSource()]
		require.True(t, ok, "unexpected wake from %q", wake.GetSource())
		require.Equal(t, counter, wake.GetCounter(), "counter of %q", wake.GetSource())
		require.True(t, bytes.HasPrefix(wake.GetPosition(), []byte(wake.GetSource()+"@")))
	}
}

func requireNotFound(t *testing.T, err error) {
	t.Helper()
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
}

// Wakes that arrive before a task starts coalesce into one task carrying the
// highest counter, a pure repeat persists nothing, and History gains only the
// task's own events.
func TestWorkflowWakeCoalescesIntoOneTask(t *testing.T) {
	w := newWakeTestEnv(t)
	id := "wake-coalesce-" + uuid.NewString()
	runID := w.start(id)

	w.complete(w.poll(id), false)
	require.False(t, w.hasPendingTask(id))
	before := len(w.eventTypes(id, runID))

	first := w.mustWake(id, runID, "orders", 1)
	require.False(t, first.GetFolded())
	require.Equal(t, runID, first.GetRunId())
	require.True(t, w.mustWake(id, "", "orders", 2).GetFolded())

	transitions := w.stateTransitions(id)
	repeat := w.mustWake(id, runID, "orders", 2)
	require.True(t, repeat.GetFolded())
	require.Equal(t, transitions, w.stateTransitions(id), "a pure repeat persists nothing")

	task := w.poll(id)
	requireWakes(t, task.GetWakes(), map[string]int64{"orders": 2})
	require.Equal(t,
		[]enumspb.EventType{
			enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		},
		w.eventTypes(id, runID)[before:],
		"History gains only the task's own events")
	w.complete(task, false)
	require.False(t, w.hasPendingTask(id))

	// Asked for its next task inline, the worker gets one carrying the wake.
	w.mustWake(id, runID, "orders", 3)
	task = w.poll(id)
	w.mustWake(id, runID, "orders", 4)
	completed := w.complete(task, true)
	require.NotNil(t, completed.GetWorkflowTask())
	requireWakes(t, completed.GetWorkflowTask().GetWakes(), map[string]int64{"orders": 4})
	w.complete(completed.GetWorkflowTask(), false)
	require.False(t, w.hasPendingTask(id))

	// A failed task drops nothing, so the next attempt carries the wake.
	w.mustWake(id, runID, "orders", 5)
	task = w.poll(id)
	requireWakes(t, task.GetWakes(), map[string]int64{"orders": 5})
	_, err := w.env.FrontendClient().RespondWorkflowTaskFailed(w.ctx(),
		&workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: w.ns,
			TaskToken: task.GetTaskToken(),
			Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
			Identity:  "tester",
		})
	require.NoError(t, err)
	task = w.poll(id)
	require.Equal(t, int32(2), task.GetAttempt())
	requireWakes(t, task.GetWakes(), map[string]int64{"orders": 5})
	w.complete(task, false)

	// A closed workflow takes no wakes.
	w.mustWake(id, runID, "orders", 6)
	w.complete(w.poll(id), false, completeWorkflowCommand()...)
	_, err = w.wake(id, runID, "orders", 7)
	requireNotFound(t, err)
	_, err = w.wake(id, "", "orders", 7)
	requireNotFound(t, err)
}

// The stall: a wake reaches the server before the watcher has read the
// record, the task drains nothing and completes, and the watcher sends the
// same wake again. A completed task deletes what it carried, so the repeat is
// a new wake and runs another task.
func TestWorkflowWakeRepeatAfterCompletionRunsAgain(t *testing.T) {
	w := newWakeTestEnv(t)
	id := "wake-stall-" + uuid.NewString()
	runID := w.start(id)
	w.complete(w.poll(id), false)

	require.False(t, w.mustWake(id, runID, "orders", 1).GetFolded())
	task := w.poll(id)
	requireWakes(t, task.GetWakes(), map[string]int64{"orders": 1})
	w.complete(task, false)
	require.False(t, w.hasPendingTask(id))

	require.False(t, w.mustWake(id, runID, "orders", 1).GetFolded())
	require.True(t, w.hasPendingTask(id))
	requireWakes(t, w.poll(id).GetWakes(), map[string]int64{"orders": 1})
}

// A wake that lands while a task runs is accepted, whatever its counter: only
// the open task knows what it read. The next task carries it, with the
// position that wake sent.
func TestWorkflowWakeDuringATaskIsCarriedByTheNext(t *testing.T) {
	w := newWakeTestEnv(t)
	id := "wake-midtask-" + uuid.NewString()
	runID := w.start(id)
	w.complete(w.poll(id), false)

	w.mustWake(id, runID, "audit", 1)
	task := w.poll(id)
	requireWakes(t, task.GetWakes(), map[string]int64{"audit": 1})
	require.False(t, w.mustWake(id, runID, "orders", 3).GetFolded())
	w.complete(task, false)
	require.True(t, w.hasPendingTask(id))
	task = w.poll(id)
	requireWakes(t, task.GetWakes(), map[string]int64{"orders": 3})
	w.complete(task, false)

	// A lower counter than the one the open task carries.
	w.mustWake(id, runID, "orders", 5)
	task = w.poll(id)
	requireWakes(t, task.GetWakes(), map[string]int64{"orders": 5})
	require.False(t, w.mustWake(id, runID, "orders", 3).GetFolded())
	w.complete(task, false)
	task = w.poll(id)
	requireWakes(t, task.GetWakes(), map[string]int64{"orders": 3})
	require.Equal(t, []byte("orders@3"), task.GetWakes()[0].GetPosition())
}

// A pending wake never blocks a workflow from closing or continuing as new,
// and its entry dies with the run.
func TestWorkflowWakeDoesNotBlockCompletion(t *testing.T) {
	w := newWakeTestEnv(t)

	id := "wake-complete-" + uuid.NewString()
	runID := w.start(id)
	task := w.poll(id)
	w.mustWake(id, runID, "orders", 1)
	w.complete(task, false, completeWorkflowCommand()...)
	require.False(t, w.hasPendingTask(id))
	_, err := w.wake(id, runID, "orders", 2)
	requireNotFound(t, err)

	id = "wake-can-" + uuid.NewString()
	runID = w.start(id)
	task = w.poll(id)
	w.mustWake(id, runID, "orders", 1)
	w.complete(task, false, w.continueAsNewCommand(id))
	task = w.poll(id)
	require.NotEqual(t, runID, task.GetWorkflowExecution().GetRunId())
	require.Empty(t, task.GetWakes(), "entries die with the run; the watcher wakes again")
}

// A paused workflow takes wakes but runs no task for them until it resumes.
func TestWorkflowWakeWhilePaused(t *testing.T) {
	w := newWakeTestEnv(t, testcore.WithDynamicConfig(dynamicconfig.WorkflowPauseEnabled, true))
	id := "wake-paused-" + uuid.NewString()
	runID := w.start(id)
	w.complete(w.poll(id), false)

	_, err := w.env.FrontendClient().PauseWorkflowExecution(w.ctx(),
		&workflowservice.PauseWorkflowExecutionRequest{
			Namespace:  w.ns,
			WorkflowId: id,
			RunId:      runID,
			Identity:   "tester",
			Reason:     "wake test",
			RequestId:  uuid.NewString(),
		})
	require.NoError(t, err)

	require.False(t, w.mustWake(id, runID, "orders", 1).GetFolded())
	require.False(t, w.hasPendingTask(id), "no task while paused")

	_, err = w.env.FrontendClient().UnpauseWorkflowExecution(w.ctx(),
		&workflowservice.UnpauseWorkflowExecutionRequest{
			Namespace:  w.ns,
			WorkflowId: id,
			RunId:      runID,
			Identity:   "tester",
			Reason:     "wake test",
			RequestId:  uuid.NewString(),
		})
	require.NoError(t, err)
	require.True(t, w.hasPendingTask(id), "resuming schedules a task for the wake")
	requireWakes(t, w.poll(id).GetWakes(), map[string]int64{"orders": 1})
}

// A run id names a chain: a wake addressed to a run that continued as new
// lands on the current run, and one addressed to a run of another chain
// under the same workflow id is not found.
func TestWorkflowWakeFollowsTheChain(t *testing.T) {
	w := newWakeTestEnv(t)
	id := "wake-chain-" + uuid.NewString()
	firstRun := w.start(id)

	w.complete(w.poll(id), false, w.continueAsNewCommand(id))

	resp := w.mustWake(id, firstRun, "orders", 1)
	require.False(t, resp.GetFolded())
	secondRun := resp.GetRunId()
	require.NotEqual(t, firstRun, secondRun, "the wake lands on the run that continued")

	task := w.poll(id)
	require.Equal(t, secondRun, task.GetWorkflowExecution().GetRunId())
	requireWakes(t, task.GetWakes(), map[string]int64{"orders": 1})

	// The same run id addressed through the current run also lands.
	require.Equal(t, secondRun, w.mustWake(id, secondRun, "orders", 2).GetRunId())

	// End this chain and start another under the same workflow id. A wake
	// still pending does not hold the completion back: it is a hint, not an
	// event the workflow has to handle.
	w.complete(task, false, completeWorkflowCommand()...)
	require.False(t, w.hasPendingTask(id))
	otherChain := w.start(id)

	_, err := w.wake(id, firstRun, "orders", 3)
	requireNotFound(t, err)
	_, err = w.wake(id, secondRun, "orders", 3)
	requireNotFound(t, err)
	require.Equal(t, otherChain, w.mustWake(id, otherChain, "orders", 3).GetRunId())
}

// Malformed wakes are refused at the frontend.
func TestWorkflowWakeValidation(t *testing.T) {
	w := newWakeTestEnv(t)
	id := "wake-validate-" + uuid.NewString()
	runID := w.start(id)

	send := func(wake *workflowpb.Wake) error {
		_, err := w.env.FrontendClient().WakeWorkflowExecution(w.ctx(),
			&workflowservice.WakeWorkflowExecutionRequest{
				Namespace:         w.ns,
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID},
				Wake:              wake,
			})
		return err
	}
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, send(nil), &invalid)
	require.ErrorAs(t, send(&workflowpb.Wake{Counter: 1}), &invalid)
	require.ErrorAs(t, send(&workflowpb.Wake{Source: "orders"}), &invalid)
	require.ErrorAs(t, send(&workflowpb.Wake{Source: "orders", Counter: -1}), &invalid)
	require.ErrorAs(t,
		send(&workflowpb.Wake{Source: "orders", Counter: 1, Position: make([]byte, 1025)}),
		&invalid)
	require.NoError(t,
		send(&workflowpb.Wake{Source: "orders", Counter: 1, Position: make([]byte, 1024)}))
}

// startSpeculativeUpdate sends an Update to a workflow with no other work,
// which History answers with a speculative Workflow Task, and waits until the
// Update is admitted. The returned channel yields the Update's response.
func (w *wakeTestEnv) startSpeculativeUpdate(
	id string,
	runID string,
	updateID string,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := w.env.FrontendClient().UpdateWorkflowExecution(w.ctx(),
			&workflowservice.UpdateWorkflowExecutionRequest{
				Namespace:         w.ns,
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID},
				Request: &updatepb.Request{
					Meta:  &updatepb.Meta{UpdateId: updateID},
					Input: &updatepb.Input{Name: "wake-update"},
				},
				WaitPolicy: &updatepb.WaitPolicy{
					LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
				},
			})
		done <- err
	}()
	ref := &updatepb.UpdateRef{
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID},
		UpdateId:          updateID,
	}
	await.RequireTruef(w.t, func() bool {
		resp, err := w.env.FrontendClient().PollWorkflowExecutionUpdate(w.ctx(),
			&workflowservice.PollWorkflowExecutionUpdateRequest{
				Namespace: w.ns,
				UpdateRef: ref,
				WaitPolicy: &updatepb.WaitPolicy{
					LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED,
				},
			})
		return err == nil &&
			resp.GetStage() >= enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED
	}, 10*time.Second, 20*time.Millisecond, "update %s was not admitted", updateID)
	return done
}

// A wake against a speculative Workflow Task. Accepting the wake is a write,
// and every write converts a pending speculative task to a normal one, so the
// task's events are written and the task that starts carries the wake along
// with the Update. A speculative task that already started does not carry the
// wake; it is converted the same way, and once it completes a normal task
// carries the wake. Rejecting the Update never drops the wake.
//
// The mutable state is not described while an Update is in flight: that
// reloads the execution and drops the in-memory Update registry.
func TestWorkflowWakeWithASpeculativeTask(t *testing.T) {
	w := newWakeTestEnv(t)
	id := "wake-speculative-" + uuid.NewString()
	runID := w.start(id)
	w.complete(w.poll(id), false)
	taskEvents := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
	}

	// Wake while the speculative task is scheduled but not started.
	before := len(w.eventTypes(id, runID))
	updateDone := w.startSpeculativeUpdate(id, runID, "u1")
	require.False(t, w.mustWake(id, runID, "orders", 1).GetFolded())
	task := w.poll(id)
	require.Len(t, task.GetMessages(), 1, "the task still carries the Update")
	requireWakes(t, task.GetWakes(), map[string]int64{"orders": 1})
	// Completing without answering the Update rejects it.
	w.complete(task, false)
	require.NoError(t, await.Rcv(t, updateDone))
	require.Equal(t, taskEvents, w.eventTypes(id, runID)[before:],
		"the converted task's events are written")
	require.False(t, w.hasPendingTask(id), "the wake was carried and dropped")

	// Wake while the speculative task is started.
	before = len(w.eventTypes(id, runID))
	updateDone = w.startSpeculativeUpdate(id, runID, "u2")
	task = w.poll(id)
	require.Len(t, task.GetMessages(), 1)
	require.Empty(t, task.GetWakes())
	require.False(t, w.mustWake(id, runID, "orders", 2).GetFolded())
	w.complete(task, false)
	require.NoError(t, await.Rcv(t, updateDone))
	require.Equal(t,
		append(taskEvents, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED),
		w.eventTypes(id, runID)[before:],
		"the converted task's events are written, then the task for the wake")
	require.True(t, w.hasPendingTask(id), "a normal task is scheduled for the wake")
	task = w.poll(id)
	require.Empty(t, task.GetMessages())
	requireWakes(t, task.GetWakes(), map[string]int64{"orders": 2})
}
