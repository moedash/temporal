package recordworkflowtaskstarted

import (
	"context"

	workflowpb "go.temporal.io/api/workflow/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/metrics"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// TakeWakes hands a starting Workflow Task the wakes pending on its workflow
// and records that it carries them. Called while the start's transaction is
// open, so the record commits with the start.
//
// A speculative task gets none: its start is not persisted, so nothing would
// say the wakes went out, and a discarded speculative task completes nothing
// that could settle them. A workflow with a wake waiting gets a normal task.
func TakeWakes(
	ctx context.Context,
	shardContext historyi.ShardContext,
	ms historyi.MutableState,
	workflowTask *historyi.WorkflowTaskInfo,
) ([]*workflowpb.Wake, error) {
	if workflowTask == nil || workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		return nil, nil
	}
	if !ms.HasChasmWorkflowComponent() {
		return nil, nil
	}
	// Read-only first, so a workflow that has never been woken does not pay a
	// node write on every task it starts.
	readOnly, _, err := ms.ChasmWorkflowComponentReadOnly(ctx)
	if err != nil {
		return nil, err
	}
	if !readOnly.HasPendingWakes() {
		return nil, nil
	}

	wf, chasmCtx, err := ms.ChasmWorkflowComponent(ctx)
	if err != nil {
		return nil, err
	}
	wakes := wf.TakeWakesForTask(chasmCtx)
	metrics.WorkflowWakeDelivered.With(shardContext.GetMetricsHandler()).Record(
		int64(len(wakes)), metrics.NamespaceTag(ms.GetNamespaceEntry().Name().String()))
	return wakes, nil
}

// pendingWakes returns what TakeWakes would hand out without recording it,
// for a repeated start request that persists nothing.
func pendingWakes(
	ctx context.Context,
	ms historyi.MutableState,
	workflowTask *historyi.WorkflowTaskInfo,
) ([]*workflowpb.Wake, error) {
	if workflowTask == nil || workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		return nil, nil
	}
	if !ms.HasChasmWorkflowComponent() {
		return nil, nil
	}
	wf, readCtx, err := ms.ChasmWorkflowComponentReadOnly(ctx)
	if err != nil {
		return nil, err
	}
	return wf.PendingWakes(readCtx), nil
}
