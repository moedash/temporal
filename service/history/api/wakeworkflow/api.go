package wakeworkflow

import (
	"context"
	"errors"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// errNeedsChainLookup reports that the named run is neither the current run
// nor the first run of its chain, so its chain has to be read from the run.
var errNeedsChainLookup = errors.New("wake run id needs a chain lookup")

// chain says which runs a wake may land on. A wake addressed to a run lands on
// the current run of that run's chain, since the run named may have continued
// as new since the sender learned its id.
type chain struct {
	runID               string
	firstExecutionRunID string
}

func (c chain) admits(ms historyi.MutableState) (bool, error) {
	if c.runID == "" {
		return true, nil
	}
	first := ms.GetExecutionInfo().GetFirstExecutionRunId()
	if ms.GetExecutionState().GetRunId() == c.runID || first == c.runID {
		return true, nil
	}
	if c.firstExecutionRunID == "" {
		return false, errNeedsChainLookup
	}
	return first == c.firstExecutionRunID, nil
}

func Invoke(
	ctx context.Context,
	req *historyservice.WakeWorkflowExecutionRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.WakeWorkflowExecutionResponse, error) {
	request := req.GetWakeRequest()
	workflowID := request.GetWorkflowExecution().GetWorkflowId()
	namespaceEntry, err := api.GetActiveNamespace(
		shardContext, namespace.ID(req.GetNamespaceId()), workflowID)
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID().String()

	target := chain{runID: request.GetWorkflowExecution().GetRunId()}
	resp, err := wake(ctx, req, shardContext, workflowConsistencyChecker, namespaceID, target)
	if errors.Is(err, errNeedsChainLookup) {
		target.firstExecutionRunID, err = firstExecutionRunID(
			ctx, workflowConsistencyChecker, namespaceID, workflowID, target.runID)
		if err != nil {
			return nil, err
		}
		resp, err = wake(ctx, req, shardContext, workflowConsistencyChecker, namespaceID, target)
	}
	if err != nil {
		return nil, err
	}

	counter := metrics.WorkflowWakeAccepted
	if resp.GetFolded() {
		counter = metrics.WorkflowWakeFolded
	}
	counter.With(shardContext.GetMetricsHandler()).Record(
		1, metrics.NamespaceTag(namespaceEntry.Name().String()))
	return resp, nil
}

func wake(
	ctx context.Context,
	req *historyservice.WakeWorkflowExecutionRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	namespaceID string,
	target chain,
) (*historyservice.WakeWorkflowExecutionResponse, error) {
	request := req.GetWakeRequest()
	wakeMsg := request.GetWake()
	resp := &historyservice.WakeWorkflowExecutionResponse{}

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		// Always the current run: a run id names a chain, not a run.
		definition.NewWorkflowKey(namespaceID, request.GetWorkflowExecution().GetWorkflowId(), ""),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			releaseFn := workflowLease.GetReleaseFn()

			admitted, err := target.admits(mutableState)
			if err != nil || !admitted {
				// Nothing was touched, so release with nil to keep the cached
				// mutable state rather than reload it.
				releaseFn(nil)
				if err != nil {
					return nil, err
				}
				return nil, consts.ErrWorkflowExecutionNotFound
			}
			if !mutableState.IsWorkflowExecutionRunning() {
				releaseFn(nil)
				return nil, consts.ErrWorkflowCompleted
			}
			resp.RunId = mutableState.GetExecutionState().GetRunId()

			// Checked read-only so that a duplicate leaves the execution exactly
			// as it was: reaching the component mutably is already a write. A
			// fold that raises the counter still persists the new position.
			if mutableState.HasChasmWorkflowComponent() {
				wf, readCtx, err := mutableState.ChasmWorkflowComponentReadOnly(ctx)
				if err != nil {
					return nil, err
				}
				if wf.WakeIsDuplicate(readCtx, wakeMsg.GetSource(), wakeMsg.GetCounter()) {
					resp.Folded = true
					return &api.UpdateWorkflowAction{Noop: true}, nil
				}
			}

			mutableState.EnsureChasmWorkflowComponent(ctx)
			wf, chasmCtx, err := mutableState.ChasmWorkflowComponent(ctx)
			if err != nil {
				return nil, err
			}
			limit := shardContext.GetConfig().MaximumWakeSourcesPerExecution(
				mutableState.GetNamespaceEntry().Name().String())
			folded, err := wf.AcceptWake(
				chasmCtx,
				wakeMsg.GetSource(),
				wakeMsg.GetPosition(),
				wakeMsg.GetCounter(),
				limit,
			)
			if err != nil {
				return nil, err
			}
			resp.Folded = folded

			// No task is created here. The transaction close schedules one for
			// an undelivered wake, which also covers a wake that lands while a
			// task is running, and holds off while a first task backoff runs.
			return &api.UpdateWorkflowAction{}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// firstExecutionRunID reads the chain a run belongs to from the run itself.
func firstExecutionRunID(
	ctx context.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	namespaceID string,
	workflowID string,
	runID string,
) (string, error) {
	lease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(namespaceID, workflowID, runID),
		locks.PriorityHigh,
	)
	if err != nil {
		return "", err
	}
	defer func() { lease.GetReleaseFn()(nil) }()

	first := lease.GetMutableState().GetExecutionInfo().GetFirstExecutionRunId()
	if first == "" {
		return runID, nil
	}
	return first, nil
}
