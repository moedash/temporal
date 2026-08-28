package service

import (
	"context"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
)

// retentionTaskHandler deletes a stream once its retention has elapsed. Close
// only seals; deletion is deliberately later, so a consumer can still drain a
// finished stream without coordinating a shutdown with the producer.
type retentionTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streampb.StreamRetentionTask]

	shardController   shard.Controller
	namespaceRegistry namespace.Registry
	logger            log.Logger
}

func newRetentionTaskHandler(
	shardController shard.Controller,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
) *retentionTaskHandler {
	return &retentionTaskHandler{
		shardController:   shardController,
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
	}
}

func (h *retentionTaskHandler) Validate(
	_ chasm.Context,
	s *stream.Stream,
	_ chasm.TaskInvocation,
	_ *streampb.StreamRetentionTask,
) (bool, error) {
	// A stream that was reopened, or never closed, has nothing to expire. The
	// task is scheduled at close and only meaningful while that still holds.
	return s.State.GetClosed(), nil
}

func (h *retentionTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.StreamRetentionTask,
) error {
	namespaceID := ref.NamespaceID
	streamID := ref.BusinessID

	// Runs outside a request, so nothing has tagged the context yet. Deletions
	// still have to be attributed to the namespace they belong to.
	if name, nsErr := h.namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID)); nsErr == nil {
		ctx = headers.SetCallerInfo(ctx, headers.NewBackgroundLowCallerInfo(name.String()))
	}

	shardCtx, err := h.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(namespaceID), streamID)
	if err != nil {
		return err
	}

	state, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return err
	}

	deleteLogBuckets(ctx, shardCtx, h.logger, namespaceID, state)

	return chasm.DeleteExecution[*stream.Stream](ctx, ref.ExecutionKey, chasm.DeleteExecutionRequest{})
}

// deleteLogBuckets drops every bucket tree a stream still holds.
//
// Log data first, then the execution. The other order would drop the only
// record of which buckets exist: a tree is located by arithmetic from the
// collection id, which lives on the execution and is recorded nowhere else, so
// once the execution is gone nothing can name the trees to delete them.
//
// A bucket that fails to delete is logged and skipped rather than aborting the
// sweep, because the alternative is refusing to delete the stream at all.
// Correctness does not depend on the cleanup, storage does.
func deleteLogBuckets(
	ctx context.Context,
	shardCtx historyi.ShardContext,
	logger log.Logger,
	namespaceID string,
	state *streampb.StreamState,
) {
	lastBucket := stream.BucketOf(max(state.GetHeadOffset()-1, 0), state.GetBucketSize())
	for b := stream.BucketOf(state.GetBaseOffset(), state.GetBucketSize()); b <= lastBucket; b++ {
		if err := stream.DeleteBucket(ctx, shardCtx.GetExecutionManager(), shardCtx.GetShardID(),
			namespaceID, state.GetCollectionId(), b); err != nil {
			logger.Warn("failed to delete a stream bucket, its storage is leaked",
				tag.NewStringTag("collection-id", state.GetCollectionId()),
				tag.NewInt64("bucket", b),
				tag.Error(err))
		}
	}
}

func (h *retentionTaskHandler) Discard(
	_ context.Context,
	_ chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.StreamRetentionTask,
) error {
	// Nothing to undo: the task carries no side effect until it executes.
	return nil
}

// notifyConsumersTaskHandler tells workflows in other executions that the
// stream moved.
//
// Appending never schedules a workflow task by itself, because a stream item is
// data an execution produced rather than a decision input to it. A workflow
// that subscribed is the exception, and it has no other way to find out: it
// cannot read another execution's frontier while closing its own transaction.
// So the frontier is pushed into its cursor, which dirties that execution and
// lets its own transaction close decide it owes a workflow task.
type notifyConsumersTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streampb.StreamNotifyConsumersTask]

	namespaceRegistry namespace.Registry
	logger            log.Logger
}

func newNotifyConsumersTaskHandler(
	namespaceRegistry namespace.Registry,
	logger log.Logger,
) *notifyConsumersTaskHandler {
	return &notifyConsumersTaskHandler{
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
	}
}

func (h *notifyConsumersTaskHandler) Validate(
	_ chasm.Context,
	s *stream.Stream,
	_ chasm.TaskInvocation,
	_ *streampb.StreamNotifyConsumersTask,
) (bool, error) {
	for _, consumer := range s.State.GetConsumers() {
		if consumer.GetExternal() && consumer.GetActive() {
			return true, nil
		}
	}
	return false, nil
}

func (h *notifyConsumersTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.StreamNotifyConsumersTask,
) error {
	namespaceID := ref.NamespaceID
	streamID := ref.BusinessID

	if name, nsErr := h.namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID)); nsErr == nil {
		ctx = headers.SetCallerInfo(ctx, headers.NewBackgroundLowCallerInfo(name.String()))
	}

	state, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return err
	}
	head := state.GetHeadOffset()

	for _, consumer := range state.GetConsumers() {
		if !consumer.GetExternal() || !consumer.GetActive() || consumer.GetOffset() >= head {
			continue
		}

		_, _, err := chasm.UpdateComponent(
			ctx,
			chasm.NewComponentRef[*chasmworkflow.Workflow](chasm.ExecutionKey{
				NamespaceID: namespaceID,
				BusinessID:  consumer.GetWorkflowId(),
			}),
			func(wf *chasmworkflow.Workflow, mctx chasm.MutableContext, at int64) (struct{}, error) {
				return struct{}{}, wf.AdvanceKnownHead(mctx, streamID, at)
			},
			head,
		)
		if err != nil {
			// One unreachable consumer must not hold up the others, and the
			// next append schedules this again. A consumer that never comes
			// back is drained by its own truncation floor, not from here.
			h.logger.Warn("failed to tell a stream consumer that the frontier moved",
				tag.NewStringTag("stream-id", streamID),
				tag.NewStringTag("consumer-workflow-id", consumer.GetWorkflowId()),
				tag.Error(err))
		}
	}
	return nil
}

func (h *notifyConsumersTaskHandler) Discard(
	_ context.Context,
	_ chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.StreamNotifyConsumersTask,
) error {
	return nil
}
