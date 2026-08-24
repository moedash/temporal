package stream

import (
	"context"

	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
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
	s *Stream,
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

	state, err := chasm.ReadComponent(ctx, ref, (*Stream).snapshot, struct{}{})
	if err != nil {
		return err
	}

	// Log data first, then the execution. The other order would drop the only
	// record of which buckets exist and leak them permanently.
	lastBucket := BucketOf(max(state.GetHeadOffset()-1, 0), state.GetBucketSize())
	for b := BucketOf(state.GetBaseOffset(), state.GetBucketSize()); b <= lastBucket; b++ {
		if err := DeleteBucket(ctx, shardCtx.GetExecutionManager(), shardCtx.GetShardID(),
			namespaceID, state.GetCollectionId(), b); err != nil {
			h.logger.Warn("failed to delete a stream bucket during retention cleanup",
				tag.NewStringTag("collection-id", state.GetCollectionId()),
				tag.NewInt64("bucket", b),
				tag.Error(err))
		}
	}

	return chasm.DeleteExecution[*Stream](ctx, ref.ExecutionKey, chasm.DeleteExecutionRequest{})
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
