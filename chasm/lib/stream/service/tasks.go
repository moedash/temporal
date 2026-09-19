package service

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
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

	// Runs outside a request, so nothing has tagged the context yet. Deletions
	// still have to be attributed to the namespace they belong to.
	if name, nsErr := h.namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID)); nsErr == nil {
		ctx = headers.SetCallerInfo(ctx, headers.NewBackgroundLowCallerInfo(name.String()))
	}

	// The payload is component state, so deleting the execution takes it too.
	return chasm.DeleteExecution[*stream.Stream](ctx, ref.ExecutionKey, chasm.DeleteExecutionRequest{})
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

	// This task runs on the stream's shard. Its consumers live wherever their
	// own executions do, so telling them goes back out through the service to
	// be routed rather than resolved here.
	routed streampb.StreamServiceClient
}

func newNotifyConsumersTaskHandler(
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	routed streampb.StreamServiceClient,
) *notifyConsumersTaskHandler {
	return &notifyConsumersTaskHandler{
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
		routed:            routed,
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

	// A write rather than a read, because it also lowers the coalescing flag.
	// Appends that commit after this transition schedule their own task.
	state, _, err := chasm.UpdateComponent(ctx, ref, (*stream.Stream).TakeNotifySnapshot, struct{}{})
	if err != nil {
		return err
	}
	head := state.GetHeadOffset()

	// Collected rather than returned at the first failure, so one unreachable
	// consumer does not stop the others being told in this pass, and returned
	// at the end so the task retries rather than dropping the notification.
	//
	// Dropping it is not survivable for a consumer on another host: its known
	// head would never advance, and delivery clips to it.
	var notifyErrs []error

	for consumerID, consumer := range state.GetConsumers() {
		if !consumer.GetExternal() || !consumer.GetActive() || consumer.GetOffset() >= head {
			continue
		}

		response, err := h.routed.AdvanceConsumerHead(ctx, &streampb.AdvanceConsumerHeadRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &streampb.AdvanceConsumerHeadInput{
				WorkflowId: consumer.GetWorkflowId(),
				OwnerRunId: consumer.GetRunId(),
				StreamId:   streamID,
				HeadOffset: head,
			},
		})
		var gone *serviceerror.NotFound
		out := response.GetFrontendResponse()
		switch {
		case errors.As(err, &gone), err == nil && out.GetConsumerClosed():
			// The run that subscribed is over and nothing carries the
			// subscription on. Its replay floor is holding storage for a
			// recovery that can no longer be asked for, and this probe is the
			// one place that finds out.
			notifyErrs = append(notifyErrs, h.forget(ctx, ref, consumerID))
		case err != nil:
			h.logger.Error("failed to tell a stream consumer that the frontier moved",
				tag.NewStringTag("stream-id", streamID),
				tag.NewStringTag("consumer-workflow-id", consumer.GetWorkflowId()),
				tag.Error(err))
			notifyErrs = append(notifyErrs, err)
		case out.GetSuccessorRunId() != "":
			// A continue-as-new moved the subscription to a new run. The pin
			// follows it, with the floor the successor's cursor began at; the
			// predecessor's ranges are in a closed history that never replays.
			notifyErrs = append(notifyErrs, h.rekey(ctx, ref, consumer, out))
		default:
			// Told, and still there. Nothing to clean up.
		}
	}
	return errors.Join(notifyErrs...)
}

func (h *notifyConsumersTaskHandler) forget(
	ctx context.Context,
	ref chasm.ComponentRef,
	consumerID string,
) error {
	_, _, err := chasm.UpdateComponent(ctx, ref,
		func(s *stream.Stream, mctx chasm.MutableContext, id string) (struct{}, error) {
			s.ForgetConsumer(mctx, id)
			return struct{}{}, nil
		}, consumerID)
	return err
}

func (h *notifyConsumersTaskHandler) rekey(
	ctx context.Context,
	ref chasm.ComponentRef,
	consumer *streampb.ConsumerCursor,
	out *streampb.AdvanceConsumerHeadOutput,
) error {
	registration := stream.ConsumerRegistration{
		ConsumerID: externalConsumerID(consumer.GetWorkflowId(), out.GetSuccessorRunId()),
		WorkflowID: consumer.GetWorkflowId(),
		RunID:      out.GetSuccessorRunId(),
		Offset:     out.GetSuccessorStartOffset(),
		External:   true,
	}
	_, _, err := chasm.UpdateComponent(ctx, ref,
		func(s *stream.Stream, mctx chasm.MutableContext, reg stream.ConsumerRegistration) (struct{}, error) {
			// Registering the successor drops the predecessor's entry, since
			// they share a workflow id and differ in run.
			_, err := s.RegisterConsumer(mctx, reg)
			return struct{}{}, err
		}, registration)
	return err
}

func (h *notifyConsumersTaskHandler) Discard(
	_ context.Context,
	_ chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.StreamNotifyConsumersTask,
) error {
	return nil
}
