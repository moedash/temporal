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
	"golang.org/x/sync/errgroup"
)

// consumerNotifier pushes a stream's frontier at its workflow consumers and
// keeps the consumer table honest about which of them still exist.
//
// A consumer lives wherever its own execution does, so telling it goes back
// out through the service to be routed rather than resolved on this shard. The
// answer also says when the run that subscribed is over: the pin is then
// dropped, or moved to the run that continued it.
type consumerNotifier struct {
	logger log.Logger
	routed streampb.StreamServiceClient
}

// notifyConcurrency bounds how many consumers are told at once. A stream may
// hold up to MaxConsumersPerStream of them, and telling them one at a time
// makes one task's runtime grow with the subscriber count, while telling them
// all at once puts that many routed calls on the wire from one task.
const notifyConcurrency = 16

// notify tells every active external consumer behind head that it moved. With
// probeAll it also tells the caught-up ones, which is how a closed stream
// learns whether anyone still holds it.
//
// The group carries no cancelling context on purpose: one unreachable consumer
// must not stop the others being told. The first error is what comes back, so
// the task retries and tells the whole set again.
func (n *consumerNotifier) notify(
	ctx context.Context,
	ref chasm.ComponentRef,
	namespaceName string,
	state *streampb.StreamState,
	probeAll bool,
) error {
	head := state.GetHeadOffset()
	var group errgroup.Group
	group.SetLimit(notifyConcurrency)
	for consumerID, consumer := range state.GetConsumers() {
		if !consumer.GetExternal() || !consumer.GetActive() {
			continue
		}
		if !probeAll && consumer.GetOffset() >= head {
			continue
		}
		group.Go(func() error {
			return n.notifyOne(ctx, ref, namespaceName, consumerID, consumer, head)
		})
	}
	return group.Wait()
}

// notifyOne tells one consumer, and acts on what it answers.
//
// The call carries its own deadline. The request's deadline belongs to the
// whole task, and a consumer on a slow host would otherwise take all of it and
// leave the rest untold.
func (n *consumerNotifier) notifyOne(
	ctx context.Context,
	ref chasm.ComponentRef,
	namespaceName string,
	consumerID string,
	consumer *streampb.ConsumerCursor,
	head int64,
) error {
	callCtx, cancel := context.WithTimeout(ctx, stream.RoutedCallTimeout)
	defer cancel()

	response, err := n.routed.AdvanceConsumerHead(callCtx, &streampb.AdvanceConsumerHeadRequest{
		NamespaceId: ref.NamespaceID,
		FrontendRequest: &streampb.AdvanceConsumerHeadInput{
			// Set, because the routed request's GetNamespace() is what rate
			// limiting, validation, authorization and redirection all resolve
			// the call against. Left empty they resolve against no namespace.
			Namespace:  namespaceName,
			WorkflowId: consumer.GetWorkflowId(),
			OwnerRunId: consumer.GetRunId(),
			StreamId:   ref.BusinessID,
			HeadOffset: head,
		},
	})
	out := response.GetFrontendResponse()
	switch {
	case err != nil:
		// A NotFound is not taken as proof the consumer is gone. The handler
		// already reads a missing execution as closed and says so in its
		// answer, so one arriving here is the transport's: a namespace id the
		// far host cannot resolve, or a shard that has moved. Releasing a
		// durability guarantee on that would be a guess.
		n.logger.Error("failed to tell a stream consumer that the frontier moved",
			tag.NewStringTag("stream-id", ref.BusinessID),
			tag.NewStringTag("consumer-workflow-id", consumer.GetWorkflowId()),
			tag.Error(err))
		return err
	case out.GetConsumerClosed():
		// The run that subscribed is over and nothing carries the
		// subscription on. Its replay floor is holding storage for a
		// recovery that can no longer be asked for, and this probe is the
		// one place that finds out.
		return forgetConsumer(ctx, ref, consumerID)
	case out.GetSuccessorRunId() != "":
		// A continue-as-new moved the subscription to a new run. The pin
		// follows it, with the floor the successor's cursor began at; the
		// predecessor's ranges are in a closed history that never replays.
		return rekeyConsumer(ctx, ref, consumer, out)
	default:
		// Told, and still there. Nothing to clean up.
		return nil
	}
}

func forgetConsumer(ctx context.Context, ref chasm.ComponentRef, consumerID string) error {
	_, _, err := chasm.UpdateComponent(ctx, ref,
		func(s *stream.Stream, mctx chasm.MutableContext, id string) (struct{}, error) {
			s.ForgetConsumer(mctx, id)
			return struct{}{}, nil
		}, consumerID)
	return err
}

func rekeyConsumer(
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
		func(
			s *stream.Stream, mctx chasm.MutableContext, reg stream.ConsumerRegistration,
		) (struct{}, error) {
			// Registering the successor drops the predecessor's entry, since
			// they share a workflow id and differ in run.
			_, err := s.RegisterConsumer(mctx, reg)
			return struct{}{}, err
		}, registration)
	return err
}

// backgroundCallerContext tags a task's context with the namespace, since the
// task runs outside any request and nothing else has done it. The name comes
// back too: a routed call the task makes has to carry it on the request, which
// is what the interceptors resolve the call against.
func backgroundCallerContext(
	ctx context.Context,
	registry namespace.Registry,
	namespaceID string,
) (context.Context, string) {
	name, err := registry.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		return ctx, ""
	}
	return headers.SetCallerInfo(
		ctx, headers.NewBackgroundLowCallerInfo(name.String())), name.String()
}

// retentionTaskHandler deletes a stream once its retention has elapsed. Close
// only seals; deletion is deliberately later, so a consumer can still drain a
// finished stream without coordinating a shutdown with the producer.
type retentionTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*streampb.StreamRetentionTask]

	shardController   shard.Controller
	namespaceRegistry namespace.Registry
	logger            log.Logger
	config            *stream.Config
	notifier          consumerNotifier
}

func newRetentionTaskHandler(
	shardController shard.Controller,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	config *stream.Config,
	routed streampb.StreamServiceClient,
) *retentionTaskHandler {
	return &retentionTaskHandler{
		shardController:   shardController,
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
		config:            config,
		notifier:          consumerNotifier{logger: logger, routed: routed},
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

// Execute deletes the stream unless a workflow still consumes it.
//
// A consumer's History depends on the ranges it consumed, and deleting them
// turns its next replay into a failure. So while any consumer is active the
// deletion waits and asks again later. A closed stream gets no appends, and
// appends are what otherwise make the stream discover that a consumer is
// gone, so the probe here is what lets a held stream ever be released.
func (h *retentionTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.StreamRetentionTask,
) error {
	ctx, namespaceName := backgroundCallerContext(ctx, h.namespaceRegistry, ref.NamespaceID)

	state, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return err
	}
	if err := h.notifier.notify(ctx, ref, namespaceName, state, true); err != nil {
		return err
	}

	held, _, err := chasm.UpdateComponent(ctx, ref,
		func(s *stream.Stream, mctx chasm.MutableContext, _ struct{}) (bool, error) {
			for _, consumer := range s.State.GetConsumers() {
				if consumer.GetActive() {
					mctx.AddTask(s, chasm.TaskAttributes{
						ScheduledTime: mctx.Now(s).Add(h.config.RetentionRecheckInterval()),
					}, &streampb.StreamRetentionTask{})
					return true, nil
				}
			}
			return false, nil
		}, struct{}{})
	if err != nil || held {
		return err
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
	notifier          consumerNotifier
}

func newNotifyConsumersTaskHandler(
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	routed streampb.StreamServiceClient,
) *notifyConsumersTaskHandler {
	return &notifyConsumersTaskHandler{
		namespaceRegistry: namespaceRegistry,
		notifier:          consumerNotifier{logger: logger, routed: routed},
	}
}

// Validate accepts the task unconditionally.
//
// The scheduling append already decided there was someone to tell, and it
// raised a flag saying a task is outstanding so that later appends schedule
// none of their own. Turning the task down here would leave that flag up with
// nothing left to lower it, and no append would ever schedule another: a
// consumer that subscribed afterwards would wait forever. Execute lowers the
// flag first, so finding nothing to do there costs one state write and leaves
// the stream ready to schedule again.
func (h *notifyConsumersTaskHandler) Validate(
	_ chasm.Context,
	_ *stream.Stream,
	_ chasm.TaskInvocation,
	_ *streampb.StreamNotifyConsumersTask,
) (bool, error) {
	return true, nil
}

func (h *notifyConsumersTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.StreamNotifyConsumersTask,
) error {
	ctx, namespaceName := backgroundCallerContext(ctx, h.namespaceRegistry, ref.NamespaceID)

	// A write rather than a read, because it also lowers the coalescing flag.
	// Appends that commit after this transition schedule their own task.
	state, _, err := chasm.UpdateComponent(ctx, ref, (*stream.Stream).TakeNotifySnapshot, struct{}{})
	if err != nil {
		return err
	}
	return h.notifier.notify(ctx, ref, namespaceName, state, false)
}

// Discard lowers the coalescing flag the scheduling append raised, for whatever
// reason the task is being dropped. Left up, the flag means no append ever
// schedules another notify task and a consumer subscribing later is never
// woken. A stream that is gone has no flag to lower.
func (h *notifyConsumersTaskHandler) Discard(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *streampb.StreamNotifyConsumersTask,
) error {
	ctx, _ = backgroundCallerContext(ctx, h.namespaceRegistry, ref.NamespaceID)
	_, _, err := chasm.UpdateComponent(ctx, ref, (*stream.Stream).TakeNotifySnapshot, struct{}{})
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	return err
}
