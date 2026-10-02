package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/encoding/protojson"
)

// ChannelHeader names the channel on a callback post, so an endpoint serving
// several channels can route without parsing the body.
const ChannelHeader = "Temporal-Notification-Channel"

// fanOutConcurrency bounds how many workflow listeners one fan-out tells at
// once. A channel holds up to a thousand, and telling them one at a time makes
// the task's runtime grow with that count.
const fanOutConcurrency = 16

// backgroundCallerContext tags a task's context with the namespace, since the
// task runs outside any request. The name comes back too: a routed call the
// task makes carries it on the request, which is what the interceptors resolve
// the call against.
func backgroundCallerContext(
	ctx context.Context,
	registry namespace.Registry,
	namespaceID string,
) (context.Context, string) {
	name, err := registry.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		return ctx, ""
	}
	return headers.SetCallerInfo(ctx, headers.NewBackgroundLowCallerInfo(name.String())), name.String()
}

// fanOutTaskHandler hands the channel's latest notification to its listeners.
// Callback listeners are handed it inside the channel's own transition; each
// workflow listener is reached on its own shard through the routed service.
type fanOutTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*channelpb.ChannelFanOutTask]

	namespaceRegistry namespace.Registry
	metricsHandler    metrics.Handler
	deliverer         *workflowDeliverer
}

func newFanOutTaskHandler(
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	deliverer *workflowDeliverer,
) *fanOutTaskHandler {
	return &fanOutTaskHandler{
		namespaceRegistry: namespaceRegistry,
		metricsHandler:    metricsHandler,
		deliverer:         deliverer,
	}
}

// Validate accepts the task unconditionally. The notify that scheduled it
// raised a flag so later notifies schedule none of their own, and turning the
// task down would leave the flag up with nothing to lower it.
func (h *fanOutTaskHandler) Validate(
	_ chasm.Context,
	_ *channel.Channel,
	_ chasm.TaskInvocation,
	_ *channelpb.ChannelFanOutTask,
) (bool, error) {
	return true, nil
}

// Execute delivers the latest notification. The first error comes back so the
// task retries and tells the whole set again; a run that already has the
// counter takes the retry as a duplicate and changes nothing.
func (h *fanOutTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *channelpb.ChannelFanOutTask,
) error {
	ctx, ns := backgroundCallerContext(ctx, h.namespaceRegistry, ref.NamespaceID)
	fanOut, _, err := chasm.UpdateComponent(ctx, ref, (*channel.Channel).TakeFanOut, struct{}{})
	if err != nil {
		return err
	}
	if fanOut.CallbackFolded > 0 {
		metrics.ChannelNotificationsFolded.With(h.metricsHandler).Record(
			int64(fanOut.CallbackFolded), metrics.NamespaceTag(ns), independentKindTag, callbackKindTag)
	}
	if fanOut.Latest == nil {
		return nil
	}

	return h.deliverer.deliverAll(ctx, ref, ns, fanOut.WorkflowListeners, fanOut.Latest)
}

// Discard lowers the coalescing flag the scheduling notify raised. Left up, no
// notify would ever schedule another fan-out. A channel that is gone has no
// flag to lower.
func (h *fanOutTaskHandler) Discard(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *channelpb.ChannelFanOutTask,
) error {
	ctx, _ = backgroundCallerContext(ctx, h.namespaceRegistry, ref.NamespaceID)
	_, _, err := chasm.UpdateComponent(ctx, ref,
		func(c *channel.Channel, _ chasm.MutableContext, _ struct{}) (struct{}, error) {
			c.State.FanOutPending = false
			return struct{}{}, nil
		}, struct{}{})
	if executionAbsent(err) {
		return nil
	}
	return err
}

// callbackTaskHandler posts one callback listener's in-flight notification.
// It runs on the outbound queue, so a destination that keeps failing trips the
// same per-destination circuit breaker completion callbacks use, and it goes
// out through the callback library's HTTP caller.
type callbackTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*channelpb.ChannelCallbackTask]

	namespaceRegistry  namespace.Registry
	logger             log.Logger
	metricsHandler     metrics.Handler
	callbackConfig     *callback.Config
	httpCallerProvider callback.HTTPCallerProvider
}

func newCallbackTaskHandler(
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	metricsHandler metrics.Handler,
	callbackConfig *callback.Config,
	httpCallerProvider callback.HTTPCallerProvider,
) *callbackTaskHandler {
	return &callbackTaskHandler{
		namespaceRegistry:  namespaceRegistry,
		logger:             logger,
		metricsHandler:     metricsHandler,
		callbackConfig:     callbackConfig,
		httpCallerProvider: httpCallerProvider,
	}
}

func (h *callbackTaskHandler) Validate(
	ctx chasm.Context,
	c *channel.Channel,
	_ chasm.TaskInvocation,
	task *channelpb.ChannelCallbackTask,
) (bool, error) {
	_, ok := c.CallbackDeliveryFor(ctx, task)
	return ok, nil
}

func (h *callbackTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	attrs chasm.TaskAttributes,
	task *channelpb.ChannelCallbackTask,
) error {
	ctx, ns := backgroundCallerContext(ctx, h.namespaceRegistry, ref.NamespaceID)
	delivery, err := chasm.ReadComponent(ctx, ref,
		func(c *channel.Channel, cctx chasm.Context, t *channelpb.ChannelCallbackTask) (channel.CallbackDelivery, error) {
			d, ok := c.CallbackDeliveryFor(cctx, t)
			if !ok {
				return channel.CallbackDelivery{}, errStaleDelivery
			}
			return d, nil
		}, task)
	if errors.Is(err, errStaleDelivery) {
		return nil
	}
	if err != nil {
		return err
	}

	callCtx, cancel := context.WithTimeout(ctx, h.callbackConfig.RequestTimeout(ns, attrs.Destination))
	defer cancel()
	retryable, postErr := h.post(callCtx, ref, attrs.Destination, delivery)
	if postErr != nil {
		h.logger.Warn("channel callback delivery failed",
			tag.NewStringTag("channel", ref.BusinessID),
			tag.NewStringTag("listener-id", task.GetListenerId()),
			tag.Attempt(delivery.Attempt),
			tag.Bool("retryable", retryable),
			tag.Error(postErr))
	}

	delivered, _, saveErr := chasm.UpdateComponent(ctx, ref, (*channel.Channel).CompleteCallbackDelivery,
		channel.CallbackOutcome{
			ListenerID:  task.GetListenerId(),
			Sequence:    task.GetSequence(),
			Err:         postErr,
			Retryable:   retryable,
			RetryPolicy: h.callbackConfig.RetryPolicy(),
		})
	if saveErr == nil && delivered {
		kind := independentKindTag
		if delivery.Linked {
			kind = linkedKindTag
		}
		metrics.ChannelNotificationsDelivered.With(h.metricsHandler).Record(
			1, metrics.NamespaceTag(ns), kind, callbackKindTag)
	}
	if postErr != nil && retryable {
		// Reported to the queue as the destination being down, which feeds its
		// circuit breaker. The retry itself is the backoff task the outcome
		// scheduled, so a rerun of this task finds its attempt over.
		return queueserrors.NewDestinationDownError(postErr.Error(), saveErr)
	}
	return saveErr
}

var errStaleDelivery = errors.New("channel callback delivery is no longer in flight")

// post sends the notification as JSON, with the callback's own headers and the
// channel named in a header of its own. A 2xx is a delivery. A 4xx other than
// a timeout or a throttle will not succeed on retry, so it is not retried.
func (h *callbackTaskHandler) post(
	ctx context.Context,
	ref chasm.ComponentRef,
	destination string,
	delivery channel.CallbackDelivery,
) (bool, error) {
	body, err := protojson.Marshal(delivery.Notification)
	if err != nil {
		return false, err
	}
	nexus := delivery.Callback.GetNexus()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, nexus.GetUrl(), bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	for key, value := range nexus.GetHeader() {
		request.Header.Set(key, value)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(ChannelHeader, delivery.Notification.GetChannel())

	caller := h.httpCallerProvider(queuescommon.NamespaceIDAndDestination{
		NamespaceID: ref.NamespaceID,
		Destination: destination,
	})
	response, err := caller(request)
	if err != nil {
		return true, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return false, nil
	}
	statusErr := fmt.Errorf("callback endpoint answered %s", response.Status)
	switch {
	case response.StatusCode == http.StatusRequestTimeout,
		response.StatusCode == http.StatusTooManyRequests,
		response.StatusCode >= 500:
		return true, statusErr
	default:
		return false, statusErr
	}
}

func (h *callbackTaskHandler) Discard(
	_ context.Context,
	_ chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *channelpb.ChannelCallbackTask,
) error {
	// Nothing to undo: the task has no side effect until it executes.
	return nil
}

// callbackBackoffTaskHandler resumes a callback delivery once its backoff has
// run out.
type callbackBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func newCallbackBackoffTaskHandler() *callbackBackoffTaskHandler {
	return &callbackBackoffTaskHandler{}
}

func (h *callbackBackoffTaskHandler) Validate(
	ctx chasm.Context,
	c *channel.Channel,
	_ chasm.TaskInvocation,
	task *channelpb.ChannelCallbackBackoffTask,
) (bool, error) {
	return c.BackoffDone(ctx, task), nil
}

func (h *callbackBackoffTaskHandler) Execute(
	mctx chasm.MutableContext,
	c *channel.Channel,
	_ chasm.TaskAttributes,
	task *channelpb.ChannelCallbackBackoffTask,
) error {
	return c.ResumeCallback(mctx, task)
}

// idleTaskHandler deletes a channel that has had no listeners and no activity
// for a full retention.
type idleTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*channelpb.ChannelIdleTask]

	namespaceRegistry namespace.Registry
	config            *channel.Config
}

func newIdleTaskHandler(namespaceRegistry namespace.Registry, config *channel.Config) *idleTaskHandler {
	return &idleTaskHandler{namespaceRegistry: namespaceRegistry, config: config}
}

func (h *idleTaskHandler) Validate(
	_ chasm.Context,
	c *channel.Channel,
	_ chasm.TaskInvocation,
	_ *channelpb.ChannelIdleTask,
) (bool, error) {
	return c.State.GetIdleCheckPending() && !c.State.GetClosed(), nil
}

// Execute closes the channel in one transition and deletes it in the next.
// Closing first means a notify or registration that lands in between starts a
// new channel instead of joining the one about to be deleted, and the delete
// names this run so it cannot take that new one.
func (h *idleTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *channelpb.ChannelIdleTask,
) error {
	ctx, ns := backgroundCallerContext(ctx, h.namespaceRegistry, ref.NamespaceID)
	limits := channel.DefaultLimits()
	if ns != "" {
		limits = h.config.LimitsFor(ns)
	}
	expired, _, err := chasm.UpdateComponent(ctx, ref, (*channel.Channel).RunIdleCheck, limits)
	if err != nil || !expired {
		return err
	}
	key := ref.ExecutionKey
	if key.RunID == "" {
		return serviceerror.NewInternal("idle channel task has no run id to delete")
	}
	err = chasm.DeleteExecution[*channel.Channel](ctx, key, chasm.DeleteExecutionRequest{})
	if executionAbsent(err) {
		return nil
	}
	return err
}

func (h *idleTaskHandler) Discard(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *channelpb.ChannelIdleTask,
) error {
	ctx, _ = backgroundCallerContext(ctx, h.namespaceRegistry, ref.NamespaceID)
	_, _, err := chasm.UpdateComponent(ctx, ref,
		func(c *channel.Channel, _ chasm.MutableContext, _ struct{}) (struct{}, error) {
			c.State.IdleCheckPending = false
			return struct{}{}, nil
		}, struct{}{})
	if executionAbsent(err) {
		return nil
	}
	return err
}
