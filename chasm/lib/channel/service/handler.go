package service

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

type handler struct {
	channelpb.UnimplementedChannelServiceServer

	namespaceRegistry namespace.Registry
	logger            log.Logger
	metricsHandler    metrics.Handler
	timeSource        clock.TimeSource
	config            *channel.Config
	limiters          *notifyLimiters
}

func newHandler(
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
	config *channel.Config,
) *handler {
	return &handler{
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
		metricsHandler:    metricsHandler,
		timeSource:        timeSource,
		config:            config,
		limiters:          newNotifyLimiters(config),
	}
}

// namespaceName resolves an id for the limits and the metrics tag. The
// interceptors have already refused a namespace that does not exist, so an id
// the registry cannot name resolves to the empty string.
func (h *handler) namespaceName(namespaceID string) string {
	name, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		return ""
	}
	return name.String()
}

func (h *handler) limitsFor(namespaceName string) channel.Limits {
	if namespaceName == "" {
		return channel.DefaultLimits()
	}
	return h.config.LimitsFor(namespaceName)
}

// withCallerInfo attributes the channel's persistence calls to the namespace
// that caused them, so they are rate limited, prioritized and metered as that
// namespace's.
func withCallerInfo(ctx context.Context, namespaceName string) context.Context {
	if namespaceName == "" {
		return ctx
	}
	return headers.SetCallerInfo(ctx, headers.NewCallerInfo(namespaceName, headers.CallerTypeAPI, ""))
}

func channelKey(namespaceID, name string) chasm.ExecutionKey {
	return chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: name}
}

func channelRef(namespaceID, name string) chasm.ComponentRef {
	return chasm.NewComponentRef[*channel.Channel](channelKey(namespaceID, name))
}

func startChannel(mctx chasm.MutableContext, _ struct{}) (*channel.Channel, error) {
	return channel.NewChannel(mctx), nil
}

// errChannelClosed refuses an update to a channel the idle task has closed on
// its way to deleting it, so the caller starts a new one instead.
var errChannelClosed = serviceerror.NewNotFound("channel is closed")

// upsert applies an update to the channel, creating the channel first when
// there is none or the one there is closed.
//
// Creation is a transition of its own rather than an update-with-start: the
// engine applies the update of an update-with-start after it has synced the
// new execution's structure, so listeners and retained notifications added by
// that update would not be persisted.
func upsert[O any](
	ctx context.Context,
	namespaceID, name string,
	update func(*channel.Channel, chasm.MutableContext) (O, error),
	opts ...chasm.TransitionOption,
) (O, error) {
	ref := channelRef(namespaceID, name)
	apply := func(c *channel.Channel, mctx chasm.MutableContext, _ struct{}) (O, error) {
		if c.State.GetClosed() {
			var zero O
			return zero, errChannelClosed
		}
		return update(c, mctx)
	}
	out, _, err := chasm.UpdateComponent(ctx, ref, apply, struct{}{}, opts...)
	if !executionAbsent(err) {
		return out, err
	}
	_, err = chasm.StartExecution(ctx, channelKey(namespaceID, name), startChannel, struct{}{})
	if _, started := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); err != nil && !started {
		var zero O
		return zero, err
	}
	out, _, err = chasm.UpdateComponent(ctx, ref, apply, struct{}{}, opts...)
	return out, err
}

func executionAbsent(err error) bool {
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}

// NotifyChannel accepts a notification, creating the channel if this is the
// first anyone has heard of it. A channel with no listeners still retains the
// notification, so a poller that arrives later can catch up.
func (h *handler) NotifyChannel(
	ctx context.Context,
	req *channelpb.NotifyChannelRequest,
) (*channelpb.NotifyChannelResponse, error) {
	in := req.GetFrontendRequest()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	limits := h.limitsFor(ns)
	if err := channel.CheckNotification(in.GetNotification(), limits.MaxMetadataBytes); err != nil {
		return nil, err
	}
	if err := h.limiters.allowNotify(ns, h.timeSource.Now()); err != nil {
		return nil, err
	}

	// Read first. A notify that would not raise the channel's latest counter
	// leaves the ring alone, but it still reaches every listener: one that
	// holds it pending folds it, and one that does not, because its last
	// task already ran, is woken again.
	repeat, err := h.readRepeat(ctx, req.GetNamespaceId(), in.GetNotification())
	if err != nil {
		return nil, err
	}
	if repeat.repeat {
		if err := h.redeliver(ctx, req.GetNamespaceId(), ns, in.GetNotification(), repeat); err != nil {
			return nil, err
		}
		return &channelpb.NotifyChannelResponse{
			FrontendResponse: &channelpb.NotifyChannelOutput{ListenerCount: int64(repeat.listeners)},
		}, nil
	}
	count, err := h.notify(ctx, req.GetNamespaceId(), in, limits)
	if errors.Is(err, chasm.ErrRequestIDAlreadyUsed) {
		// A retry of a notify that was accepted. Answered with the listeners
		// the channel holds now, without accepting it a second time.
		snapshot, readErr := chasm.ReadComponent(ctx,
			channelRef(req.GetNamespaceId(), in.GetNotification().GetChannel()),
			(*channel.Channel).Describe, struct{}{})
		if readErr != nil {
			return nil, readErr
		}
		return &channelpb.NotifyChannelResponse{
			FrontendResponse: &channelpb.NotifyChannelOutput{ListenerCount: int64(len(snapshot.Listeners))},
		}, nil
	}
	if err != nil {
		return nil, err
	}
	metrics.ChannelNotificationsAccepted.With(h.metricsHandler).Record(
		1, metrics.NamespaceTag(ns))
	return &channelpb.NotifyChannelResponse{
		FrontendResponse: &channelpb.NotifyChannelOutput{
			ListenerCount: int64(count),
		},
	}, nil
}

// repeatRead is what a notify learns from reading the channel first.
type repeatRead struct {
	// The counter is not above the channel's latest, so the ring is left as
	// it is.
	repeat    bool
	listeners int
	// Some callback listener does not hold the notification.
	callbacksNeed bool
}

// readRepeat reads whether the notification would advance the channel. A
// channel that does not exist, or is closed, is not a repeat: the notify
// creates a channel.
func (h *handler) readRepeat(
	ctx context.Context,
	namespaceID string,
	n *channelpb.Notification,
) (repeatRead, error) {
	got, err := chasm.ReadComponent(ctx, channelRef(namespaceID, n.GetChannel()),
		func(c *channel.Channel, cctx chasm.Context, counter int64) (repeatRead, error) {
			if c.State.GetClosed() || c.Advances(counter) {
				return repeatRead{}, nil
			}
			return repeatRead{
				repeat:        true,
				listeners:     c.ListenerCount(),
				callbacksNeed: c.CallbacksNeed(cctx, counter),
			}, nil
		}, n.GetCounter())
	if executionAbsent(err) {
		return repeatRead{}, nil
	}
	return got, err
}

// redeliver hands a notification that does not advance the channel to every
// listener. The channel is written only when a callback listener needs it.
func (h *handler) redeliver(
	ctx context.Context,
	namespaceID, ns string,
	n *channelpb.Notification,
	read repeatRead,
) error {
	ref := channelRef(namespaceID, n.GetChannel())
	if read.callbacksNeed {
		folded, _, err := chasm.UpdateComponent(ctx, ref,
			func(c *channel.Channel, mctx chasm.MutableContext, n *channelpb.Notification) (int, error) {
				_, folded, err := c.HandToCallbacks(mctx, n)
				return folded, err
			}, n)
		if err != nil && !executionAbsent(err) {
			return err
		}
		if folded > 0 {
			metrics.ChannelNotificationsFolded.With(h.metricsHandler).Record(
				int64(folded), metrics.NamespaceTag(ns), callbackKindTag)
		}
	}
	if !read.callbacksNeed && read.listeners > 0 {
		metrics.ChannelNotificationsFolded.With(h.metricsHandler).Record(
			int64(read.listeners), metrics.NamespaceTag(ns), callbackKindTag)
	}
	return nil
}

// notify accepts the notification on the channel, creating it if absent. A
// request id makes a retry of an accepted notify a no-op: the engine records
// it with the update and refuses its reuse.
func (h *handler) notify(
	ctx context.Context,
	namespaceID string,
	in *channelpb.NotifyChannelInput,
	limits channel.Limits,
) (int, error) {
	n := in.GetNotification()
	var opts []chasm.TransitionOption
	if in.GetRequestId() != "" {
		opts = append(opts, chasm.WithRequestID(in.GetRequestId()))
	}
	result, err := upsert(ctx, namespaceID, n.GetChannel(),
		func(c *channel.Channel, mctx chasm.MutableContext) (channel.NotifyResult, error) {
			return c.Notify(mctx, n, limits)
		}, opts...)
	return result.ListenerCount, err
}

// RegisterChannelListener adds a callback listener, creating the channel if it
// is absent. A retry with the same request id gets the same listener back.
func (h *handler) RegisterChannelListener(
	ctx context.Context,
	req *channelpb.RegisterChannelListenerRequest,
) (*channelpb.RegisterChannelListenerResponse, error) {
	in := req.GetFrontendRequest()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	if err := channel.CheckChannelName(in.GetChannel(), 0); err != nil {
		return nil, err
	}
	reg := channel.CallbackRegistration{
		RequestID: in.GetRequestId(),
		Callback:  in.GetCallback(),
		Limits:    h.limitsFor(ns),
	}
	listenerID, err := upsert(ctx, req.GetNamespaceId(), in.GetChannel(),
		func(c *channel.Channel, mctx chasm.MutableContext) (string, error) {
			return c.RegisterCallbackListener(mctx, reg)
		})
	if err != nil {
		return nil, err
	}
	return &channelpb.RegisterChannelListenerResponse{
		FrontendResponse: &channelpb.RegisterChannelListenerOutput{ListenerId: listenerID},
	}, nil
}

// UnregisterChannelListener drops a callback listener. A channel or listener
// that is already gone is not an error, so a retry succeeds.
func (h *handler) UnregisterChannelListener(
	ctx context.Context,
	req *channelpb.UnregisterChannelListenerRequest,
) (*channelpb.UnregisterChannelListenerResponse, error) {
	in := req.GetFrontendRequest()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	limits := h.limitsFor(ns)
	_, _, err := chasm.UpdateComponent(ctx, channelRef(req.GetNamespaceId(), in.GetChannel()),
		func(c *channel.Channel, mctx chasm.MutableContext, id string) (struct{}, error) {
			return struct{}{}, c.UnregisterListener(mctx, id, limits)
		}, in.GetListenerId())
	if err != nil && !executionAbsent(err) {
		return nil, err
	}
	return &channelpb.UnregisterChannelListenerResponse{
		FrontendResponse: &channelpb.UnregisterChannelListenerOutput{},
	}, nil
}

// pollBudget is how long one blocking poll may hold its caller: what it asked
// for, capped at the server's long-poll timeout. It ends before the caller's
// own deadline, so expiry is answered with an empty response.
func pollBudget(ctx context.Context, wait time.Duration) (context.Context, context.CancelFunc) {
	return contextutil.WithDeadlineBuffer(ctx, min(wait, channel.LongPollTimeout), channel.LongPollBuffer)
}

// pollRecheckInterval is how often a blocking poll on a channel nobody has
// created yet asks again whether it exists.
const pollRecheckInterval = time.Second

// PollChannel returns the retained notifications above the caller's counter.
// With wait set and none to return, it blocks until a notify brings one or the
// long-poll budget runs out, and then answers with what there is.
func (h *handler) PollChannel(
	ctx context.Context,
	req *channelpb.PollChannelRequest,
) (*channelpb.PollChannelResponse, error) {
	in := req.GetFrontendRequest()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	metrics.ChannelPollers.With(h.metricsHandler).Record(1, metrics.NamespaceTag(ns))
	ref := channelRef(req.GetNamespaceId(), in.GetChannel())
	limits := h.limitsFor(ns)

	wait := in.GetWait().AsDuration()
	latest, lastActivity, err := h.readLatest(ctx, ref)
	if executionAbsent(err) {
		if wait <= 0 {
			return &channelpb.PollChannelResponse{FrontendResponse: &channelpb.PollChannelOutput{}}, nil
		}
		pollCtx, cancel := pollBudget(ctx, wait)
		defer cancel()
		latest, lastActivity, err = h.waitForChannel(pollCtx, ctx, ref)
		if err != nil || pollCtx.Err() != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return &channelpb.PollChannelResponse{FrontendResponse: &channelpb.PollChannelOutput{}}, nil
		}
	} else if err != nil {
		return nil, err
	}

	if wait > 0 && latest <= in.GetAfterCounter() {
		pollCtx, cancel := pollBudget(ctx, wait)
		defer cancel()
		_, _, err := chasm.PollComponent(pollCtx, ref,
			func(c *channel.Channel, _ chasm.Context, after int64) (struct{}, bool, error) {
				// Monotonic, as PollComponent requires: the latest counter
				// only grows.
				return struct{}{}, c.LatestCounter() > after, nil
			}, in.GetAfterCounter())
		if err != nil && (pollCtx.Err() == nil || ctx.Err() != nil) {
			return nil, err
		}
	}

	notifications, err := chasm.ReadComponent(ctx, ref, (*channel.Channel).Poll, channel.PollRequest{
		AfterCounter: in.GetAfterCounter(),
		Max:          int(in.GetMaxNotifications()),
	})
	if err != nil {
		return nil, err
	}
	h.touchForPoll(ctx, ref, lastActivity, limits)
	return &channelpb.PollChannelResponse{
		FrontendResponse: &channelpb.PollChannelOutput{Notifications: notifications},
	}, nil
}

func (h *handler) readLatest(ctx context.Context, ref chasm.ComponentRef) (int64, time.Time, error) {
	snapshot, err := chasm.ReadComponent(ctx, ref,
		func(c *channel.Channel, _ chasm.Context, _ struct{}) (channel.Snapshot, error) {
			return channel.Snapshot{
				Latest:           c.State.GetLatest(),
				LastActivityTime: c.State.GetLastActivityTime().AsTime(),
			}, nil
		}, struct{}{})
	if err != nil {
		return 0, time.Time{}, err
	}
	return snapshot.Latest.GetCounter(), snapshot.LastActivityTime, nil
}

// waitForChannel parks a blocking poll on a channel nobody has created yet,
// asking again on an interval, so a client can start polling before the first
// notify.
func (h *handler) waitForChannel(
	pollCtx, callerCtx context.Context,
	ref chasm.ComponentRef,
) (int64, time.Time, error) {
	ticker := time.NewTicker(pollRecheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-pollCtx.Done():
			if callerCtx.Err() != nil {
				return 0, time.Time{}, callerCtx.Err()
			}
			return 0, time.Time{}, pollCtx.Err()
		case <-ticker.C:
		}
		latest, lastActivity, err := h.readLatest(pollCtx, ref)
		switch {
		case err == nil:
			return latest, lastActivity, nil
		case executionAbsent(err), pollCtx.Err() != nil:
			continue
		default:
			return 0, time.Time{}, err
		}
	}
}

// touchForPoll records the poll as activity when the last one is old enough to
// matter to the idle check. A failure here costs nothing the caller needs, so
// it is logged rather than returned.
func (h *handler) touchForPoll(
	ctx context.Context,
	ref chasm.ComponentRef,
	lastActivity time.Time,
	limits channel.Limits,
) {
	retention := limits.Retention
	if retention <= 0 {
		retention = channel.DefaultRetention
	}
	if h.timeSource.Now().Sub(lastActivity) < retention/2 {
		return
	}
	_, _, err := chasm.UpdateComponent(ctx, ref, (*channel.Channel).TouchForPoll, limits)
	if err != nil && !executionAbsent(err) {
		h.logger.Warn("failed to record a channel poll as activity")
	}
}

// DescribeChannel lists the listeners and reports the latest notification and
// how many are retained.
func (h *handler) DescribeChannel(
	ctx context.Context,
	req *channelpb.DescribeChannelRequest,
) (*channelpb.DescribeChannelResponse, error) {
	in := req.GetFrontendRequest()
	ctx = withCallerInfo(ctx, h.namespaceName(req.GetNamespaceId()))
	snapshot, err := chasm.ReadComponent(ctx, channelRef(req.GetNamespaceId(), in.GetChannel()),
		(*channel.Channel).Describe, struct{}{})
	if err != nil {
		return nil, err
	}
	return &channelpb.DescribeChannelResponse{
		FrontendResponse: &channelpb.DescribeChannelOutput{
			Listeners:     snapshot.Listeners,
			Latest:        snapshot.Latest,
			RetainedCount: snapshot.RetainedCount,
		},
	}, nil
}

var callbackKindTag = metrics.StringTag("listener_kind", channel.ListenerKindCallback)

// listenerKindTag derives the kind tag from the listener's callback variant.
func listenerKindTag(cb *commonpb.Callback) metrics.Tag {
	return metrics.StringTag("listener_kind", channel.ListenerKind(cb))
}
