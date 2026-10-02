package service

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
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

func workflowRef(namespaceID, workflowID, runID string) chasm.ComponentRef {
	return chasm.NewComponentRef[*chasmworkflow.Workflow](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  workflowID,
		RunID:       runID,
	})
}

func startChannel[I any](mctx chasm.MutableContext, _ I) (*channel.Channel, error) {
	return channel.NewChannel(mctx), nil
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

	result, err := chasm.UpdateWithStartExecution(
		ctx,
		channelKey(req.GetNamespaceId(), in.GetNotification().GetChannel()),
		startChannel[*channelpb.Notification],
		func(c *channel.Channel, mctx chasm.MutableContext, n *channelpb.Notification) (channel.NotifyResult, error) {
			return c.Notify(mctx, n, limits)
		},
		in.GetNotification(),
	)
	if err != nil {
		return nil, err
	}
	metrics.ChannelNotificationsAccepted.With(h.metricsHandler).Record(1, metrics.NamespaceTag(ns))
	return &channelpb.NotifyChannelResponse{
		FrontendResponse: &channelpb.NotifyChannelOutput{
			ListenerCount: int64(result.UpdateOutput.ListenerCount),
		},
	}, nil
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
	result, err := chasm.UpdateWithStartExecution(
		ctx,
		channelKey(req.GetNamespaceId(), in.GetChannel()),
		startChannel[channel.CallbackRegistration],
		(*channel.Channel).RegisterCallbackListener,
		reg,
	)
	if err != nil {
		return nil, err
	}
	return &channelpb.RegisterChannelListenerResponse{
		FrontendResponse: &channelpb.RegisterChannelListenerOutput{ListenerId: result.UpdateOutput},
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

// pollBudget is how long one blocking poll may hold its caller. It ends before
// the caller's own deadline, so expiry is answered with an empty response.
func pollBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	return contextutil.WithDeadlineBuffer(ctx, channel.LongPollTimeout, channel.LongPollBuffer)
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

	latest, lastActivity, err := h.readLatest(ctx, ref)
	if executionAbsent(err) {
		if !in.GetWait() {
			return &channelpb.PollChannelResponse{FrontendResponse: &channelpb.PollChannelOutput{}}, nil
		}
		pollCtx, cancel := pollBudget(ctx)
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

	if in.GetWait() && latest <= in.GetAfterCounter() {
		pollCtx, cancel := pollBudget(ctx)
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

// RegisterWorkflowListener records a workflow run as a listener on the
// channel's shard, creating the channel if it is absent.
//
// Internal. Called by the completion of the Workflow Task that subscribed,
// which runs on the workflow's shard and cannot reach the channel itself.
func (h *handler) RegisterWorkflowListener(
	ctx context.Context,
	req *channelpb.RegisterWorkflowListenerRequest,
) (*channelpb.RegisterWorkflowListenerResponse, error) {
	in := req.GetFrontendRequest()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	if err := channel.CheckChannelName(in.GetChannel(), 0); err != nil {
		return nil, err
	}
	reg := channel.WorkflowRegistration{
		WorkflowID:          in.GetWorkflowId(),
		RunID:               in.GetRunId(),
		FirstExecutionRunID: in.GetFirstExecutionRunId(),
		Limits:              h.limitsFor(ns),
	}
	_, err := chasm.UpdateWithStartExecution(
		ctx,
		channelKey(req.GetNamespaceId(), in.GetChannel()),
		startChannel[channel.WorkflowRegistration],
		func(c *channel.Channel, mctx chasm.MutableContext, r channel.WorkflowRegistration) (struct{}, error) {
			return struct{}{}, c.RegisterWorkflowListener(mctx, r)
		},
		reg,
	)
	if err != nil {
		return nil, err
	}
	return &channelpb.RegisterWorkflowListenerResponse{
		FrontendResponse: &channelpb.RegisterWorkflowListenerOutput{},
	}, nil
}

// listenerProbe is what one run says about a channel: whether it is still
// open, whether it subscribed, and whether it already has this counter.
type listenerProbe struct {
	runID      string
	closed     bool
	subscribed bool
	duplicate  bool
}

// probeListener reads a run without touching it. A run that is gone, or that
// never had a Workflow component and so cannot have subscribed, reads as
// closed: the fan-out only needs to know whether delivering can achieve
// anything.
func (h *handler) probeListener(
	ctx context.Context,
	namespaceID, workflowID, runID string,
	n *channelpb.Notification,
) (listenerProbe, error) {
	probe, err := chasm.ReadComponent(ctx, workflowRef(namespaceID, workflowID, runID),
		func(wf *chasmworkflow.Workflow, cctx chasm.Context, _ struct{}) (listenerProbe, error) {
			p := listenerProbe{
				runID:      cctx.ExecutionKey().RunID,
				closed:     !cctx.ExecutionInfo().CloseTime.IsZero(),
				subscribed: wf.SubscribedToChannel(n.GetChannel()),
			}
			if p.subscribed {
				p.duplicate = wf.ChannelNotificationIsDuplicate(cctx, n.GetChannel(), n.GetCounter())
			}
			return p, nil
		}, struct{}{})
	if executionAbsent(err) {
		return listenerProbe{runID: runID, closed: true}, nil
	}
	return probe, err
}

func (h *handler) accept(
	ctx context.Context,
	namespaceID, workflowID, runID string,
	n *channelpb.Notification,
) (bool, error) {
	folded, _, err := chasm.UpdateComponent(ctx, workflowRef(namespaceID, workflowID, runID),
		(*chasmworkflow.Workflow).AcceptChannelNotification, n)
	return folded, err
}

// DeliverChannelNotification hands a notification to one workflow listener,
// on the shard that owns it.
//
// Internal. Called by the channel's fan-out, which runs on the channel's shard.
// The channel has a run on record, and a run ends: if it has, the current run
// is asked instead, and the answer tells the channel to re-key the listener to
// it or, when no run listens, to drop it.
func (h *handler) DeliverChannelNotification(
	ctx context.Context,
	req *channelpb.DeliverChannelNotificationRequest,
) (*channelpb.DeliverChannelNotificationResponse, error) {
	in := req.GetFrontendRequest()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	namespaceID, workflowID, n := req.GetNamespaceId(), in.GetWorkflowId(), in.GetNotification()
	if n.GetChannel() == "" || n.GetCounter() <= 0 {
		return nil, serviceerror.NewInvalidArgument("notification needs a channel and a counter")
	}

	out := &channelpb.DeliverChannelNotificationOutput{}
	target, err := h.probeListener(ctx, namespaceID, workflowID, in.GetRunId(), n)
	if err != nil {
		return nil, err
	}
	if target.closed {
		pinned := target.runID
		if target, err = h.probeListener(ctx, namespaceID, workflowID, "", n); err != nil {
			return nil, err
		}
		if target.closed || target.runID == pinned || !target.subscribed {
			out.ListenerClosed = true
			return &channelpb.DeliverChannelNotificationResponse{FrontendResponse: out}, nil
		}
		out.SuccessorRunId = target.runID
	} else if !target.subscribed {
		out.ListenerClosed = true
		return &channelpb.DeliverChannelNotificationResponse{FrontendResponse: out}, nil
	}

	// A duplicate leaves the run exactly as it was: reaching the component
	// mutably is already a write.
	if target.duplicate {
		out.Duplicate = true
		return &channelpb.DeliverChannelNotificationResponse{FrontendResponse: out}, nil
	}
	folded, err := h.accept(ctx, namespaceID, workflowID, target.runID, n)
	if err != nil {
		return nil, err
	}
	out.Folded = folded
	tag := metrics.NamespaceTag(ns)
	metrics.ChannelNotificationsDelivered.With(h.metricsHandler).Record(1, tag, workflowKindTag)
	if folded {
		metrics.ChannelNotificationsFolded.With(h.metricsHandler).Record(1, tag, workflowKindTag)
	}
	return &channelpb.DeliverChannelNotificationResponse{FrontendResponse: out}, nil
}

var (
	workflowKindTag = metrics.StringTag("listener_kind", "workflow")
	callbackKindTag = metrics.StringTag("listener_kind", "callback")
)
