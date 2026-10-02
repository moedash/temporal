package service

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/metrics"
)

// The linked kind is served on the shard of the workflow that holds the
// channel. The requests are the independent kind's with workflow_execution
// set; an empty run id means the chain's current run, as it does for a
// Signal. A workflow that has closed took its linked channels with it, so a
// call on one answers NotFound, and a name nobody has notified yet on a
// running workflow is a linked channel with nothing in it rather than one
// that does not exist, which is how a client probes for the linked kind.

var (
	independentKindTag = metrics.StringTag("kind", "independent")
	linkedKindTag      = metrics.StringTag("kind", "linked")
)

func ownerRef(namespaceID string, owner *commonpb.WorkflowExecution) chasm.ComponentRef {
	return workflowRef(namespaceID, owner.GetWorkflowId(), owner.GetRunId())
}

func checkOwner(owner *commonpb.WorkflowExecution) error {
	if owner.GetWorkflowId() == "" {
		return serviceerror.NewInvalidArgument("a linked channel call needs the owner's workflow id")
	}
	return nil
}

func ownerClosedError(owner *commonpb.WorkflowExecution) error {
	return serviceerror.NewNotFoundf(
		"workflow %q has closed and its linked channels are gone with it", owner.GetWorkflowId())
}

func ownerClosed(ctx chasm.Context) bool {
	return !ctx.ExecutionInfo().CloseTime.IsZero()
}

// ownerRead is what a call learns from reading the owner first.
type ownerRead struct {
	closed bool
	// The linked channel exists on the run.
	exists bool
	latest int64
	// For a notify at the counter asked about.
	advances      bool
	ownerHolds    bool
	callbacksNeed bool
	listeners     int
}

// readOwner reads the owner without touching it. An absent workflow comes
// back as NotFound.
func (h *handler) readOwner(
	ctx context.Context,
	namespaceID string,
	owner *commonpb.WorkflowExecution,
	name string,
	counter int64,
) (ownerRead, error) {
	return chasm.ReadComponent(ctx, ownerRef(namespaceID, owner),
		func(wf *chasmworkflow.Workflow, cctx chasm.Context, _ struct{}) (ownerRead, error) {
			r := ownerRead{closed: ownerClosed(cctx), advances: true, listeners: 1}
			c, ok := wf.LinkedChannel(cctx, name)
			if !ok {
				return r, nil
			}
			r.exists = true
			r.latest = c.LatestCounter()
			r.advances = c.Advances(counter)
			r.ownerHolds = c.OwnerHolds(counter)
			r.callbacksNeed = c.CallbacksNeed(cctx, counter)
			r.listeners = c.LinkedListenerCount()
			return r, nil
		}, struct{}{})
}

// NotifyLinkedChannel accepts a notification on a channel linked to a
// workflow, creating the channel on the run on first use. The owner is the
// listener: the notification waits on its run for the next scheduled event,
// and the same write hands it to the callback listeners.
//
// Read first, as the independent kind does. A notification the owner already
// holds, pending or on a task that has not started, and that every callback
// listener holds too, changes nothing and writes nothing.
func (h *handler) NotifyLinkedChannel(
	ctx context.Context,
	req *channelpb.NotifyChannelRequest,
) (*channelpb.NotifyChannelResponse, error) {
	in := req.GetFrontendRequest()
	owner := in.GetWorkflowExecution()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	limits := h.limitsFor(ns)
	if err := checkOwner(owner); err != nil {
		return nil, err
	}
	n := in.GetNotification()
	if err := channel.CheckNotification(n, limits.MaxMetadataBytes); err != nil {
		return nil, err
	}
	if err := h.limiters.allowNotify(ns, h.timeSource.Now()); err != nil {
		return nil, err
	}

	read, err := h.readOwner(ctx, req.GetNamespaceId(), owner, n.GetChannel(), n.GetCounter())
	if err != nil {
		return nil, err
	}
	if read.closed {
		return nil, ownerClosedError(owner)
	}
	nsTag := metrics.NamespaceTag(ns)
	if read.exists && !read.advances && read.ownerHolds && !read.callbacksNeed {
		metrics.ChannelNotificationsFolded.With(h.metricsHandler).Record(
			1, nsTag, linkedKindTag, workflowKindTag)
		return &channelpb.NotifyChannelResponse{
			FrontendResponse: &channelpb.NotifyChannelOutput{ListenerCount: int64(read.listeners)},
		}, nil
	}

	result, _, err := chasm.UpdateComponent(ctx, ownerRef(req.GetNamespaceId(), owner),
		func(
			wf *chasmworkflow.Workflow,
			mctx chasm.MutableContext,
			n *channelpb.Notification,
		) (channel.LinkedNotifyResult, error) {
			if ownerClosed(mctx) {
				return channel.LinkedNotifyResult{}, ownerClosedError(owner)
			}
			return wf.NotifyLinkedChannel(mctx, n, limits)
		}, n)
	if err != nil {
		return nil, err
	}
	if result.Advanced {
		metrics.ChannelNotificationsAccepted.With(h.metricsHandler).Record(1, nsTag, linkedKindTag)
	}
	if result.OwnerHeld || result.OwnerFolded {
		metrics.ChannelNotificationsFolded.With(h.metricsHandler).Record(
			1, nsTag, linkedKindTag, workflowKindTag)
	}
	if !result.OwnerHeld {
		metrics.ChannelNotificationsDelivered.With(h.metricsHandler).Record(
			1, nsTag, linkedKindTag, workflowKindTag)
	}
	if result.CallbackFolded > 0 {
		metrics.ChannelNotificationsFolded.With(h.metricsHandler).Record(
			int64(result.CallbackFolded), nsTag, linkedKindTag, callbackKindTag)
	}
	return &channelpb.NotifyChannelResponse{
		FrontendResponse: &channelpb.NotifyChannelOutput{ListenerCount: int64(result.ListenerCount)},
	}, nil
}

// RegisterLinkedChannelListener adds a callback listener to a linked channel,
// creating the channel on the run if it has none by that name. A new
// listener is posted the latest notification, as on an independent channel.
func (h *handler) RegisterLinkedChannelListener(
	ctx context.Context,
	req *channelpb.RegisterChannelListenerRequest,
) (*channelpb.RegisterChannelListenerResponse, error) {
	in := req.GetFrontendRequest()
	owner := in.GetWorkflowExecution()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	if err := checkOwner(owner); err != nil {
		return nil, err
	}
	if err := channel.CheckChannelName(in.GetChannel(), 0); err != nil {
		return nil, err
	}
	limits := h.limitsFor(ns)
	reg := channel.CallbackRegistration{
		RequestID: in.GetRequestId(),
		Callback:  in.GetCallback(),
		Limits:    limits,
	}
	listenerID, _, err := chasm.UpdateComponent(ctx, ownerRef(req.GetNamespaceId(), owner),
		func(wf *chasmworkflow.Workflow, mctx chasm.MutableContext, name string) (string, error) {
			if ownerClosed(mctx) {
				return "", ownerClosedError(owner)
			}
			c, err := wf.LinkedChannelOrNew(mctx, name, limits.MaxLinkedChannelsPerWorkflow)
			if err != nil {
				return "", err
			}
			return c.RegisterCallbackListener(mctx, reg)
		}, in.GetChannel())
	if err != nil {
		return nil, err
	}
	return &channelpb.RegisterChannelListenerResponse{
		FrontendResponse: &channelpb.RegisterChannelListenerOutput{ListenerId: listenerID},
	}, nil
}

// UnregisterLinkedChannelListener drops a callback listener from a linked
// channel. A workflow, channel or listener that is already gone is not an
// error, so a retry succeeds.
func (h *handler) UnregisterLinkedChannelListener(
	ctx context.Context,
	req *channelpb.UnregisterChannelListenerRequest,
) (*channelpb.UnregisterChannelListenerResponse, error) {
	in := req.GetFrontendRequest()
	owner := in.GetWorkflowExecution()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	if err := checkOwner(owner); err != nil {
		return nil, err
	}
	limits := h.limitsFor(ns)
	done := &channelpb.UnregisterChannelListenerResponse{
		FrontendResponse: &channelpb.UnregisterChannelListenerOutput{},
	}
	read, err := h.readOwner(ctx, req.GetNamespaceId(), owner, in.GetChannel(), 0)
	if executionAbsent(err) {
		return done, nil
	}
	if err != nil {
		return nil, err
	}
	if read.closed || !read.exists {
		return done, nil
	}
	_, _, err = chasm.UpdateComponent(ctx, ownerRef(req.GetNamespaceId(), owner),
		func(wf *chasmworkflow.Workflow, mctx chasm.MutableContext, id string) (struct{}, error) {
			c, ok := wf.LinkedChannel(mctx, in.GetChannel())
			if !ok {
				return struct{}{}, nil
			}
			return struct{}{}, c.UnregisterListener(mctx, id, limits)
		}, in.GetListenerId())
	if err != nil && !executionAbsent(err) {
		return nil, err
	}
	return done, nil
}

// PollLinkedChannel returns the retained notifications of a linked channel
// above the caller's counter, waiting on the owner's execution for one when
// none is there yet. A name nobody has notified is a channel with nothing in
// it: the poll waits for the first notify, which creates it.
func (h *handler) PollLinkedChannel(
	ctx context.Context,
	req *channelpb.PollChannelRequest,
) (*channelpb.PollChannelResponse, error) {
	in := req.GetFrontendRequest()
	owner := in.GetWorkflowExecution()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	if err := checkOwner(owner); err != nil {
		return nil, err
	}
	metrics.ChannelPollers.With(h.metricsHandler).Record(1, metrics.NamespaceTag(ns), linkedKindTag)
	ref := ownerRef(req.GetNamespaceId(), owner)
	name := in.GetChannel()

	read, err := h.readOwner(ctx, req.GetNamespaceId(), owner, name, 0)
	if err != nil {
		return nil, err
	}
	if read.closed {
		return nil, ownerClosedError(owner)
	}
	if wait := in.GetWait().AsDuration(); wait > 0 && read.latest <= in.GetAfterCounter() {
		pollCtx, cancel := pollBudget(ctx, wait)
		defer cancel()
		_, _, err := chasm.PollComponent(pollCtx, ref,
			func(wf *chasmworkflow.Workflow, cctx chasm.Context, after int64) (struct{}, bool, error) {
				// Monotonic, as PollComponent requires: a linked channel is
				// never removed from a run, and its latest counter only grows.
				c, ok := wf.LinkedChannel(cctx, name)
				return struct{}{}, ok && c.LatestCounter() > after, nil
			}, in.GetAfterCounter())
		if err != nil && (pollCtx.Err() == nil || ctx.Err() != nil) {
			return nil, err
		}
	}

	notifications, err := chasm.ReadComponent(ctx, ref,
		func(wf *chasmworkflow.Workflow, cctx chasm.Context, r channel.PollRequest) ([]*channelpb.Notification, error) {
			c, ok := wf.LinkedChannel(cctx, name)
			if !ok {
				return nil, nil
			}
			return c.Poll(cctx, r)
		}, channel.PollRequest{AfterCounter: in.GetAfterCounter(), Max: int(in.GetMaxNotifications())})
	if err != nil {
		return nil, err
	}
	return &channelpb.PollChannelResponse{
		FrontendResponse: &channelpb.PollChannelOutput{Notifications: notifications},
	}, nil
}

// DescribeLinkedChannel reports a linked channel: the owner and the callback
// listeners, the latest notification and how many are retained. A name
// nobody has notified yet on a running workflow answers as linked with
// nothing in it.
func (h *handler) DescribeLinkedChannel(
	ctx context.Context,
	req *channelpb.DescribeChannelRequest,
) (*channelpb.DescribeChannelResponse, error) {
	in := req.GetFrontendRequest()
	owner := in.GetWorkflowExecution()
	ctx = withCallerInfo(ctx, h.namespaceName(req.GetNamespaceId()))
	if err := checkOwner(owner); err != nil {
		return nil, err
	}
	out, err := chasm.ReadComponent(ctx, ownerRef(req.GetNamespaceId(), owner),
		func(wf *chasmworkflow.Workflow, cctx chasm.Context, name string) (*channelpb.DescribeChannelOutput, error) {
			if ownerClosed(cctx) {
				return nil, ownerClosedError(owner)
			}
			key := cctx.ExecutionKey()
			res := &channelpb.DescribeChannelOutput{
				Linked:   true,
				LinkedTo: &commonpb.WorkflowExecution{WorkflowId: key.BusinessID, RunId: key.RunID},
			}
			c, ok := wf.LinkedChannel(cctx, name)
			if !ok {
				return res, nil
			}
			snapshot, err := c.Describe(cctx, struct{}{})
			if err != nil {
				return nil, err
			}
			res.Listeners = append([]*channelpb.ChannelListenerInfo{c.OwnerListenerInfo(cctx)},
				snapshot.Listeners...)
			res.Latest = snapshot.Latest
			res.RetainedCount = snapshot.RetainedCount
			return res, nil
		}, in.GetChannel())
	if err != nil {
		return nil, err
	}
	return &channelpb.DescribeChannelResponse{FrontendResponse: out}, nil
}
