package service

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/metrics"
)

// The linked kind is served on the shard of the execution that holds the
// channel, a workflow run or a standalone activity. The requests are the
// independent kind's with execution set. An empty run id means the chain's
// current run, as it does for a Signal. An execution that has closed took its
// linked channels with it, so a call on one answers NotFound, and a name
// nobody has notified yet on a running execution is a linked channel with
// nothing in it rather than one that does not exist, which is how a client
// probes for the linked kind.
//
// Each call is written once over the owner's component type and dispatched
// on the execution's type, since the engine reads a component by its
// archetype. An unset type is a workflow: the HTTP routes bind the business
// id alone, and the frontend fills the type in where the route says.

var (
	independentKindTag = metrics.StringTag("kind", "independent")
	linkedKindTag      = metrics.StringTag("kind", "linked")
)

func ownerRef[O channel.LinkedOwner](
	namespaceID string,
	owner *commonpb.Execution,
) chasm.ComponentRef {
	return chasm.NewComponentRef[O](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  owner.GetBusinessId(),
		RunID:       owner.GetRunId(),
	})
}

func isActivity(owner *commonpb.Execution) bool {
	return owner.GetType() == enumspb.EXECUTION_TYPE_ACTIVITY
}

// checkOwner refuses an execution the linked kind cannot serve.
func checkOwner(owner *commonpb.Execution) error {
	if owner.GetBusinessId() == "" {
		return serviceerror.NewInvalidArgument("a linked channel call needs the owner's business id")
	}
	switch owner.GetType() {
	case enumspb.EXECUTION_TYPE_UNSPECIFIED,
		enumspb.EXECUTION_TYPE_WORKFLOW,
		enumspb.EXECUTION_TYPE_ACTIVITY:
		return nil
	default:
		return serviceerror.NewInvalidArgumentf(
			"a channel cannot be linked to an execution of type %v", owner.GetType())
	}
}

func ownerKind(owner *commonpb.Execution) string {
	if isActivity(owner) {
		return "activity"
	}
	return "workflow"
}

func ownerClosedError(owner *commonpb.Execution) error {
	return serviceerror.NewNotFoundf("%s %q has closed and its linked channels are gone with it",
		ownerKind(owner), owner.GetBusinessId())
}

func ownerClosed(ctx chasm.Context) bool {
	return !ctx.ExecutionInfo().CloseTime.IsZero()
}

// ownerRead is what a call learns from reading the owner first.
type ownerRead struct {
	closed bool
	// The linked channel exists on the execution.
	exists bool
	latest int64
	// For a notify at the counter asked about. An owner that does not listen
	// holds every counter: there is nothing to hand it.
	advances      bool
	ownerHolds    bool
	callbacksNeed bool
	listeners     int
}

// readOwner reads the owner without touching it. An absent execution comes
// back as NotFound.
func readOwner[O channel.LinkedOwner](
	ctx context.Context,
	namespaceID string,
	owner *commonpb.Execution,
	name string,
	counter int64,
) (ownerRead, error) {
	return chasm.ReadComponent(ctx, ownerRef[O](namespaceID, owner),
		func(o O, cctx chasm.Context, _ struct{}) (ownerRead, error) {
			listens := channel.OwnerListens(cctx)
			r := ownerRead{closed: ownerClosed(cctx), advances: true, ownerHolds: !listens}
			if listens {
				r.listeners = 1
			}
			c, ok := o.LinkedChannel(cctx, name)
			if !ok {
				return r, nil
			}
			r.exists = true
			r.latest = c.LatestCounter()
			r.advances = c.Advances(counter)
			r.ownerHolds = !listens || c.OwnerHolds(counter)
			r.callbacksNeed = c.CallbacksNeed(cctx, counter)
			r.listeners = c.LinkedListenerCount(cctx)
			return r, nil
		}, struct{}{})
}

// NotifyLinkedChannel accepts a notification on a channel linked to an
// execution, creating the channel on the execution on first use. A workflow
// owner is the listener: the notification waits on its run for the next
// scheduled event, and the same write hands it to the callback listeners. An
// activity owner is not, so the write reaches the callbacks and the ring.
//
// Read first, as the independent kind does. A notification the owner already
// holds, pending or on a task that has not started, and that every callback
// listener holds too, changes nothing and writes nothing.
func (h *handler) NotifyLinkedChannel(
	ctx context.Context,
	req *channelpb.NotifyChannelRequest,
) (*channelpb.NotifyChannelResponse, error) {
	in := req.GetFrontendRequest()
	owner := in.GetExecution()
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
	if isActivity(owner) {
		return notifyLinked[*activity.Activity](ctx, h, req.GetNamespaceId(), ns, owner, n, limits)
	}
	return notifyLinked[*chasmworkflow.Workflow](ctx, h, req.GetNamespaceId(), ns, owner, n, limits)
}

func notifyLinked[O channel.LinkedOwner](
	ctx context.Context,
	h *handler,
	namespaceID, ns string,
	owner *commonpb.Execution,
	n *channelpb.Notification,
	limits channel.Limits,
) (*channelpb.NotifyChannelResponse, error) {
	read, err := readOwner[O](ctx, namespaceID, owner, n.GetChannel(), n.GetCounter())
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

	result, _, err := chasm.UpdateComponent(ctx, ownerRef[O](namespaceID, owner),
		func(
			o O, mctx chasm.MutableContext, n *channelpb.Notification,
		) (channel.LinkedNotifyResult, error) {
			if ownerClosed(mctx) {
				return channel.LinkedNotifyResult{}, ownerClosedError(owner)
			}
			return o.NotifyLinkedChannel(mctx, n, limits)
		}, n)
	if err != nil {
		return nil, err
	}
	if result.Advanced {
		metrics.ChannelNotificationsAccepted.With(h.metricsHandler).Record(1, nsTag, linkedKindTag)
	}
	if result.OwnerListens {
		if result.OwnerHeld || result.OwnerFolded {
			metrics.ChannelNotificationsFolded.With(h.metricsHandler).Record(
				1, nsTag, linkedKindTag, workflowKindTag)
		}
		if !result.OwnerHeld {
			metrics.ChannelNotificationsDelivered.With(h.metricsHandler).Record(
				1, nsTag, linkedKindTag, workflowKindTag)
		}
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
// creating the channel on the execution if it has none by that name. A new
// listener is posted the latest notification, as on an independent channel.
func (h *handler) RegisterLinkedChannelListener(
	ctx context.Context,
	req *channelpb.RegisterChannelListenerRequest,
) (*channelpb.RegisterChannelListenerResponse, error) {
	in := req.GetFrontendRequest()
	owner := in.GetExecution()
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
	var listenerID string
	var err error
	if isActivity(owner) {
		listenerID, err = registerLinked[*activity.Activity](
			ctx, req.GetNamespaceId(), owner, in.GetChannel(), reg)
	} else {
		listenerID, err = registerLinked[*chasmworkflow.Workflow](
			ctx, req.GetNamespaceId(), owner, in.GetChannel(), reg)
	}
	if err != nil {
		return nil, err
	}
	return &channelpb.RegisterChannelListenerResponse{
		FrontendResponse: &channelpb.RegisterChannelListenerOutput{ListenerId: listenerID},
	}, nil
}

func registerLinked[O channel.LinkedOwner](
	ctx context.Context,
	namespaceID string,
	owner *commonpb.Execution,
	name string,
	reg channel.CallbackRegistration,
) (string, error) {
	listenerID, _, err := chasm.UpdateComponent(ctx, ownerRef[O](namespaceID, owner),
		func(o O, mctx chasm.MutableContext, name string) (string, error) {
			if ownerClosed(mctx) {
				return "", ownerClosedError(owner)
			}
			c, err := o.LinkedChannelOrNew(mctx, name, reg.Limits.MaxLinkedChannelsPerWorkflow)
			if err != nil {
				return "", err
			}
			return c.RegisterCallbackListener(mctx, reg)
		}, name)
	return listenerID, err
}

// UnregisterLinkedChannelListener drops a callback listener from a linked
// channel. An execution, channel or listener that is already gone is not an
// error, so a retry succeeds.
func (h *handler) UnregisterLinkedChannelListener(
	ctx context.Context,
	req *channelpb.UnregisterChannelListenerRequest,
) (*channelpb.UnregisterChannelListenerResponse, error) {
	in := req.GetFrontendRequest()
	owner := in.GetExecution()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	if err := checkOwner(owner); err != nil {
		return nil, err
	}
	limits := h.limitsFor(ns)
	var err error
	if isActivity(owner) {
		err = unregisterLinked[*activity.Activity](
			ctx, req.GetNamespaceId(), owner, in.GetChannel(), in.GetListenerId(), limits)
	} else {
		err = unregisterLinked[*chasmworkflow.Workflow](
			ctx, req.GetNamespaceId(), owner, in.GetChannel(), in.GetListenerId(), limits)
	}
	if err != nil {
		return nil, err
	}
	return &channelpb.UnregisterChannelListenerResponse{
		FrontendResponse: &channelpb.UnregisterChannelListenerOutput{},
	}, nil
}

func unregisterLinked[O channel.LinkedOwner](
	ctx context.Context,
	namespaceID string,
	owner *commonpb.Execution,
	name, listenerID string,
	limits channel.Limits,
) error {
	read, err := readOwner[O](ctx, namespaceID, owner, name, 0)
	if executionAbsent(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if read.closed || !read.exists {
		return nil
	}
	_, _, err = chasm.UpdateComponent(ctx, ownerRef[O](namespaceID, owner),
		func(o O, mctx chasm.MutableContext, id string) (struct{}, error) {
			c, ok := o.LinkedChannel(mctx, name)
			if !ok {
				return struct{}{}, nil
			}
			return struct{}{}, c.UnregisterListener(mctx, id, limits)
		}, listenerID)
	if err != nil && !executionAbsent(err) {
		return err
	}
	return nil
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
	owner := in.GetExecution()
	ns := h.namespaceName(req.GetNamespaceId())
	ctx = withCallerInfo(ctx, ns)
	if err := checkOwner(owner); err != nil {
		return nil, err
	}
	metrics.ChannelPollers.With(h.metricsHandler).Record(1, metrics.NamespaceTag(ns), linkedKindTag)
	if isActivity(owner) {
		return pollLinked[*activity.Activity](ctx, req.GetNamespaceId(), owner, in)
	}
	return pollLinked[*chasmworkflow.Workflow](ctx, req.GetNamespaceId(), owner, in)
}

func pollLinked[O channel.LinkedOwner](
	ctx context.Context,
	namespaceID string,
	owner *commonpb.Execution,
	in *channelpb.PollChannelInput,
) (*channelpb.PollChannelResponse, error) {
	ref := ownerRef[O](namespaceID, owner)
	name := in.GetChannel()

	read, err := readOwner[O](ctx, namespaceID, owner, name, 0)
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
			func(o O, cctx chasm.Context, after int64) (struct{}, bool, error) {
				// Monotonic, as PollComponent requires: a linked channel is
				// never removed from its owner, and its latest counter only
				// grows.
				c, ok := o.LinkedChannel(cctx, name)
				return struct{}{}, ok && c.LatestCounter() > after, nil
			}, in.GetAfterCounter())
		if err != nil && (pollCtx.Err() == nil || ctx.Err() != nil) {
			return nil, err
		}
	}

	notifications, err := chasm.ReadComponent(ctx, ref,
		func(o O, cctx chasm.Context, r channel.PollRequest) ([]*channelpb.Notification, error) {
			c, ok := o.LinkedChannel(cctx, name)
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

// DescribeLinkedChannel reports a linked channel: the owner when it listens,
// the callback listeners, the latest notification and how many are retained.
// A name nobody has notified yet on a running execution answers as linked
// with nothing in it.
func (h *handler) DescribeLinkedChannel(
	ctx context.Context,
	req *channelpb.DescribeChannelRequest,
) (*channelpb.DescribeChannelResponse, error) {
	in := req.GetFrontendRequest()
	owner := in.GetExecution()
	ctx = withCallerInfo(ctx, h.namespaceName(req.GetNamespaceId()))
	if err := checkOwner(owner); err != nil {
		return nil, err
	}
	var out *channelpb.DescribeChannelOutput
	var err error
	if isActivity(owner) {
		out, err = describeLinked[*activity.Activity](ctx, req.GetNamespaceId(), owner, in.GetChannel())
	} else {
		out, err = describeLinked[*chasmworkflow.Workflow](
			ctx, req.GetNamespaceId(), owner, in.GetChannel())
	}
	if err != nil {
		return nil, err
	}
	return &channelpb.DescribeChannelResponse{FrontendResponse: out}, nil
}

func describeLinked[O channel.LinkedOwner](
	ctx context.Context,
	namespaceID string,
	owner *commonpb.Execution,
	name string,
) (*channelpb.DescribeChannelOutput, error) {
	return chasm.ReadComponent(ctx, ownerRef[O](namespaceID, owner),
		func(o O, cctx chasm.Context, name string) (*channelpb.DescribeChannelOutput, error) {
			if ownerClosed(cctx) {
				return nil, ownerClosedError(owner)
			}
			res := &channelpb.DescribeChannelOutput{Linked: true, LinkedTo: channel.ExecutionOf(cctx)}
			c, ok := o.LinkedChannel(cctx, name)
			if !ok {
				return res, nil
			}
			snapshot, err := c.Describe(cctx, struct{}{})
			if err != nil {
				return nil, err
			}
			if ownerInfo := c.OwnerListenerInfo(cctx); ownerInfo != nil {
				res.Listeners = append(res.Listeners, ownerInfo)
			}
			res.Listeners = append(res.Listeners, snapshot.Listeners...)
			res.Latest = snapshot.Latest
			res.RetainedCount = snapshot.RetainedCount
			return res, nil
		}, name)
}
