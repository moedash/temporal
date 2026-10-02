package service

import (
	"context"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"golang.org/x/sync/errgroup"
)

// workflowDeliverer hands a notification to workflow listeners, each on its
// own shard through the routed service, and keeps the listener table honest
// about runs that have ended. The fan-out task uses it for an advancing
// notify, and NotifyChannel uses it directly for one that does not advance
// the channel, which writes nothing on the channel itself.
type workflowDeliverer struct {
	logger log.Logger
	config *channel.Config
	routed channelpb.ChannelServiceClient
}

func newWorkflowDeliverer(
	logger log.Logger,
	config *channel.Config,
	routed channelpb.ChannelServiceClient,
) *workflowDeliverer {
	return &workflowDeliverer{logger: logger, config: config, routed: routed}
}

// deliverAll tells every listener. No cancelling context on purpose: one
// unreachable listener must not stop the others being told. The first error
// comes back, so the caller retries the set; a run that already holds the
// notification pending takes the retry as a fold and changes nothing.
func (h *workflowDeliverer) deliverAll(
	ctx context.Context,
	ref chasm.ComponentRef,
	ns string,
	listeners []*channelpb.WorkflowListener,
	n *channelpb.Notification,
) error {
	var group errgroup.Group
	group.SetLimit(fanOutConcurrency)
	for _, listener := range listeners {
		group.Go(func() error {
			return h.deliverOne(ctx, ref, ns, listener, n)
		})
	}
	return group.Wait()
}

// deliverOne tells one workflow listener and acts on its answer. The call
// carries its own deadline, so a listener on a slow host does not take the
// whole task's.
func (h *workflowDeliverer) deliverOne(
	ctx context.Context,
	ref chasm.ComponentRef,
	ns string,
	listener *channelpb.WorkflowListener,
	n *channelpb.Notification,
) error {
	callCtx, cancel := context.WithTimeout(ctx, channel.RoutedCallTimeout)
	defer cancel()
	response, err := h.routed.DeliverChannelNotification(callCtx, &channelpb.DeliverChannelNotificationRequest{
		NamespaceId: ref.NamespaceID,
		FrontendRequest: &channelpb.DeliverChannelNotificationInput{
			Namespace:    ns,
			WorkflowId:   listener.GetWorkflowId(),
			RunId:        listener.GetRunId(),
			Notification: n,
		},
	})
	if err != nil {
		// Not taken as proof the listener is gone. The handler already reads a
		// missing run as closed and says so in its answer, so an error here is
		// the transport's, and dropping a listener on that would be a guess.
		h.logger.Warn("failed to deliver a channel notification to a workflow",
			tag.NewStringTag("channel", ref.BusinessID),
			tag.WorkflowID(listener.GetWorkflowId()),
			tag.Error(err))
		return err
	}
	out := response.GetFrontendResponse()
	limits := h.config.LimitsFor(ns)
	switch {
	case out.GetListenerClosed():
		_, _, err = chasm.UpdateComponent(ctx, ref,
			func(c *channel.Channel, mctx chasm.MutableContext, l *channelpb.WorkflowListener) (struct{}, error) {
				c.ForgetWorkflowListener(mctx, l.GetWorkflowId(), l.GetRunId(), limits)
				return struct{}{}, nil
			}, listener)
	case out.GetSuccessorRunId() != "":
		_, _, err = chasm.UpdateComponent(ctx, ref,
			func(c *channel.Channel, mctx chasm.MutableContext, l *channelpb.WorkflowListener) (struct{}, error) {
				c.RekeyWorkflowListener(mctx, l.GetWorkflowId(), l.GetRunId(), out.GetSuccessorRunId())
				return struct{}{}, nil
			}, listener)
	default:
		// Delivered, or a duplicate. The listener stays as it is.
	}
	return err
}
