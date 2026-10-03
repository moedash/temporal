package respondworkflowtaskcompleted

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// registerStagedChannelListeners registers this run on every channel a
// subscribe command in the task named, on each channel's own shard.
//
// The command handler recorded the subscription and wrote its event, but it
// holds the state lock with nowhere to do I/O from, so the registration
// happens here, between the commands and the commit. The channel hands back
// its latest notification, which the run takes as pending so the task this
// completion schedules carries it. A refusal fails the task
// with the given cause, which rolls the subscription and its event back with
// it. A registration that succeeded before a later one was refused stays on
// its channel; the channel's next fan-out finds the run does not listen and
// drops it.
func registerStagedChannelListeners(
	ctx context.Context,
	ms historyi.MutableState,
	cause enumspb.WorkflowTaskFailedCause,
	staged []string,
) error {
	if len(staged) == 0 {
		return nil
	}
	client, ok := channel.ClientFromContext(ctx)
	if !ok {
		return serviceerror.NewInternal(
			"channel service client is not available on the workflow task completion path")
	}

	// One budget over the whole set, since the execution's lock is held for
	// all of them.
	ctx, cancelBudget := chasmworkflow.WithRoutedBudget(ctx)
	defer cancelBudget()

	wf, chasmCtx, err := ms.ChasmWorkflowComponent(ctx)
	if err != nil {
		return err
	}
	key := ms.GetWorkflowKey()
	namespaceName := ms.GetNamespaceEntry().Name().String()
	firstRunID := ms.GetExecutionInfo().GetFirstExecutionRunId()
	for _, name := range staged {
		// An unsubscribe later in the same task took the subscription off
		// again, so there is nothing to register and no latest to take.
		if !wf.SubscribedToChannel(name) {
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, channel.RoutedCallTimeout)
		resp, err := client.RegisterWorkflowListener(callCtx, &channelpb.RegisterWorkflowListenerRequest{
			NamespaceId: key.NamespaceID,
			FrontendRequest: &channelpb.RegisterWorkflowListenerInput{
				Namespace:           namespaceName,
				Channel:             name,
				WorkflowId:          key.WorkflowID,
				RunId:               key.RunID,
				FirstExecutionRunId: firstRunID,
			},
		})
		cancel()
		if err != nil {
			return chasmworkflow.AdmissionFailure(cause, err)
		}
		if latest := resp.GetFrontendResponse().GetLatest(); latest != nil {
			if _, err := wf.AcceptChannelNotification(chasmCtx, latest); err != nil {
				return err
			}
		}
	}
	return nil
}
