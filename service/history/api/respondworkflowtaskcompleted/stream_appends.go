package respondworkflowtaskcompleted

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/service/history/api/recordworkflowtaskstarted"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// resolveStagedStreamSubscriptions turns subscribe commands for streams in
// other executions into cursors on this workflow.
//
// The lookup cannot happen in the command handler, which runs under the state
// lock with nowhere to do I/O from, and it cannot happen at delivery either,
// because by then the cursor has to already exist. So it happens here, between
// the commands and the commit.
//
// The pin goes on the stream before the cursor goes on the workflow, and that
// order is the guarantee. Interrupted after the first write there is a pin
// holding storage nothing reads, which costs space. The other order would leave
// a cursor no truncation floor protects, and truncation would be free to take a
// range it still points at.
//
// The pin is taken through the routed service client. The engine on this
// request context resolves shards through the local controller, which refuses
// any shard this host does not own, so a stream living elsewhere in the
// cluster can only be reached by going back out through the service.
//
// A refusal, such as a stream that does not exist or an offset below its floor,
// comes back as a workflow task failure so the worker sees a cause rather than
// retrying the same command. A pin already taken for an earlier subscription
// in the same task stays on its stream; the notify task releases it when it
// finds this workflow does not consume that stream.
func resolveStagedStreamSubscriptions(
	ctx context.Context,
	ms historyi.MutableState,
	namespaceID string,
	limits stream.Limits,
	staged []chasmworkflow.PendingStreamSubscription,
) error {
	if len(staged) == 0 {
		return nil
	}

	wf, chasmCtx, err := ms.ChasmWorkflowComponent(ctx)
	if err != nil {
		return err
	}

	for _, pending := range staged {
		// Already subscribed, so only the event is owed. Re-registering would
		// re-run the pin write with the original start offset, which would drag
		// the stream's truncation floor back to where this consumer began.
		if pending.AlreadySubscribed {
			cursor, ok := wf.StreamCursors[pending.StreamID]
			if !ok {
				return serviceerror.NewInternalf(
					"stream %q was marked already subscribed but has no cursor", pending.StreamID)
			}
			chasmworkflow.RecordStreamSubscribedOffset(
				pending.Event, cursor.Get(chasmCtx).Offset())
			continue
		}

		// A stream this workflow owns needs no lookup and no pin registration:
		// it is in this execution, and its cursor commits with everything else.
		if _, owned := wf.Streams[pending.StreamID]; owned {
			startOffset, err := wf.SubscribeToOwnedStream(
				chasmCtx, pending.StreamID, pending.StartOffset, limits)
			if err != nil {
				return chasmworkflow.StreamAdmissionFailure(
					enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SUBSCRIBE_STREAM_ATTRIBUTES, err)
			}
			chasmworkflow.RecordStreamSubscribedOffset(pending.Event, startOffset)
			continue
		}

		pin, err := registerExternalConsumer(ctx, ms, namespaceID, pending)
		if err != nil {
			return chasmworkflow.StreamAdmissionFailure(
				enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SUBSCRIBE_STREAM_ATTRIBUTES, err)
		}

		if _, err := wf.SubscribeToExternalStream(chasmCtx, chasmworkflow.ExternalStreamSubscription{
			StreamID:    pending.StreamID,
			StartOffset: pin.GetStartOffset(),
			KnownHead:   pin.GetKnownHead(),
		}); err != nil {
			return err
		}

		// Completed after the cursor exists. The event was written where the
		// command was, but nothing outside this transaction sees either until
		// the commit below, so a crash in between leaves no event claiming a
		// subscription that was never made.
		chasmworkflow.RecordStreamSubscribedOffset(pending.Event, pin.GetStartOffset())
	}
	return nil
}

// registerExternalConsumer takes the pin on the stream's own shard and returns
// the resolved start offset and the frontier as of registration.
func registerExternalConsumer(
	ctx context.Context,
	ms historyi.MutableState,
	namespaceID string,
	pending chasmworkflow.PendingStreamSubscription,
) (*streamlib.RegisterStreamConsumerOutput, error) {
	client, ok := recordworkflowtaskstarted.StreamClientFromContext(ctx)
	if !ok {
		return nil, serviceerror.NewInternal(
			"stream service client is not available on the workflow task completion path")
	}

	key := ms.GetWorkflowKey()
	callCtx, cancel := recordworkflowtaskstarted.WithRoutedDeadline(ctx)
	defer cancel()
	registered, err := client.RegisterStreamConsumer(callCtx, &streamlib.RegisterStreamConsumerRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &streamlib.RegisterStreamConsumerInput{
			Namespace:          ms.GetNamespaceEntry().Name().String(),
			StreamId:           pending.StreamID,
			ConsumerWorkflowId: key.WorkflowID,
			ConsumerRunId:      key.RunID,
			StartOffset:        pending.StartOffset,
		},
	})
	if err != nil {
		return nil, err
	}
	return registered.GetFrontendResponse(), nil
}
