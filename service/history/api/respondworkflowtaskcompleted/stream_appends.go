package respondworkflowtaskcompleted

import (
	"context"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// flushStagedStreamAppends writes the log nodes staged by stream commands
// during this workflow task.
//
// Ordering is the whole point: nodes first, then the workflow task commit
// advances the stream's frontier as part of the workflow's own mutable state.
// Doing it the other way would publish offsets whose bytes are not yet durable.
// A crash in between leaves nodes at or past the frontier, which no reader can
// observe, and the retried task stages them again.
func flushStagedStreamAppends(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID string,
	staged []chasmworkflow.PendingStreamAppend,
) error {
	for _, p := range staged {
		if err := stream.WriteAppend(ctx, shardContext.GetExecutionManager(),
			shardContext.GetShardID(), namespaceID, p.CollectionID, p.Append); err != nil {
			return err
		}
	}
	return nil
}

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
func resolveStagedStreamSubscriptions(
	ctx context.Context,
	ms historyi.MutableState,
	namespaceID string,
	completedEventID int64,
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
		// A stream this workflow owns needs no lookup and no pin registration:
		// it is in this execution, and its cursor commits with everything else.
		if _, owned := wf.Streams[pending.StreamID]; owned {
			startOffset, err := wf.SubscribeToOwnedStream(
				chasmCtx, pending.StreamID, pending.StartOffset)
			if err != nil {
				return err
			}
			wf.RecordStreamSubscribed(pending.StreamID, startOffset, completedEventID)
			continue
		}

		ref := chasm.NewComponentRef[*stream.Stream](chasm.ExecutionKey{
			NamespaceID: namespaceID,
			BusinessID:  pending.StreamID,
		})

		// The engine rides the request context, installed by the interceptor.
		state, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).Snapshot, struct{}{})
		if err != nil {
			return err
		}

		startOffset := pending.StartOffset
		if startOffset < 0 {
			// Resolved once, here, and recorded. Left to delivery it would be a
			// reading rather than a fact, and replay would resolve it again
			// against a stream that has since moved.
			startOffset = state.GetHeadOffset()
		}

		consumerID := "workflow:" + ms.GetExecutionInfo().GetWorkflowId()
		if _, _, err := chasm.UpdateComponent(
			ctx, ref,
			func(s *stream.Stream, mctx chasm.MutableContext, offset int64) (struct{}, error) {
				return struct{}{}, s.RegisterConsumer(
					mctx, consumerID, ms.GetExecutionInfo().GetWorkflowId(), "", offset, true)
			},
			startOffset,
		); err != nil {
			return err
		}

		if _, err := wf.SubscribeToExternalStream(chasmCtx, chasmworkflow.ExternalStreamSubscription{
			StreamID:     pending.StreamID,
			CollectionID: state.GetCollectionId(),
			BucketSize:   state.GetBucketSize(),
			StartOffset:  startOffset,
			KnownHead:    state.GetHeadOffset(),
		}); err != nil {
			return err
		}

		// Recorded after the cursor exists, so a crash between them leaves no
		// event claiming a subscription that was never made.
		wf.RecordStreamSubscribed(pending.StreamID, startOffset, completedEventID)
	}
	return nil
}
