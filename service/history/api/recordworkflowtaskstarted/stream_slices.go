package recordworkflowtaskstarted

import (
	"context"
	"slices"

	"go.temporal.io/api/serviceerror"
	apistreampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm/lib/stream"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// deliverStreamSlices hands the next range of every stream this workflow
// consumes to the task being started, and stages that range on the cursor so
// the event closing the task can record what was delivered.
//
// The log read runs with the workflow lock held. That is the price of deciding
// a range and staging it in one transaction: staged first and read after, a
// failed read would leave a range that the worker never received but that the
// completion would still record as consumed.
func deliverStreamSlices(
	ctx context.Context,
	shardContext historyi.ShardContext,
	ms historyi.MutableState,
) ([]*apistreampb.StreamSlice, error) {
	if !ms.HasChasmWorkflowComponent() {
		return nil, nil
	}
	// Read-only first. Reaching the component mutably marks it dirty, and a
	// workflow with no subscription should not pay a node in its transaction
	// for every task it runs.
	if readOnly, _, err := ms.ChasmWorkflowComponentReadOnly(ctx); err != nil {
		return nil, err
	} else if len(readOnly.StreamCursors) == 0 {
		return nil, nil
	}

	wf, chasmCtx, err := ms.ChasmWorkflowComponent(ctx)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(wf.StreamCursors))
	for name := range wf.StreamCursors {
		names = append(names, name)
	}
	// Delivery order has to be stable, because the completion records these
	// ranges in the order they were produced.
	slices.Sort(names)

	maxItems := stream.MaxConsumeItemsPerTask
	execMgr := shardContext.GetExecutionManager()
	shardID := shardContext.GetShardID()
	namespaceID := ms.GetExecutionInfo().GetNamespaceId()

	slicesOut := make([]*apistreampb.StreamSlice, 0, len(names))
	for _, name := range names {
		cursor := wf.StreamCursors[name].Get(chasmCtx)

		// Only a stream in this execution can be read here. Reaching one owned
		// by another execution needs its frontier, and reading that from
		// inside the workflow lock is a different problem than this one.
		field, ok := wf.Streams[name]
		if !ok {
			return nil, serviceerror.NewFailedPreconditionf(
				"workflow consumes stream %q, which it does not own", name)
		}
		owned := field.Get(chasmCtx)
		state, err := owned.Snapshot(chasmCtx, struct{}{})
		if err != nil {
			return nil, err
		}

		// A range already staged is redelivered unchanged. The same task can be
		// started more than once, and letting the second attempt pick up newer
		// data would hand the workflow a different range than the one the
		// completion is going to record.
		from, to, restaged := cursor.Pending()
		if !restaged {
			from = cursor.Offset()
			// Clip to the frontier. Bytes reach the log before the transaction
			// that makes them visible commits, so reading past head risks
			// delivering an offset whose content a retry could still replace.
			to = min(from+int64(maxItems), state.GetHeadOffset())
		}

		var messages []*apistreampb.StreamMessage
		next := from
		if to > from {
			blobs, startOffsets, err := stream.ReadRange(
				ctx, execMgr, shardID, namespaceID,
				cursor.CollectionID(), cursor.BucketSize(), from, to, 0)
			if err != nil {
				return nil, err
			}
			collected, readTo, err := stream.CollectMessages(blobs, startOffsets, from, to, maxItems, nil)
			if err != nil {
				return nil, err
			}
			messages = stream.ToAPIMessages(collected)
			next = readTo
		}

		if !restaged {
			if err := cursor.StagePending(chasmCtx, from, next); err != nil {
				return nil, err
			}
		}

		// Attached even when empty. A task that saw nothing still has to record
		// that it saw nothing, and the slice is what the completion reads.
		slicesOut = append(slicesOut, &apistreampb.StreamSlice{
			StreamId:   cursor.StreamID(),
			FromOffset: from,
			ToOffset:   next,
			Messages:   messages,
		})
	}
	return slicesOut, nil
}
