package recordworkflowtaskstarted

import (
	"context"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	apistreampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm/lib/stream"
	"go.temporal.io/server/common/persistence/serialization"
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
// streamAddress is everything needed to read a stream's log without loading
// the stream component: buckets derive from the collection id arithmetically.
type streamAddress struct {
	collectionID string
	bucketSize   int64
}

func deliverStreamSlices(
	ctx context.Context,
	shardContext historyi.ShardContext,
	ms historyi.MutableState,
) ([]*apistreampb.StreamSlice, map[string]streamAddress, error) {
	if !ms.HasChasmWorkflowComponent() {
		return nil, nil, nil
	}
	// Read-only first. Reaching the component mutably marks it dirty, and a
	// workflow with no subscription should not pay a node in its transaction
	// for every task it runs.
	if readOnly, _, err := ms.ChasmWorkflowComponentReadOnly(ctx); err != nil {
		return nil, nil, err
	} else if len(readOnly.StreamCursors) == 0 {
		return nil, nil, nil
	}

	wf, chasmCtx, err := ms.ChasmWorkflowComponent(ctx)
	if err != nil {
		return nil, nil, err
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
	addresses := make(map[string]streamAddress, len(names))
	for _, name := range names {
		cursor := wf.StreamCursors[name].Get(chasmCtx)

		// Only a stream in this execution can be read here. Reaching one owned
		// by another execution needs its frontier, and reading that from
		// inside the workflow lock is a different problem than this one.
		field, ok := wf.Streams[name]
		if !ok {
			return nil, nil, serviceerror.NewFailedPreconditionf(
				"workflow consumes stream %q, which it does not own", name)
		}
		owned := field.Get(chasmCtx)
		state, err := owned.Snapshot(chasmCtx, struct{}{})
		if err != nil {
			return nil, nil, err
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
				return nil, nil, err
			}
			// The collected run is contiguous from `from`, so the byte cap
			// recomputes the same end offset the read would have reported.
			collected, _, err := stream.CollectMessages(blobs, startOffsets, from, to, maxItems, nil)
			if err != nil {
				return nil, nil, err
			}
			// Cap before converting: the byte budget applies to the run as
			// stored, and trimming decides how far the recorded range reaches.
			collected, readTo := stream.CapByBytes(collected, from, stream.MaxConsumeBytesPerTask)
			messages = stream.ToAPIMessages(collected)
			next = readTo
		}

		if !restaged {
			if err := cursor.StagePending(chasmCtx, from, next); err != nil {
				return nil, nil, err
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
		addresses[cursor.StreamID()] = streamAddress{
			collectionID: cursor.CollectionID(),
			bucketSize:   cursor.BucketSize(),
		}
	}
	return slicesOut, addresses, nil
}

// attachReplaySlices re-supplies the payloads for ranges that earlier workflow
// tasks recorded, keyed by the event that recorded each one.
//
// History holds offsets and never payloads, which is the property the whole
// design rests on. The cost lands here: a worker replaying from History has to
// be handed the same bytes those tasks were given, and the only place they
// exist is the stream's log. The response field alone cannot carry this,
// because it is built once per delivery while a cache miss replays every prior
// task, so each range travels with the id of the event that recorded it.
func attachReplaySlices(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID string,
	addresses map[string]streamAddress,
	resp *historyservice.RecordWorkflowTaskStartedResponseWithRawHistory,
) error {
	// Only a workflow with a live subscription has anything to re-supply, and
	// that is exactly when delivery attached a slice for the current task. So
	// this costs nothing for every other workflow.
	if len(addresses) == 0 {
		return nil
	}

	events, err := eventsOfResponse(resp)
	if err != nil {
		return err
	}

	execMgr := shardContext.GetExecutionManager()
	shardID := shardContext.GetShardID()

	for _, event := range events {
		for _, recorded := range event.GetWorkflowTaskCompletedEventAttributes().GetStreamCursors() {
			address, ok := addresses[recorded.GetStreamId()]
			if !ok {
				// A subscription the workflow has since dropped. The range is
				// still part of its history, but nothing is consuming it now.
				continue
			}

			var messages []*apistreampb.StreamMessage
			if recorded.GetToOffset() > recorded.GetFromOffset() {
				blobs, startOffsets, err := stream.ReadRange(
					ctx, execMgr, shardID, namespaceID,
					address.collectionID, address.bucketSize,
					recorded.GetFromOffset(), recorded.GetToOffset(), 0)
				if err != nil {
					return err
				}
				collected, _, err := stream.CollectMessages(
					blobs, startOffsets,
					recorded.GetFromOffset(), recorded.GetToOffset(),
					int(recorded.GetToOffset()-recorded.GetFromOffset()), nil)
				if err != nil {
					return err
				}
				messages = stream.ToAPIMessages(collected)
			}

			// Attached even when empty: the task observed nothing, and replay
			// has to reproduce that rather than infer it from an absence.
			resp.StreamSlices = append(resp.StreamSlices, &apistreampb.StreamSlice{
				StreamId:                     recorded.GetStreamId(),
				FromOffset:                   recorded.GetFromOffset(),
				ToOffset:                     recorded.GetToOffset(),
				Messages:                     messages,
				WorkflowTaskCompletedEventId: event.GetEventId(),
			})
		}
	}
	return nil
}

// eventsOfResponse reads the events the response is carrying, whichever of the
// three representations it happens to be using.
func eventsOfResponse(
	resp *historyservice.RecordWorkflowTaskStartedResponseWithRawHistory,
) ([]*historypb.HistoryEvent, error) {
	if resp.GetHistory() != nil {
		return resp.GetHistory().GetEvents(), nil
	}

	raw := resp.GetRawHistoryBytes()
	if len(raw) == 0 {
		raw = resp.GetRawHistory() //nolint:staticcheck // SA1019: still populated while the newer field rolls out.
	}
	if len(raw) == 0 {
		return nil, nil
	}

	serializer := serialization.NewSerializer()
	var events []*historypb.HistoryEvent
	for _, batch := range raw {
		batchEvents, err := serializer.DeserializeEvents(&commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         batch,
		})
		if err != nil {
			return nil, err
		}
		events = append(events, batchEvents...)
	}
	return events, nil
}
