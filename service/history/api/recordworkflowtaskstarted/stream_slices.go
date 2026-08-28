package recordworkflowtaskstarted

import (
	"context"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
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
	// The shard the log lives on, which is the stream's own, not the
	// consumer's. They differ whenever the stream is in another execution.
	shardID int32
}

// logShardID resolves the shard holding a stream's log. History nodes are
// stored per shard, so reading an external stream from the consumer's shard
// finds nothing at all rather than failing loudly.
func logShardID(
	shardContext historyi.ShardContext,
	namespaceID string,
	cursor *stream.Cursor,
) int32 {
	if !cursor.IsExternal() {
		return shardContext.GetShardID()
	}
	return shardContext.GetConfig().GetShardID(namespace.ID(namespaceID), cursor.StreamID())
}

// deliveryFrontier is the offset a delivery clips to. For a stream this
// execution owns it is read directly; for one in another execution it is
// whatever that stream last pushed here, because reading the real value would
// mean reaching across executions while this transaction is open.
func deliveryFrontier(
	chasmCtx chasm.Context,
	wf *chasmworkflow.Workflow,
	name string,
	cursor *stream.Cursor,
) (int64, error) {
	if cursor.IsExternal() {
		return cursor.KnownHead(), nil
	}
	field, ok := wf.Streams[name]
	if !ok {
		return 0, serviceerror.NewFailedPreconditionf(
			"workflow consumes stream %q, which it neither owns nor subscribed to externally", name)
	}
	state, err := field.Get(chasmCtx).Snapshot(chasmCtx, struct{}{})
	if err != nil {
		return 0, err
	}
	return state.GetHeadOffset(), nil
}

// readDeliverable reads [from, to) from the stream's log and returns the
// messages along with the offset the range actually reaches, which the byte cap
// can pull back short of `to`.
func readDeliverable(
	ctx context.Context,
	execMgr persistence.ExecutionManager,
	shardID int32,
	namespaceID string,
	cursor *stream.Cursor,
	from, to int64,
) ([]*streampb.StreamMessage, int64, error) {
	if to <= from {
		return nil, from, nil
	}
	blobs, startOffsets, err := stream.ReadRange(
		ctx, execMgr, shardID, namespaceID,
		cursor.CollectionID(), cursor.BucketSize(), from, to, 0)
	if err != nil {
		return nil, 0, err
	}
	// The collected run is contiguous from `from`, so the byte cap recomputes
	// the same end offset the read would have reported.
	collected, _, err := stream.CollectMessages(
		blobs, startOffsets, from, to, stream.MaxConsumeItemsPerTask, nil)
	if err != nil {
		return nil, 0, err
	}
	// Cap before converting: the byte budget applies to the run as stored, and
	// trimming decides how far the recorded range reaches.
	collected, readTo := stream.CapByBytes(collected, from, stream.MaxConsumeBytesPerTask)
	return stream.ToAPIMessages(collected), readTo, nil
}

// DeliverStreamSlices hands the next range to a task built outside this
// package. The inline task returned by RespondWorkflowTaskCompleted is built by
// its own handler, so without this a subscribed workflow gets no data on the
// dispatch path every current SDK asks for.
//
// Only the live range. That task is always sticky, so the worker still holds
// the execution and has no ranges to replay.
func DeliverStreamSlices(
	ctx context.Context,
	shardContext historyi.ShardContext,
	ms historyi.MutableState,
) ([]*streampb.StreamSlice, error) {
	slices, _, err := deliverStreamSlices(ctx, shardContext, ms)
	return slices, err
}

func deliverStreamSlices(
	ctx context.Context,
	shardContext historyi.ShardContext,
	ms historyi.MutableState,
) ([]*streampb.StreamSlice, map[string]streamAddress, error) {
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
	namespaceID := ms.GetExecutionInfo().GetNamespaceId()

	slicesOut := make([]*streampb.StreamSlice, 0, len(names))
	addresses := make(map[string]streamAddress, len(names))
	for _, name := range names {
		cursor := wf.StreamCursors[name].Get(chasmCtx)

		head, err := deliveryFrontier(chasmCtx, wf, name, cursor)
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
			to = min(from+int64(maxItems), head)
		}

		messages, next, err := readDeliverable(
			ctx, execMgr, logShardID(shardContext, namespaceID, cursor), namespaceID, cursor, from, to)
		if err != nil {
			return nil, nil, err
		}

		if !restaged {
			if err := cursor.StagePending(chasmCtx, from, next); err != nil {
				return nil, nil, err
			}
		}

		// Attached even when empty. A task that saw nothing still has to record
		// that it saw nothing, and the slice is what the completion reads.
		slicesOut = append(slicesOut, &streampb.StreamSlice{
			StreamId:   cursor.StreamID(),
			FromOffset: from,
			ToOffset:   next,
			Messages:   messages,
		})
		addresses[cursor.StreamID()] = streamAddress{
			collectionID: cursor.CollectionID(),
			bucketSize:   cursor.BucketSize(),
			shardID:      logShardID(shardContext, namespaceID, cursor),
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

	// A sticky task means the worker still holds the execution, so it has
	// nothing to replay. Its history begins at the previous task's completion,
	// and that event carries the range that task already consumed, so
	// re-supplying it here would hand the workflow the same messages twice.
	if resp.GetStickyExecutionEnabled() {
		return nil
	}

	events, err := eventsOfResponse(resp)
	if err != nil {
		return err
	}

	execMgr := shardContext.GetExecutionManager()

	for _, event := range events {
		for _, recorded := range event.GetWorkflowTaskCompletedEventAttributes().GetStreamCursors() {
			address, ok := addresses[recorded.GetStreamId()]
			if !ok {
				// A subscription the workflow has since dropped. The range is
				// still part of its history, but nothing is consuming it now.
				continue
			}

			var messages []*streampb.StreamMessage
			if recorded.GetToOffset() > recorded.GetFromOffset() {
				blobs, startOffsets, err := stream.ReadRange(
					ctx, execMgr, address.shardID, namespaceID,
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
			resp.StreamSlices = append(resp.StreamSlices, &streampb.StreamSlice{
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
