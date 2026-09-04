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
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence/serialization"
	historyi "go.temporal.io/server/service/history/interfaces"
	"google.golang.org/protobuf/proto"
)

// deliverStreamSlices hands the next range of every stream this workflow
// consumes to the task being started, and stages that range on the cursor so
// the event closing the task can record what was delivered.
//
// The log read runs with the workflow lock held. That is the price of deciding
// a range and staging it in one transaction: staged first and read after, a
// failed read would leave a range that the worker never received but that the
// completion would still record as consumed.
// streamOrigin says where a subscribed stream lives, which decides how its
// payload is read. The payload is component state now, so an external stream is
// read from its own execution and an owned one from the consumer's.
type streamOrigin struct {
	external bool
	// The name the consumer knows the stream by, which is how an owned one is
	// found on the consumer's own component.
	name string

	// Where the subscription began, and how far its completed tasks have
	// committed. Together they say exactly which offsets replay owes the
	// worker, so a re-supply that comes up short can be detected instead of
	// silently handing the workflow less than it originally saw.
	start     int64
	committed int64
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

	// A consumer that fell behind a truncating stream is told so rather than
	// handed the rest and left with a hole it cannot see. Nothing holds the
	// floor for a consumer any more, on purpose: a floor that waited for the
	// slowest reader was never released and turned every cap into a no-op.
	if cursor.Offset() < state.GetBaseOffset() {
		return 0, serviceerror.NewFailedPreconditionf(
			"stream %q was truncated past this consumer: it is at offset %d and the stream now starts at %d",
			name, cursor.Offset(), state.GetBaseOffset())
	}
	return state.GetHeadOffset(), nil
}

func readDeliverable(
	ctx context.Context,
	chasmCtx chasm.Context,
	wf *chasmworkflow.Workflow,
	namespaceID string,
	name string,
	cursor *stream.Cursor,
	from, to int64,
) ([]*streampb.StreamMessage, int64, error) {
	if to <= from {
		return nil, from, nil
	}
	w, err := readWindowFor(ctx, chasmCtx, wf, namespaceID, name, cursor.IsExternal(),
		cursor.StreamID(), from, to)
	if err != nil {
		return nil, 0, err
	}
	// The collected run is contiguous from `from`, so the byte cap recomputes
	// the same end offset the read would have reported.
	collected, _, err := stream.CollectMessages(
		w.Blobs, w.Starts, from, w.To, stream.MaxConsumeItemsPerTask, nil)
	if err != nil {
		return nil, 0, err
	}
	// Cap before converting: the byte budget applies to the run as stored, and
	// trimming decides how far the recorded range reaches.
	collected, readTo := stream.CapByBytes(collected, from, stream.MaxConsumeBytesPerTask)
	return stream.ToAPIMessages(collected), readTo, nil
}

// readWindowFor reads a range from whichever component holds it.
//
// An external stream resolves through the local shard controller, so a stream
// on a shard this host does not own is not reachable. An owned stream is read
// from the consumer's own already-loaded component, which is both cheaper and
// the only safe order: re-entering this execution through the engine while its
// task is being built would contend with the lock already held.
func readWindowFor(
	ctx context.Context,
	chasmCtx chasm.Context,
	wf *chasmworkflow.Workflow,
	namespaceID string,
	name string,
	external bool,
	streamID string,
	from, to int64,
) (stream.Window, error) {
	req := stream.WindowRequest{From: from, MaxMessages: int32(to - from)}
	if !external {
		s := wf.OwnedStream(chasmCtx, name)
		if s == nil {
			return stream.Window{To: from}, nil
		}
		return s.ReadWindow(chasmCtx, req)
	}
	return chasm.ReadComponent(ctx,
		chasm.NewComponentRef[*stream.Stream](chasm.ExecutionKey{
			NamespaceID: namespaceID,
			BusinessID:  streamID,
		}),
		(*stream.Stream).ReadWindow, req)
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
	live, _, err := deliverStreamSlices(ctx, shardContext, ms)
	return live, err
}

func deliverStreamSlices(
	ctx context.Context,
	shardContext historyi.ShardContext,
	ms historyi.MutableState,
) ([]*streampb.StreamSlice, map[string]streamOrigin, error) {
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
	namespaceID := ms.GetExecutionInfo().GetNamespaceId()

	slicesOut := make([]*streampb.StreamSlice, 0, len(names))
	addresses := make(map[string]streamOrigin, len(names))
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
			ctx, chasmCtx, wf, namespaceID, name, cursor, from, to)
		if err != nil {
			return nil, nil, err
		}

		if restaged {
			// The staged range is the one the completion will record, so the
			// worker has to be handed exactly that. A re-read that comes back
			// short would otherwise deliver less than history claims was
			// consumed, and replay would then disagree with the original run.
			if next != to {
				return nil, nil, serviceerror.NewInternalf(
					"stream %q staged range [%d,%d) re-read as [%d,%d)",
					cursor.StreamID(), from, to, from, next)
			}
		} else if err := cursor.StagePending(chasmCtx, from, next); err != nil {
			return nil, nil, err
		}

		// Attached even when empty. A task that saw nothing still has to record
		// that it saw nothing, and the slice is what the completion reads.
		slicesOut = append(slicesOut, &streampb.StreamSlice{
			StreamId:   cursor.StreamID(),
			FromOffset: from,
			ToOffset:   next,
			Messages:   messages,
		})
		addresses[cursor.StreamID()] = streamOrigin{
			external:  cursor.IsExternal(),
			name:      name,
			start:     cursor.StartOffset(),
			committed: cursor.Offset(),
		}
	}
	return slicesOut, addresses, nil
}

// Bounds on one cold replay's re-supply. History holds offsets and not payloads,
// so replaying a consumer means re-reading every range its completed tasks
// recorded, and that grows with the workflow's whole life rather than with the
// task being started. These cap one response; a consumer past them cannot be
// replayed by this path at all, which is a limit of the design rather than of
// the numbers.
const (
	maxReplayMessages = 100_000
	maxReplayBytes    = 64 << 20
)

// ownedRange names a range of a stream the consumer owns.
type ownedRange struct {
	name string
	req  stream.WindowRequest
}

// readRecordedRange re-supplies a range a completed task recorded.
//
// It runs after the execution lock is released, so the consumer's own component
// is read back through the engine like any other. Both kinds resolve through
// the local shard controller, so a stream on a shard this host does not own is
// not reachable.
func readRecordedRange(
	ctx context.Context,
	consumer definition.WorkflowKey,
	origin streamOrigin,
	streamID string,
	from, to int64,
) (stream.Window, error) {
	req := stream.WindowRequest{From: from, MaxMessages: int32(to - from)}
	if origin.external {
		return chasm.ReadComponent(ctx,
			chasm.NewComponentRef[*stream.Stream](chasm.ExecutionKey{
				NamespaceID: consumer.NamespaceID,
				BusinessID:  streamID,
			}),
			(*stream.Stream).ReadWindow, req)
	}
	return chasm.ReadComponent(ctx,
		chasm.NewComponentRef[*chasmworkflow.Workflow](chasm.ExecutionKey{
			NamespaceID: consumer.NamespaceID,
			BusinessID:  consumer.WorkflowID,
		}),
		func(wf *chasmworkflow.Workflow, cctx chasm.Context, r ownedRange) (stream.Window, error) {
			s := wf.OwnedStream(cctx, r.name)
			if s == nil {
				return stream.Window{To: r.req.From}, nil
			}
			return s.ReadWindow(cctx, r.req)
		},
		ownedRange{name: origin.name, req: req})
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
	consumer definition.WorkflowKey,
	namespaceID string,
	addresses map[string]streamOrigin,
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

	budget := replayBudget{}
	// How far the recorded ranges reach, per stream, so the coverage check
	// below can tell a complete re-supply from a short one.
	reached := make(map[string]int64, len(addresses))

	for _, event := range events {
		for _, recorded := range event.GetWorkflowTaskCompletedEventAttributes().GetStreamCursors() {
			address, ok := addresses[recorded.GetStreamId()]
			if !ok {
				// A subscription the workflow has since dropped. The range is
				// still part of its history, but nothing is consuming it now.
				continue
			}

			messages, err := replayMessagesFor(ctx, consumer, address, recorded, &budget)
			if err != nil {
				return err
			}

			if to := recorded.GetToOffset(); to > reached[recorded.GetStreamId()] {
				reached[recorded.GetStreamId()] = to
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

	return checkReplayCoverage(consumer, addresses, reached)
}

// replayBudget accumulates what one response has already committed to
// re-supplying, across every stream and every recorded range in it.
type replayBudget struct {
	messages int
	bytes    int
}

// replayMessagesFor re-reads one recorded range, or returns nothing for a range
// that recorded an empty observation.
func replayMessagesFor(
	ctx context.Context,
	consumer definition.WorkflowKey,
	address streamOrigin,
	recorded *streampb.StreamCursor,
	budget *replayBudget,
) ([]*streampb.StreamMessage, error) {
	if recorded.GetToOffset() <= recorded.GetFromOffset() {
		return nil, nil
	}

	w, err := readRecordedRange(ctx, consumer, address,
		recorded.GetStreamId(), recorded.GetFromOffset(), recorded.GetToOffset())
	if err != nil {
		return nil, replayReadError(consumer, recorded, err)
	}
	collected, _, err := stream.CollectMessages(
		w.Blobs, w.Starts,
		recorded.GetFromOffset(), recorded.GetToOffset(),
		int(recorded.GetToOffset()-recorded.GetFromOffset()), nil)
	if err != nil {
		return nil, err
	}
	messages := stream.ToAPIMessages(collected)

	budget.messages += len(messages)
	for _, m := range messages {
		budget.bytes += proto.Size(m)
	}
	// Bounded because every prior task's range is re-read into one response, so
	// a long-lived consumer's cold replay grows with its whole history. Refused
	// rather than trimmed: a short re-supply is what replay cannot survive.
	if budget.messages > maxReplayMessages || budget.bytes > maxReplayBytes {
		return nil, serviceerror.NewFailedPreconditionf(
			"replaying workflow %q needs more than %d messages or %d bytes of stream history to re-supply; "+
				"the consumed ranges cannot be re-delivered in one response",
			consumer.GetWorkflowID(), maxReplayMessages, maxReplayBytes)
	}
	return messages, nil
}

// checkReplayCoverage refuses a re-supply that stops short of what History says
// the consumer consumed.
//
// The events carried on the response are one page. A consumer whose recording
// events run past it would be handed only part of what it originally saw, and
// would then replay against fewer messages than the first run had. The cursor
// knows how far it has committed, so that is checked rather than assumed.
func checkReplayCoverage(
	consumer definition.WorkflowKey,
	addresses map[string]streamOrigin,
	reached map[string]int64,
) error {
	for streamID, address := range addresses {
		got, ok := reached[streamID]
		if !ok {
			got = address.start
		}
		if got < address.committed {
			return serviceerror.NewFailedPreconditionf(
				"workflow %q consumed stream %q through offset %d but its history page only records through %d; "+
					"re-supplying the rest needs the events beyond this page",
				consumer.GetWorkflowID(), streamID, address.committed, got)
		}
	}
	return nil
}

// replayReadError says why a range a completed task recorded can no longer be
// read. Truncation and deletion are the reachable causes, and neither is
// recoverable for this workflow: without the bytes it can never replay, and
// without replaying it can never start another task. The bare read error names
// offsets and no workflow, which is not enough to act on.
func replayReadError(
	consumer definition.WorkflowKey,
	recorded *streampb.StreamCursor,
	cause error,
) error {
	return serviceerror.NewFailedPreconditionf(
		"workflow %q cannot replay: stream %q no longer holds offsets [%d,%d) that one of its completed tasks consumed (%v)",
		consumer.GetWorkflowID(), recorded.GetStreamId(),
		recorded.GetFromOffset(), recorded.GetToOffset(), cause)
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
