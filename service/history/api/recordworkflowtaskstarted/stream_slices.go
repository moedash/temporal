package recordworkflowtaskstarted

import (
	"context"
	"errors"
	"fmt"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/proto"
)

// rangeUnavailable says a stream range this consumer depends on can no longer be
// served: the stream was truncated or deleted past it, or re-supplying it would
// exceed what one response may carry. Retrying the request would only ask the
// same stream again, so the workflow task is failed with it instead.
type rangeUnavailable struct {
	msg string
}

func (e *rangeUnavailable) Error() string {
	return e.msg
}

func unavailablef(format string, args ...any) error {
	return &rangeUnavailable{msg: fmt.Sprintf(format, args...)}
}

// unavailableIfGone tells a read failure that means the range is gone for good
// from one worth retrying. A shard that is moving or a store that timed out is
// the second kind and keeps its own error.
func unavailableIfGone(err error, format string, args ...any) error {
	var precondition *serviceerror.FailedPrecondition
	var notFound *serviceerror.NotFound
	if errors.As(err, &precondition) || errors.As(err, &notFound) {
		return unavailablef(format+": %v", append(args, err)...)
	}
	return err
}

// failTaskForStreams records why the task cannot be started, and either
// schedules the next attempt or ends the workflow.
//
// The event is what an operator sees; a bare error to matching would only make
// the task come back. One retry is worth having, because a shard that was
// moving can serve the range on the next attempt. A second failure with the
// same cause is not a race: the range is gone or the re-supply is over its
// budget, and every further attempt redoes the whole page walk, fails again,
// and grows History until the size limit terminates the workflow for an
// unrelated reason. Terminating here says what actually happened.
func failTaskForStreams(
	ms historyi.MutableState,
	workflowTask *historyi.WorkflowTaskInfo,
	cause *rangeUnavailable,
) error {
	repeated := lastFailureWasStreamRange(ms)
	if _, err := ms.AddWorkflowTaskFailedEvent(
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_STREAM_RANGE_UNAVAILABLE,
		failure.NewServerFailure(cause.Error(), true),
		consts.IdentityHistoryService,
		nil,
		"",
		"",
		"",
		0,
	); err != nil {
		return err
	}
	ms.FlushBufferedEvents()
	if repeated {
		_, err := ms.AddWorkflowExecutionTerminatedEvent(
			cause.Error(), nil, consts.IdentityHistoryService, false, nil)
		return err
	}
	return workflow.ScheduleWorkflowTask(ms)
}

// lastFailureWasStreamRange reports whether the workflow task before this one
// failed for the same reason, which is what tells a retryable hiccup from a
// range that is not coming back.
func lastFailureWasStreamRange(ms historyi.MutableState) bool {
	recorded, ok := ms.GetExecutionInfo().GetLastWorkflowTaskFailure().(*persistencespb.WorkflowExecutionInfo_LastWorkflowTaskFailureCause)
	return ok && recorded.LastWorkflowTaskFailureCause ==
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_STREAM_RANGE_UNAVAILABLE
}

// streamOrigin says where a subscribed stream lives, which decides how its
// payload is read: an external stream from its own execution, an owned one
// from the consumer's.
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

// streamOrigins describes every stream the workflow consumes from a read-only
// view of its component, for a task that is built without delivering anything
// and so has no other way of learning what a cold replay owes it.
func streamOrigins(
	ctx context.Context,
	ms historyi.MutableState,
) (map[string]streamOrigin, error) {
	if !ms.HasChasmWorkflowComponent() {
		return nil, nil
	}
	wf, chasmCtx, err := ms.ChasmWorkflowComponentReadOnly(ctx)
	if err != nil {
		return nil, err
	}
	if len(wf.StreamCursors) == 0 {
		return nil, nil
	}
	addresses := make(map[string]streamOrigin, len(wf.StreamCursors))
	for name, field := range wf.StreamCursors {
		cursor := field.Get(chasmCtx)
		addresses[cursor.StreamID()] = streamOrigin{
			external:  cursor.IsExternal(),
			name:      name,
			start:     cursor.StartOffset(),
			committed: cursor.Offset(),
		}
	}
	return addresses, nil
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
		// Reported as an unavailable range rather than a bare error, so the
		// task fails once with a cause an operator can read instead of coming
		// back from matching forever. A standby rebuilt by event-based
		// replication lands here: it applies consumed ranges into owned
		// cursors, and the stream itself does not replicate that way.
		return 0, unavailablef(
			"workflow consumes stream %q, which it neither owns nor subscribed to externally",
			name)
	}
	state, err := field.Get(chasmCtx).Snapshot(chasmCtx, struct{}{})
	if err != nil {
		return 0, err
	}

	// A consumer that fell behind a truncating stream is told so rather than
	// handed the rest and left with a hole it cannot see. The floor protects
	// an active consumer, so reaching this means the subscription was released
	// and the stream moved on.
	if cursor.Offset() < state.GetBaseOffset() {
		return 0, unavailablef(
			"stream %q was truncated past this consumer: it is at offset %d and the stream "+
				"now starts at %d", name, cursor.Offset(), state.GetBaseOffset())
	}
	return state.GetHeadOffset(), nil
}

// readDeliverable reads the next run of a stream for the task being started
// and returns it with the run id of the execution that holds the stream.
func readDeliverable(
	ctx context.Context,
	chasmCtx chasm.Context,
	wf *chasmworkflow.Workflow,
	namespaceID string,
	name string,
	cursor *stream.Cursor,
	from, to int64,
	limits stream.Limits,
) ([]*streampb.StreamRecord, int64, string, error) {
	if to <= from {
		return nil, from, "", nil
	}
	w, err := readWindowFor(ctx, chasmCtx, wf, namespaceID, name, cursor.IsExternal(),
		cursor.StreamID(), from, to)
	if err != nil {
		return nil, 0, "", unavailableIfGone(err,
			"stream %q no longer holds offsets [%d,%d) this consumer is due",
			cursor.StreamID(), from, to)
	}
	// The collected run is contiguous from `from`, so the byte cap recomputes
	// the same end offset the read would have reported.
	collected, _, err := stream.CollectRecords(
		w.Blobs, w.Starts, from, w.To, limits.MaxConsumeItemsPerTask, nil)
	if err != nil {
		return nil, 0, "", err
	}
	// Cap before converting: the byte budget applies to the run as stored, and
	// trimming decides how far the recorded range reaches.
	collected, readTo := stream.CapByBytes(collected, from, limits.MaxConsumeBytesPerTask)
	return stream.ToAPIRecords(collected), readTo, w.RunID, nil
}

// readWindowFor reads a range from whichever component holds it.
//
// Standalone external streams use the routed service client. That RPC only
// reads the source execution and never calls back into the locked consumer.
// Owned streams must use the already-loaded component: re-entering this
// execution through an RPC would contend with the lock already held.
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
	return readExternalWindow(ctx, namespaceID, streamID, from, to)
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
	workflowTask *historyi.WorkflowTaskInfo,
) ([]*streampb.StreamSlice, error) {
	live, _, err := deliverStreamSlices(ctx, shardContext, ms, workflowTask)
	return live, err
}

// deliverStreamSlices hands the next range of every stream this workflow
// consumes to the task being started, and stages that range on the cursor so
// the event closing the task can record what was delivered.
//
// The payload read runs with the workflow lock held. That is the price of
// deciding a range and staging it in one transaction: staged first and read
// after, a failed read would leave a range that the worker never received but
// that the completion would still record as consumed.
func deliverStreamSlices(
	ctx context.Context,
	shardContext historyi.ShardContext,
	ms historyi.MutableState,
	workflowTask *historyi.WorkflowTaskInfo,
) ([]*streampb.StreamSlice, map[string]streamOrigin, error) {
	// A speculative task may be thrown away, and a discarded one writes no
	// completed event, so nothing would commit the cursor. The range would then
	// be handed out again on the next task while the worker's in-memory
	// workflow had already consumed it, and live execution would see the
	// records twice where replay sees them once. A workflow with records
	// waiting gets a normal task of its own, which is where they belong.
	if workflowTask != nil && workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		return nil, nil, nil
	}
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

	limits := shardContext.GetConfig().Stream.LimitsFor(ms.GetNamespaceEntry().Name().String())
	maxItems := limits.MaxConsumeItemsPerTask
	consumer := ms.GetWorkflowKey()

	// One budget over every routed read this task makes, since a workflow may
	// consume as many streams as the subscription limit allows and the lock is
	// held throughout.
	ctx, cancelBudget := WithRoutedBudget(ctx)
	defer cancelBudget()

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
			// Clip to the frontier, which is the committed head for an owned
			// stream and the last pushed head for an external one.
			to = min(from+int64(maxItems), head)
		}

		records, next, ownerRunID, err := readDeliverable(
			ctx, chasmCtx, wf, consumer.NamespaceID, name, cursor, from, to, limits)
		if err != nil {
			return nil, nil, err
		}
		if !cursor.IsExternal() {
			ownerRunID = consumer.RunID
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
			RunId:      ownerRunID,
			FromOffset: from,
			ToOffset:   next,
			Records:    records,
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

// ownedRange names a range of a stream the consumer owns.
type ownedRange struct {
	name string
	req  stream.WindowRequest
}

// readRecordedRange re-supplies a range a completed task recorded.
//
// It runs after the execution lock is released, so the consumer's own component
// is read back through the engine. Standalone sources use the routed service
// client, since their shards can belong to a different history host.
func readRecordedRange(
	ctx context.Context,
	consumer definition.WorkflowKey,
	origin streamOrigin,
	streamID string,
	from, to int64,
) (stream.Window, error) {
	req := stream.WindowRequest{From: from, MaxMessages: int32(to - from)}
	if origin.external {
		return readExternalWindow(ctx, consumer.NamespaceID, streamID, from, to)
	}
	return chasm.ReadComponent(ctx,
		chasm.NewComponentRef[*chasmworkflow.Workflow](chasm.ExecutionKey{
			NamespaceID: consumer.NamespaceID,
			BusinessID:  consumer.WorkflowID,
			RunID:       consumer.RunID,
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

// replaySupply collects what one cold replay owes the worker: the ranges every
// completed task recorded, re-read from their streams, within one response's
// budget.
type replaySupply struct {
	ctx       context.Context
	consumer  definition.WorkflowKey
	addresses map[string]streamOrigin
	limits    stream.Limits

	records int
	bytes   int
	// How far the recorded ranges reach, per stream, so the coverage check
	// can tell a complete re-supply from a short one.
	reached map[string]int64
	slices  []*streampb.StreamSlice
}

// attachReplaySlices re-supplies the payloads for ranges that earlier workflow
// tasks recorded, keyed by the event that recorded each one.
//
// History holds offsets and never payloads, which is the property the whole
// design rests on. The cost lands here: a worker replaying from History has to
// be handed the same bytes those tasks were given, and the only place they
// exist is the stream itself. The response field alone cannot carry this,
// because it is built once per delivery while a cache miss replays every prior
// task, so each range travels with the id of the event that recorded it.
//
// The response carries one page of history. The recording events of a consumer
// that has lived longer than a page are on the pages after it, so every page is
// read here. Empty ranges are recorded too and have to travel with their
// events, which is why the walk cannot stop once the committed offset is found.
func attachReplaySlices(
	ctx context.Context,
	shardContext historyi.ShardContext,
	consumer definition.WorkflowKey,
	namespaceName string,
	addresses map[string]streamOrigin,
	pageSize int32,
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
	// re-supplying it here would hand the workflow the same records twice.
	//
	// A worker can hold a sticky queue and still have evicted the workflow, and
	// the recovery from that is the SDK's: it fails the task so the next one is
	// dispatched on the normal queue with full history, which comes back
	// through here with the slices attached. An SDK that instead refetches the
	// history through GetWorkflowExecutionHistory gets no slices, because that
	// RPC has nowhere to carry them.
	if resp.GetStickyExecutionEnabled() {
		return nil
	}

	events, err := eventsOfResponse(resp)
	if err != nil {
		return err
	}
	limits := shardContext.GetConfig().Stream.LimitsFor(namespaceName)
	supply := &replaySupply{
		ctx:       ctx,
		consumer:  consumer,
		addresses: addresses,
		limits:    limits,
		reached:   make(map[string]int64, len(addresses)),
	}
	if err := supply.collect(events); err != nil {
		return err
	}

	pages := 1
	token := resp.GetNextPageToken()
	for len(token) > 0 {
		pages++
		if pages > limits.ReplayMaxPages {
			return unavailablef(
				"replaying workflow %q needs more than %d pages of history to find the stream "+
					"ranges its tasks consumed", consumer.GetWorkflowID(), limits.ReplayMaxPages)
		}
		continuation, err := api.DeserializeHistoryToken(token)
		if err != nil {
			return err
		}
		// Read raw and decode here. The first page may have been read either
		// way, and only the raw reader accepts the token both produce.
		blobs, _, next, err := persistence.ReadFullPageRawEvents(
			ctx, shardContext.GetExecutionManager(), &persistence.ReadHistoryBranchRequest{
				BranchToken:   continuation.GetBranchToken(),
				MinEventID:    continuation.GetFirstEventId(),
				MaxEventID:    continuation.GetNextEventId(),
				PageSize:      int(pageSize),
				NextPageToken: continuation.GetPersistenceToken(),
				ShardID:       shardContext.GetShardID(),
			})
		if err != nil {
			return err
		}
		pageEvents, err := decodeEventBlobs(blobs)
		if err != nil {
			return err
		}
		if err := supply.collect(pageEvents); err != nil {
			return err
		}
		if len(next) == 0 {
			break
		}
		continuation.PersistenceToken = next
		if token, err = api.SerializeHistoryToken(continuation); err != nil {
			return err
		}
	}

	if err := supply.checkCoverage(); err != nil {
		return err
	}
	resp.StreamSlices = append(resp.StreamSlices, supply.slices...)
	return nil
}

// ReplaySlicesForQuery re-supplies the recorded ranges for a query dispatched
// straight through matching.
//
// Such a query carries the workflow's whole history to a worker that may never
// have seen the execution, exactly like a task after a cache miss, but it is
// built without RecordWorkflowTaskStarted and so never passes through the
// delivery above. The history is read from the branch rather than from a
// response, which is the only difference from the task path.
//
// The lease is held only long enough to read the cursors and the branch, and
// released before any page or stream is read, as on the task path. A range the
// stream can no longer serve is a failed precondition here rather than a failed
// task, because there is no task to fail: the query is refused and the
// workflow's next real task will fail with the cause.
func ReplaySlicesForQuery(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	workflowKey definition.WorkflowKey,
	pageSize int32,
) ([]*streampb.StreamSlice, error) {
	lease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx, nil, workflowKey, locks.PriorityHigh)
	if err != nil {
		return nil, err
	}
	ms := lease.GetMutableState()
	addresses, err := streamOrigins(ctx, ms)
	if err != nil || len(addresses) == 0 {
		lease.GetReleaseFn()(nil)
		return nil, err
	}
	branchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		lease.GetReleaseFn()(nil)
		return nil, err
	}
	nextEventID := ms.GetNextEventID()
	consumer := ms.GetWorkflowKey()
	limits := shardContext.GetConfig().Stream.LimitsFor(ms.GetNamespaceEntry().Name().String())
	lease.GetReleaseFn()(nil)

	supply := &replaySupply{
		ctx:       ctx,
		consumer:  consumer,
		addresses: addresses,
		limits:    limits,
		reached:   make(map[string]int64, len(addresses)),
	}
	var token []byte
	for pages := 1; ; pages++ {
		if pages > limits.ReplayMaxPages {
			return nil, unavailablef(
				"replaying workflow %q needs more than %d pages of history to find the stream "+
					"ranges its tasks consumed", consumer.GetWorkflowID(), limits.ReplayMaxPages)
		}
		blobs, _, next, err := persistence.ReadFullPageRawEvents(
			ctx, shardContext.GetExecutionManager(), &persistence.ReadHistoryBranchRequest{
				BranchToken:   branchToken,
				MinEventID:    common.FirstEventID,
				MaxEventID:    nextEventID,
				PageSize:      int(pageSize),
				NextPageToken: token,
				ShardID:       shardContext.GetShardID(),
			})
		if err != nil {
			return nil, err
		}
		events, err := decodeEventBlobs(blobs)
		if err != nil {
			return nil, err
		}
		if err := supply.collect(events); err != nil {
			return nil, err
		}
		if len(next) == 0 {
			break
		}
		token = next
	}
	if err := supply.checkCoverage(); err != nil {
		return nil, err
	}
	return supply.slices, nil
}

// AsRefusal turns a range that cannot be served into the error a caller with
// no task to fail should return. Any other error is handed back unchanged.
func AsRefusal(err error) error {
	var unavailable *rangeUnavailable
	if errors.As(err, &unavailable) {
		return serviceerror.NewFailedPrecondition(unavailable.Error())
	}
	return err
}

// collect re-reads every range the events on one page recorded.
func (s *replaySupply) collect(events []*historypb.HistoryEvent) error {
	for _, event := range events {
		for _, recorded := range event.GetWorkflowTaskCompletedEventAttributes().GetConsumedStreamRanges() {
			address, ok := s.addresses[recorded.GetStreamId()]
			if !ok {
				// A subscription the workflow has since dropped. The range is
				// still part of its history, but nothing is consuming it now.
				continue
			}

			records, ownerRunID, err := s.recordsFor(address, recorded)
			if err != nil {
				return err
			}

			if to := recorded.GetToOffset(); to > s.reached[recorded.GetStreamId()] {
				s.reached[recorded.GetStreamId()] = to
			}

			// Attached even when empty: the task observed nothing, and replay
			// has to reproduce that rather than infer it from an absence.
			s.slices = append(s.slices, &streampb.StreamSlice{
				StreamId:                     recorded.GetStreamId(),
				RunId:                        ownerRunID,
				FromOffset:                   recorded.GetFromOffset(),
				ToOffset:                     recorded.GetToOffset(),
				Records:                      records,
				WorkflowTaskCompletedEventId: event.GetEventId(),
			})
		}
	}
	return nil
}

// recordsFor re-reads one recorded range, or returns nothing for a range that
// recorded an empty observation. The run id is the execution holding the
// stream, which for an owned stream is the consumer itself.
func (s *replaySupply) recordsFor(
	address streamOrigin,
	recorded *streampb.StreamRange,
) ([]*streampb.StreamRecord, string, error) {
	ownerRunID := s.consumer.RunID
	if recorded.GetToOffset() <= recorded.GetFromOffset() {
		if address.external {
			ownerRunID = ""
		}
		return nil, ownerRunID, nil
	}

	w, err := readRecordedRange(s.ctx, s.consumer, address,
		recorded.GetStreamId(), recorded.GetFromOffset(), recorded.GetToOffset())
	if err != nil {
		// Truncation and deletion are the reachable causes, and neither is
		// recoverable for this workflow: without the bytes it can never
		// replay, and without replaying it can never start another task.
		return nil, "", unavailableIfGone(err,
			"workflow %q cannot replay: stream %q no longer holds offsets [%d,%d) that one "+
				"of its completed tasks consumed",
			s.consumer.GetWorkflowID(), recorded.GetStreamId(),
			recorded.GetFromOffset(), recorded.GetToOffset())
	}
	if address.external {
		ownerRunID = w.RunID
	}
	collected, _, err := stream.CollectRecords(
		w.Blobs, w.Starts,
		recorded.GetFromOffset(), recorded.GetToOffset(),
		int(recorded.GetToOffset()-recorded.GetFromOffset()), nil)
	if err != nil {
		return nil, "", err
	}
	records := stream.ToAPIRecords(collected)

	s.records += len(records)
	for _, m := range records {
		s.bytes += proto.Size(m)
	}
	// Bounded because every prior task's range is re-read into one response, so
	// a long-lived consumer's cold replay grows with its whole history. Refused
	// rather than trimmed: a short re-supply is what replay cannot survive.
	if s.records > s.limits.ReplayMaxRecords || s.bytes > s.limits.ReplayMaxBytes {
		return nil, "", unavailablef(
			"replaying workflow %q needs more than %d records or %d bytes of stream history "+
				"to re-supply; the consumed ranges cannot be re-delivered in one response",
			s.consumer.GetWorkflowID(), s.limits.ReplayMaxRecords, s.limits.ReplayMaxBytes)
	}
	return records, ownerRunID, nil
}

// checkCoverage refuses a re-supply that stops short of what the cursor says
// the consumer consumed. With every page read, a shortfall means History and
// the cursor disagree, and handing the worker less than the first run saw is
// the one outcome replay cannot survive.
func (s *replaySupply) checkCoverage() error {
	for streamID, address := range s.addresses {
		got, ok := s.reached[streamID]
		if !ok {
			got = address.start
		}
		if got < address.committed {
			return unavailablef(
				"workflow %q consumed stream %q through offset %d but its history only records "+
					"through %d", s.consumer.GetWorkflowID(), streamID, address.committed, got)
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
		//nolint:staticcheck // SA1019: still populated while the newer field rolls out.
		raw = resp.GetRawHistory()
	}
	blobs := make([]*commonpb.DataBlob, 0, len(raw))
	for _, batch := range raw {
		blobs = append(blobs, &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         batch,
		})
	}
	return decodeEventBlobs(blobs)
}

func decodeEventBlobs(blobs []*commonpb.DataBlob) ([]*historypb.HistoryEvent, error) {
	serializer := serialization.NewSerializer()
	var events []*historypb.HistoryEvent
	for _, blob := range blobs {
		batchEvents, err := serializer.DeserializeEvents(blob)
		if err != nil {
			return nil, err
		}
		events = append(events, batchEvents...)
	}
	return events, nil
}
