package recordworkflowtaskstarted

import (
	"context"
	"errors"
	"fmt"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
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
	ctx, cancelBudget := chasmworkflow.WithRoutedBudget(ctx)
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
