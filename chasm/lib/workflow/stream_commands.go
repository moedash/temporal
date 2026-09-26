package workflow

import (
	"errors"
	"fmt"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// DefaultStreamName is the stream a command addresses when it names none.
const DefaultStreamName = "output"

// StreamAdmissionFailure turns a refusal of a stream command into a workflow
// task failure with the given cause.
//
// A refusal returned as a plain error fails the RespondWorkflowTaskCompleted
// call instead, and the worker then retries the same commands against the same
// limits until the task times out, with nothing in History saying why. Only
// refusals are converted: an internal or storage error is still the request's
// to report, because a retry can succeed.
func StreamAdmissionFailure(cause enumspb.WorkflowTaskFailedCause, err error) error {
	var invalid *serviceerror.InvalidArgument
	var precondition *serviceerror.FailedPrecondition
	var exhausted *serviceerror.ResourceExhausted
	var notFound *serviceerror.NotFound
	if errors.As(err, &invalid) || errors.As(err, &precondition) ||
		errors.As(err, &exhausted) || errors.As(err, &notFound) {
		return FailWorkflowTaskError{Cause: cause, Message: err.Error()}
	}
	return err
}

// handleAppendStreamRecordsCommand appends to a stream the workflow owns.
//
// The stream is a co-located subcomponent, so the batch and the frontier land
// in the workflow task's own commit: no extra transition, no cross-execution
// write, and a task that fails takes the publish with it.
//
// The offsets are known here, unlike a subscription's, so the event is written
// here too rather than reserved and filled in later.
func handleAppendStreamRecordsCommand(
	chasmCtx chasm.MutableContext,
	wf *Workflow,
	validator Validator,
	command *commandpb.Command,
	opts CommandHandlerOptions,
	limits stream.Limits,
) error {
	badAttributes := enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_APPEND_STREAM_RECORDS_ATTRIBUTES
	attrs := command.GetAppendStreamRecordsCommandAttributes()
	if attrs == nil {
		return FailWorkflowTaskError{
			Cause: badAttributes, Message: "AppendStreamRecordsCommandAttributes is not set",
		}
	}
	if len(attrs.GetRecords()) == 0 {
		return FailWorkflowTaskError{
			Cause: badAttributes, Message: "AppendStreamRecords command carries no records",
		}
	}

	// The batch becomes one data node, so the whole batch is what has to fit.
	// Left unchecked it fails later in the flush, which surfaces as a
	// persistence error out of a task the worker will replay and re-issue
	// forever, with nothing naming the batch as the cause.
	size := 0
	for _, m := range attrs.GetRecords() {
		size += m.Size()
	}
	if !validator.IsValidPayloadSize(size) {
		return FailWorkflowTaskError{
			Cause:             enumspb.WORKFLOW_TASK_FAILED_CAUSE_PAYLOADS_TOO_LARGE,
			Message:           "AppendStreamRecordsCommandAttributes.Records exceeds size limit",
			TerminateWorkflow: true,
		}
	}

	name := attrs.GetStreamName()
	if name == "" {
		name = DefaultStreamName
	}

	s, err := wf.streamNamed(chasmCtx, name, limits)
	if err != nil {
		return StreamAdmissionFailure(badAttributes, err)
	}

	result, err := s.AddMessages(chasmCtx, stream.AddMessagesRequest{
		Records: toLibraryRecords(attrs.GetRecords()),
		Limits:  limits,
		// Every stream this execution owns shares one byte budget, because
		// their per-stream budgets multiplied out come to more than the
		// execution size limit, which terminates the workflow rather than
		// refusing an append.
		SiblingBytes: wf.siblingStreamBytes(chasmCtx, name),
	})
	if err != nil {
		return StreamAdmissionFailure(badAttributes, err)
	}
	wf.RecordStreamRecordsAppended(
		name, result.FirstOffset, result.NextOffset, opts.WorkflowTaskCompletedEventID)
	return nil
}

// handleSubscribeStreamCommand registers this workflow as a consumer.
//
// Nothing is registered here. A stream in another execution has to be pinned
// on its own shard, which a command handler cannot reach while holding the
// state lock, so the subscription is staged and resolved after the commands and
// before the commit.
func handleSubscribeStreamCommand(
	chasmCtx chasm.MutableContext,
	wf *Workflow,
	_ Validator,
	command *commandpb.Command,
	opts CommandHandlerOptions,
	limits stream.Limits,
) error {
	badAttributes := enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SUBSCRIBE_STREAM_ATTRIBUTES
	attrs := command.GetSubscribeStreamCommandAttributes()
	if attrs == nil {
		return FailWorkflowTaskError{
			Cause: badAttributes, Message: "SubscribeStreamCommandAttributes is not set",
		}
	}
	nameOrID := attrs.GetStreamNameOrId()
	if nameOrID == "" {
		return FailWorkflowTaskError{
			Cause: badAttributes, Message: "SubscribeStream command names no stream",
		}
	}

	// A second subscribe to the same stream registers nothing, but it still
	// gets an event. Every SDK matches issued commands against
	// command-generated events in order, so a command that produces none puts
	// that matching out of step, which is the whole reason this event exists.
	_, already := wf.StreamCursors[nameOrID]

	// Each new subscription costs a routed call on the completion path, made
	// with this execution's lock held, and each delivery costs another on
	// every task start. Bounded here, because nothing else bounds how many a
	// workflow may hold or how many one task may carry.
	if subscribed, known := wf.subscribedStreams(nameOrID); !known &&
		subscribed >= limits.MaxSubscriptionsPerWorkflow {
		return FailWorkflowTaskError{
			Cause: badAttributes,
			Message: fmt.Sprintf("workflow already subscribes to %d streams, the limit",
				limits.MaxSubscriptionsPerWorkflow),
		}
	}

	// Everything is staged, including a stream this workflow owns, so that the
	// resolved start offset and the event recording it are produced in one
	// place rather than two.
	wf.StagePendingSubscription(PendingStreamSubscription{
		StreamID:          nameOrID,
		StartOffset:       attrs.GetStartOffset(),
		AlreadySubscribed: already,
		Event: wf.ReserveStreamSubscribedEvent(
			nameOrID, opts.WorkflowTaskCompletedEventID),
	})
	return nil
}

// streamSubscribedEvent is the event a subscription writes.
//
// It is recorded once per subscription, not per record: the offsets a task
// consumed ride WorkflowTaskCompleted, and payloads never enter History. The
// event exists because a command that produces none desynchronises the
// command-to-event matching every SDK's replay depends on, and because without
// it nothing in History explains why a workflow started receiving stream data.
type streamSubscribedEvent struct{}

func (streamSubscribedEvent) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_WORKFLOW_STREAM_SUBSCRIBED
}

func (streamSubscribedEvent) IsWorkflowTaskTrigger() bool { return false }

// Apply recreates the cursor for a run rebuilt from its history, which is how a
// reset run comes to exist. The event records the stream and where reading
// began; the ranges consumed since are folded in as the completed events that
// carry them are applied. Whether the stream lives in this execution or in
// another is not in the event, so the rebuilt cursor is owned until the reset
// copies that from the run it was rebuilt from.
func (streamSubscribedEvent) Apply(
	mctx chasm.MutableContext,
	wf *Workflow,
	event *historypb.HistoryEvent,
) error {
	attrs := event.GetWorkflowStreamSubscribedEventAttributes()
	if _, ok := wf.StreamCursors[attrs.GetStreamId()]; ok {
		return nil
	}
	cursor, err := stream.NewCursor(mctx, stream.NewCursorRequest{
		StreamID:    attrs.GetStreamId(),
		StartOffset: attrs.GetStartOffset(),
	})
	if err != nil {
		return err
	}
	if wf.StreamCursors == nil {
		wf.StreamCursors = make(chasm.Map[string, *stream.Cursor])
	}
	wf.StreamCursors[attrs.GetStreamId()] = chasm.NewComponentField(mctx, cursor)
	return nil
}

// A command event, so it is never cherry-picked: the workflow reissues the
// subscribe command on the new branch if it still wants one.
func (streamSubscribedEvent) CherryPick(
	chasm.MutableContext,
	*Workflow,
	*historypb.HistoryEvent,
	map[enumspb.ResetReapplyExcludeType]struct{},
) error {
	return ErrEventNotCherryPickable
}

// ReserveStreamSubscribedEvent writes the event a subscribe command owes,
// leaving the start offset for the flush to fill in.
//
// Written here rather than where the offset becomes known, because an SDK
// matches the commands a workflow issued against the events they produced by
// position. The flush runs after every command, so an event written there
// would sit behind the events of commands that were issued later, and the
// first replay of a workflow that subscribed before doing anything else would
// fail on the mismatch.
func (w *Workflow) ReserveStreamSubscribedEvent(
	streamID string,
	workflowTaskCompletedEventID int64,
) *historypb.HistoryEvent {
	eventType := enumspb.EVENT_TYPE_WORKFLOW_STREAM_SUBSCRIBED
	return w.AddHistoryEvent(eventType, func(e *historypb.HistoryEvent) {
		attrs := &historypb.WorkflowStreamSubscribedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			StreamId:                     streamID,
		}
		e.Attributes = &historypb.HistoryEvent_WorkflowStreamSubscribedEventAttributes{
			WorkflowStreamSubscribedEventAttributes: attrs,
		}
	})
}

// RecordStreamSubscribedOffset completes a reserved event. Safe to do after the
// fact because the builder serializes the batch at commit, which is after the
// flush that resolves the offset.
func RecordStreamSubscribedOffset(event *historypb.HistoryEvent, startOffset int64) {
	event.GetWorkflowStreamSubscribedEventAttributes().StartOffset = startOffset
}

// streamRecordsAppendedEvent is the event a publish writes.
//
// One per batch, holding the offset range and nothing else. That is what makes
// it a fixed cost: a batch of one 20-byte record and a batch of a thousand
// 2KB records write the same event, because the bodies stay in the stream
// component. It exists for the same reason the subscription event does, that a
// command producing no event desynchronises the command-to-event matching
// every SDK's replay depends on, and it doubles as the only record in History
// that the workflow published at all.
type streamRecordsAppendedEvent struct{}

func (streamRecordsAppendedEvent) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_WORKFLOW_STREAM_RECORDS_APPENDED
}

func (streamRecordsAppendedEvent) IsWorkflowTaskTrigger() bool { return false }

// The frontier it describes is CHASM state, persisted and rebuilt with the
// execution, so there is nothing here to reconstruct.
func (streamRecordsAppendedEvent) Apply(
	chasm.MutableContext, *Workflow, *historypb.HistoryEvent,
) error {
	return nil
}

// A command event, so it is never cherry-picked: the offsets belong to a log
// the new branch did not write.
func (streamRecordsAppendedEvent) CherryPick(
	chasm.MutableContext,
	*Workflow,
	*historypb.HistoryEvent,
	map[enumspb.ResetReapplyExcludeType]struct{},
) error {
	return ErrEventNotCherryPickable
}

// RecordStreamRecordsAppended writes the event for one published batch.
func (w *Workflow) RecordStreamRecordsAppended(
	streamID string,
	fromOffset int64,
	toOffset int64,
	workflowTaskCompletedEventID int64,
) {
	eventType := enumspb.EVENT_TYPE_WORKFLOW_STREAM_RECORDS_APPENDED
	w.AddHistoryEvent(eventType, func(e *historypb.HistoryEvent) {
		attrs := &historypb.WorkflowStreamRecordsAppendedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			StreamId:                     streamID,
			FromOffset:                   fromOffset,
			ToOffset:                     toOffset,
		}
		e.Attributes = &historypb.HistoryEvent_WorkflowStreamRecordsAppendedEventAttributes{
			WorkflowStreamRecordsAppendedEventAttributes: attrs,
		}
	})
}

// streamNamed returns the workflow's stream of that name, creating it on first
// use. Implicit creation is deliberate: a workflow publishing to its own output
// should not have to coordinate with anyone about who creates it.
//
// The stream belongs to this run. After a reset the new run publishes to and
// subscribes on streams of its own, created here on first use, and the run it
// was reset from keeps the records its own history refers to. A stream the
// reset run inherited a subscription to is created by the reset itself, at the
// offset that subscription stood at, so it is already here by the time a
// command names it.
func (w *Workflow) streamNamed(
	ctx chasm.MutableContext,
	name string,
	limits stream.Limits,
) (*stream.Stream, error) {
	if w.Streams == nil {
		w.Streams = make(chasm.Map[string, *stream.Stream])
	}
	if field, ok := w.Streams[name]; ok {
		return field.Get(ctx), nil
	}

	// Checked only on the create path, so an existing stream is never refused
	// for room. The name arrives from the caller and every distinct one adds a
	// component to this execution's mutable state, so without a bound an
	// outside writer can grow that state until the size limit terminates the
	// workflow.
	if len(name) > stream.MaxStreamNameLength {
		return nil, serviceerror.NewInvalidArgumentf(
			"stream name is %d characters, over the %d limit", len(name), stream.MaxStreamNameLength)
	}
	if len(w.Streams) >= limits.MaxOwnedStreamsPerWorkflow {
		return nil, serviceerror.NewFailedPreconditionf(
			"workflow already owns %d streams, the limit", limits.MaxOwnedStreamsPerWorkflow)
	}

	// Budgeted, because the batches live in this execution's mutable state and
	// the size limit on that terminates the workflow instead of refusing.
	created, err := stream.NewStream(ctx, stream.NewStreamRequest{
		Attached: true,
		Budget: &streamlib.StreamBudget{
			MaxItems: int64(limits.OwnedStreamMaxItems),
			MaxBytes: int64(limits.OwnedStreamMaxBytes),
		},
	})
	if err != nil {
		return nil, err
	}
	w.Streams[name] = chasm.NewComponentField(ctx, created)
	return created, nil
}

// toLibraryRecords shapes a command's records for the store.
//
// The producer id is cleared rather than copied: an empty id is how a reader
// tells the owning workflow's records from an outside producer's, and only this
// path writes on the workflow's behalf. The kind is left as sent, since the
// store settles an unspecified one on the copy it serializes.
func toLibraryRecords(in []*streampb.StreamRecord) []*streamlib.StreamRecord {
	out := make([]*streamlib.StreamRecord, len(in))
	for i, m := range in {
		out[i] = &streamlib.StreamRecord{
			Body:       m.GetBody(),
			Metadata:   m.GetMetadata(),
			Topic:      m.GetTopic(),
			Kind:       m.GetKind(),
			ProducerId: "",
			Attempt:    m.GetAttempt(),
			Sequence:   m.GetSequence(),
		}
	}
	return out
}

// streamLibrary registers the stream commands with the workflow registry.
type streamLibrary struct {
	config *stream.Config
}

func newStreamLibrary(config *stream.Config) *streamLibrary {
	return &streamLibrary{config: config}
}

func (l *streamLibrary) CommandHandlers() map[enumspb.CommandType]CommandHandler {
	return map[enumspb.CommandType]CommandHandler{
		enumspb.COMMAND_TYPE_APPEND_STREAM_RECORDS: func(
			chasmCtx chasm.MutableContext,
			wf *Workflow,
			validator Validator,
			command *commandpb.Command,
			opts CommandHandlerOptions,
		) error {
			namespaceName := chasmCtx.NamespaceEntry().Name().String()
			if err := l.checkEnabled(
				namespaceName,
				enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_APPEND_STREAM_RECORDS_ATTRIBUTES,
			); err != nil {
				return err
			}
			limits := l.config.LimitsFor(namespaceName)
			return handleAppendStreamRecordsCommand(chasmCtx, wf, validator, command, opts, limits)
		},
		enumspb.COMMAND_TYPE_SUBSCRIBE_STREAM: func(
			chasmCtx chasm.MutableContext,
			wf *Workflow,
			validator Validator,
			command *commandpb.Command,
			opts CommandHandlerOptions,
		) error {
			namespaceName := chasmCtx.NamespaceEntry().Name().String()
			if err := l.checkEnabled(
				namespaceName,
				enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SUBSCRIBE_STREAM_ATTRIBUTES,
			); err != nil {
				return err
			}
			limits := l.config.LimitsFor(namespaceName)
			return handleSubscribeStreamCommand(chasmCtx, wf, validator, command, opts, limits)
		},
	}
}

// checkEnabled fails the workflow task when streams are off for the namespace.
//
// The command path does not go through the stream service, so the frontend
// gate does not cover it. Failed with a cause rather than returned raw, so the
// worker sees why instead of reissuing the same command until the task times
// out.
func (l *streamLibrary) checkEnabled(
	namespaceName string,
	cause enumspb.WorkflowTaskFailedCause,
) error {
	if l.config.EnabledFor(namespaceName) {
		return nil
	}
	return FailWorkflowTaskError{
		Cause:   cause,
		Message: "streams are not enabled for namespace: " + namespaceName,
	}
}

func (l *streamLibrary) EventDefinitions() []EventDefinition {
	return []EventDefinition{streamSubscribedEvent{}, streamRecordsAppendedEvent{}}
}

// subscribedStreams counts the distinct streams this workflow consumes or is
// about to, and says whether the given one is among them. Staged subscriptions
// count: they are resolved before the commit, so a single task can otherwise
// stage as many as it likes.
func (w *Workflow) subscribedStreams(streamID string) (int, bool) {
	seen := make(map[string]struct{}, len(w.StreamCursors)+len(w.pendingStreamSubscriptions))
	for name := range w.StreamCursors {
		seen[name] = struct{}{}
	}
	for _, pending := range w.pendingStreamSubscriptions {
		seen[pending.StreamID] = struct{}{}
	}
	_, known := seen[streamID]
	return len(seen), known
}

// siblingStreamBytes is what every other stream this workflow owns holds.
//
// The per-stream budget bounds one stream, and an outside writer can name as
// many as MaxOwnedStreamsPerWorkflow allows. Their sum is what the execution
// size limit sees, so the append has to be measured against the sum.
//
// Skipped for the common case of a single stream, where the sum is the stream
// itself and reading the others would load state nothing else needs.
func (w *Workflow) siblingStreamBytes(ctx chasm.Context, name string) int64 {
	if len(w.Streams) < 2 {
		return 0
	}
	var total int64
	for other, field := range w.Streams {
		if other == name {
			continue
		}
		total += field.Get(ctx).State.GetAppendedBytes()
	}
	return total
}
