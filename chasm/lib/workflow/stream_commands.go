package workflow

import (
	"errors"

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

// handleAddStreamMessagesCommand appends to a stream the workflow owns.
//
// The stream is a co-located subcomponent, so the batch and the frontier land
// in the workflow task's own commit: no extra transition, no cross-execution
// write, and a task that fails takes the publish with it.
//
// The offsets are known here, unlike a subscription's, so the event is written
// here too rather than reserved and filled in later.
func handleAddStreamMessagesCommand(
	chasmCtx chasm.MutableContext,
	wf *Workflow,
	validator Validator,
	command *commandpb.Command,
	opts CommandHandlerOptions,
	limits stream.Limits,
) error {
	badAttributes := enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_ADD_STREAM_MESSAGES_ATTRIBUTES
	attrs := command.GetAddStreamMessagesCommandAttributes()
	if attrs == nil {
		return FailWorkflowTaskError{
			Cause: badAttributes, Message: "AddStreamMessagesCommandAttributes is not set",
		}
	}
	if len(attrs.GetMessages()) == 0 {
		return FailWorkflowTaskError{
			Cause: badAttributes, Message: "AddStreamMessages command carries no messages",
		}
	}

	// The batch becomes one data node, so the whole batch is what has to fit.
	// Left unchecked it fails later in the flush, which surfaces as a
	// persistence error out of a task the worker will replay and re-issue
	// forever, with nothing naming the batch as the cause.
	size := 0
	for _, m := range attrs.GetMessages() {
		size += m.Size()
	}
	if !validator.IsValidPayloadSize(size) {
		return FailWorkflowTaskError{
			Cause:             enumspb.WORKFLOW_TASK_FAILED_CAUSE_PAYLOADS_TOO_LARGE,
			Message:           "AddStreamMessagesCommandAttributes.Messages exceeds size limit",
			TerminateWorkflow: true,
		}
	}

	name := attrs.GetStreamId()
	if name == "" {
		name = DefaultStreamName
	}

	s, err := wf.streamNamed(chasmCtx, name, limits)
	if err != nil {
		return StreamAdmissionFailure(badAttributes, err)
	}

	result, err := s.AddMessages(chasmCtx, stream.AddMessagesRequest{
		Messages: toLibraryMessages(attrs.GetMessages()),
		Limits:   limits,
	})
	if err != nil {
		return StreamAdmissionFailure(badAttributes, err)
	}
	// Written even when a producer sequence deduplicated the append, because
	// the command was still issued and the event is what the replaying worker
	// matches it against. It names the original offsets, which is what a
	// deduplicated append resolves to.
	wf.RecordStreamMessagesAdded(
		name, result.FirstOffset, result.Count, opts.WorkflowTaskCompletedEventID)
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
) error {
	badAttributes := enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SUBSCRIBE_STREAM_ATTRIBUTES
	attrs := command.GetSubscribeStreamCommandAttributes()
	if attrs == nil {
		return FailWorkflowTaskError{
			Cause: badAttributes, Message: "SubscribeStreamCommandAttributes is not set",
		}
	}
	streamID := attrs.GetStreamId()
	if streamID == "" {
		return FailWorkflowTaskError{
			Cause: badAttributes, Message: "SubscribeStream command names no stream",
		}
	}

	// A second subscribe to the same stream registers nothing, but it still
	// gets an event. Every SDK matches issued commands against
	// command-generated events in order, so a command that produces none puts
	// that matching out of step, which is the whole reason this event exists.
	_, already := wf.StreamCursors[streamID]

	// Everything is staged, including a stream this workflow owns, so that the
	// resolved start offset and the event recording it are produced in one
	// place rather than two.
	wf.StagePendingSubscription(PendingStreamSubscription{
		StreamID:          streamID,
		StartOffset:       attrs.GetStartOffset(),
		AlreadySubscribed: already,
		Event: wf.ReserveStreamSubscribedEvent(
			streamID, opts.WorkflowTaskCompletedEventID),
	})
	return nil
}

// streamSubscribedEvent is the event a subscription writes.
//
// It is recorded once per subscription, not per message: the offsets a task
// consumed ride WorkflowTaskCompleted, and payloads never enter History. The
// event exists because a command that produces none desynchronises the
// command-to-event matching every SDK's replay depends on, and because without
// it nothing in History explains why a workflow started receiving stream data.
type streamSubscribedEvent struct{}

func (streamSubscribedEvent) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_WORKFLOW_STREAM_SUBSCRIBED
}

func (streamSubscribedEvent) IsWorkflowTaskTrigger() bool { return false }

// The cursor lives in CHASM state, which is persisted and rebuilt with the
// execution, so there is nothing for replication or reset to reconstruct here.
func (streamSubscribedEvent) Apply(chasm.MutableContext, *Workflow, *historypb.HistoryEvent) error {
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

// streamMessagesAddedEvent is the event a publish writes.
//
// One per batch, holding the offset range and nothing else. That is what makes
// it a fixed cost: a batch of one 20-byte message and a batch of a thousand
// 2KB messages write the same event, because the bodies stay in the stream
// component. It exists for the same reason the subscription event does, that a
// command producing no event desynchronises the command-to-event matching
// every SDK's replay depends on, and it doubles as the only record in History
// that the workflow published at all.
type streamMessagesAddedEvent struct{}

func (streamMessagesAddedEvent) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_WORKFLOW_STREAM_MESSAGES_ADDED
}

func (streamMessagesAddedEvent) IsWorkflowTaskTrigger() bool { return false }

// The frontier it describes is CHASM state, persisted and rebuilt with the
// execution, so there is nothing here to reconstruct.
func (streamMessagesAddedEvent) Apply(
	chasm.MutableContext, *Workflow, *historypb.HistoryEvent,
) error {
	return nil
}

// A command event, so it is never cherry-picked: the offsets belong to a log
// the new branch did not write.
func (streamMessagesAddedEvent) CherryPick(
	chasm.MutableContext,
	*Workflow,
	*historypb.HistoryEvent,
	map[enumspb.ResetReapplyExcludeType]struct{},
) error {
	return ErrEventNotCherryPickable
}

// RecordStreamMessagesAdded writes the event for one published batch.
func (w *Workflow) RecordStreamMessagesAdded(
	streamID string,
	firstOffset int64,
	count int64,
	workflowTaskCompletedEventID int64,
) {
	eventType := enumspb.EVENT_TYPE_WORKFLOW_STREAM_MESSAGES_ADDED
	w.AddHistoryEvent(eventType, func(e *historypb.HistoryEvent) {
		attrs := &historypb.WorkflowStreamMessagesAddedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			StreamId:                     streamID,
			FirstOffset:                  firstOffset,
			MessageCount:                 count,
		}
		e.Attributes = &historypb.HistoryEvent_WorkflowStreamMessagesAddedEventAttributes{
			WorkflowStreamMessagesAddedEventAttributes: attrs,
		}
	})
}

// streamNamed returns the workflow's stream of that name, creating it on first
// use. Implicit creation is deliberate: a workflow publishing to its own output
// should not have to coordinate with anyone about who creates it.
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

func toLibraryMessages(in []*streampb.StreamMessage) []*streamlib.StreamMessage {
	out := make([]*streamlib.StreamMessage, len(in))
	for i, m := range in {
		out[i] = &streamlib.StreamMessage{
			Body:          m.GetBody(),
			Metadata:      m.GetMetadata(),
			Topic:         m.GetTopic(),
			TopicSequence: m.GetTopicSequence(),
			Kind:          streamlib.STREAM_MESSAGE_KIND_DATA,
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
		enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES: func(
			chasmCtx chasm.MutableContext,
			wf *Workflow,
			validator Validator,
			command *commandpb.Command,
			opts CommandHandlerOptions,
		) error {
			limits := l.config.LimitsFor(chasmCtx.NamespaceEntry().Name().String())
			return handleAddStreamMessagesCommand(chasmCtx, wf, validator, command, opts, limits)
		},
		enumspb.COMMAND_TYPE_SUBSCRIBE_STREAM: handleSubscribeStreamCommand,
	}
}

func (l *streamLibrary) EventDefinitions() []EventDefinition {
	return []EventDefinition{streamSubscribedEvent{}, streamMessagesAddedEvent{}}
}
