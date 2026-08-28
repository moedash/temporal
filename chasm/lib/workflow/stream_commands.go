package workflow

import (
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

// handleAddStreamMessagesCommand appends to a stream the workflow owns.
//
// The stream is a co-located subcomponent, so its frontier advances as part of
// the workflow task's own commit: no extra transition and no cross-execution
// write. The log bytes cannot be written here, because a command handler runs
// under the state lock with no context to do I/O from, so they are staged and
// flushed before the commit that makes them visible.
//
// The offsets are known here, unlike a subscription's, so the event is written
// here too rather than being staged for the flush.
func handleAddStreamMessagesCommand(
	chasmCtx chasm.MutableContext,
	wf *Workflow,
	validator Validator,
	command *commandpb.Command,
	opts CommandHandlerOptions,
) error {
	attrs := command.GetAddStreamMessagesCommandAttributes()
	if attrs == nil {
		return serviceerror.NewInvalidArgument("AddStreamMessagesCommandAttributes is not set")
	}
	if len(attrs.GetMessages()) == 0 {
		return serviceerror.NewInvalidArgument("AddStreamMessages command carries no messages")
	}

	// The batch becomes one log node, so the whole batch is what has to fit.
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

	s, err := wf.streamNamed(chasmCtx, name)
	if err != nil {
		return err
	}

	result, err := s.AddMessages(chasmCtx, stream.AddMessagesRequest{
		Messages: toLibraryMessages(attrs.GetMessages()),
		TxnID:    streamTxnID(s, opts.WorkflowTaskCompletedEventID, opts.WorkflowTaskAttempt),
	})
	if err != nil {
		return err
	}
	for _, op := range result.Appends {
		wf.StageStreamAppend(s.State.GetCollectionId(), op)
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
// A stream the workflow owns is subscribed here and now, because everything the
// cursor needs is already in this execution. One in another execution cannot
// be: its collection id is the stream's run id and its bucket size is its own,
// and finding either means a lookup a command handler cannot do. Those are
// staged and resolved in the flush before commit, the same way log writes are.
func handleSubscribeStreamCommand(
	chasmCtx chasm.MutableContext,
	wf *Workflow,
	_ Validator,
	command *commandpb.Command,
	_ CommandHandlerOptions,
) error {
	attrs := command.GetSubscribeStreamCommandAttributes()
	if attrs == nil {
		return serviceerror.NewInvalidArgument("SubscribeStreamCommandAttributes is not set")
	}
	streamID := attrs.GetStreamId()
	if streamID == "" {
		return serviceerror.NewInvalidArgument("SubscribeStream command names no stream")
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

// RecordStreamSubscribed writes the event for a resolved subscription.
func (w *Workflow) RecordStreamSubscribed(
	streamID string,
	startOffset int64,
	workflowTaskCompletedEventID int64,
) {
	w.AddHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_STREAM_SUBSCRIBED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowStreamSubscribedEventAttributes{
			WorkflowStreamSubscribedEventAttributes: &historypb.WorkflowStreamSubscribedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
				StreamId:                     streamID,
				StartOffset:                  startOffset,
			},
		}
	})
}

// streamMessagesAddedEvent is the event a publish writes.
//
// One per batch, holding the offset range and nothing else. That is what makes
// it a fixed cost: a batch of one 20-byte message and a batch of a thousand
// 2KB messages write the same event, because the bodies went to the stream's
// log. It exists for the same reason the subscription event does, that a
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
func (streamMessagesAddedEvent) Apply(chasm.MutableContext, *Workflow, *historypb.HistoryEvent) error {
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
	w.AddHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_STREAM_MESSAGES_ADDED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowStreamMessagesAddedEventAttributes{
			WorkflowStreamMessagesAddedEventAttributes: &historypb.WorkflowStreamMessagesAddedEventAttributes{
				WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
				StreamId:                     streamID,
				FirstOffset:                  firstOffset,
				MessageCount:                 count,
			},
		}
	})
}

// streamNamed returns the workflow's stream of that name, creating it on first
// use. Implicit creation is deliberate: a workflow publishing to its own output
// should not have to coordinate with anyone about who creates it.
func (w *Workflow) streamNamed(ctx chasm.MutableContext, name string) (*stream.Stream, error) {
	if w.Streams == nil {
		w.Streams = make(chasm.Map[string, *stream.Stream])
	}
	if field, ok := w.Streams[name]; ok {
		return field.Get(ctx), nil
	}

	// Keyed on the execution so the identity is stable for the workflow, and
	// distinct from any other workflow reusing the same name.
	created, err := stream.NewStream(ctx, stream.NewStreamRequest{
		CollectionID: ctx.ExecutionKey().RunID + "/" + name,
		Attached:     true,
	})
	if err != nil {
		return nil, err
	}
	w.Streams[name] = chasm.NewComponentField(ctx, created)
	return created, nil
}

// streamTxnID derives a transaction id that advances across workflow tasks and
// within one, anchored on the task's completed event id and attempt.
//
// The attempt is what keeps a retry apart from the attempt it replaces. A
// failed attempt never commits, so the stream's last committed id does not
// move, and two attempts anchored on the event id alone would both write the
// same node under the same id. Storage keys a node by that pair, so the two
// rows collapse into one and the survivor is whichever reached the database
// last, not whichever attempt committed. Replay is deterministic, but a
// re-issued attempt is not a replay: anything the worker re-runs before the
// completion is durable, a local activity for instance, may return a different
// value and publish different bytes.
//
// A later attempt is always higher, so the store resolves the collision the
// same way it does for an external producer: the newer transaction id wins.
func streamTxnID(s *stream.Stream, workflowTaskCompletedEventID int64, attempt int32) int64 {
	next := workflowTaskCompletedEventID + int64(max(attempt, 1)) - 1
	if last := s.State.GetLastTxnId(); next <= last {
		next = last + 1
	}
	return next
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

// streamLibrary registers the stream command with the workflow registry.
type streamLibrary struct{}

func (l *streamLibrary) CommandHandlers() map[enumspb.CommandType]CommandHandler {
	return map[enumspb.CommandType]CommandHandler{
		enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES: handleAddStreamMessagesCommand,
		enumspb.COMMAND_TYPE_SUBSCRIBE_STREAM:    handleSubscribeStreamCommand,
	}
}

func (l *streamLibrary) EventDefinitions() []EventDefinition {
	return []EventDefinition{streamSubscribedEvent{}, streamMessagesAddedEvent{}}
}
