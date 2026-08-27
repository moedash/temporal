package workflow

import (
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
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
// the workflow task's own commit: no history event, no extra transition, and no
// cross-execution write. The log bytes cannot be written here, because a command
// handler runs under the state lock with no context to do I/O from, so they are
// staged and flushed before the commit that makes them visible.
func handleAddStreamMessagesCommand(
	chasmCtx chasm.MutableContext,
	wf *Workflow,
	_ Validator,
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
		TxnID:    streamTxnID(s, opts.WorkflowTaskCompletedEventID),
	})
	if err != nil {
		return err
	}
	for _, op := range result.Appends {
		wf.StageStreamAppend(s.State.GetCollectionId(), op)
	}
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

	if _, owned := wf.Streams[streamID]; owned {
		_, err := wf.SubscribeToOwnedStream(chasmCtx, streamID, attrs.GetStartOffset())
		return err
	}

	// Already subscribed, so there is nothing to resolve. Re-issuing on replay
	// has to be a no-op rather than a second registration.
	if _, ok := wf.StreamCursors[streamID]; ok {
		return nil
	}

	wf.StagePendingSubscription(PendingStreamSubscription{
		StreamID:    streamID,
		StartOffset: attrs.GetStartOffset(),
	})
	return nil
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
// within one, anchored on the task's completed event id.
//
// A retried workflow task can reuse an id, and that is safe here in a way it is
// not for an external producer: the workflow replays deterministically and
// re-issues the same command, so a reused id lands the same bytes at the same
// node, which the store treats as an idempotent overwrite. The hazard the
// external path guards against is different content under an equal id, which
// deterministic replay cannot produce.
func streamTxnID(s *stream.Stream, workflowTaskCompletedEventID int64) int64 {
	next := workflowTaskCompletedEventID
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
	// None, deliberately. Publishing produces no history event at all.
	return nil
}
