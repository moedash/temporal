package service

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
)

type handler struct {
	streampb.UnimplementedStreamServiceServer

	shardController   shard.Controller
	namespaceRegistry namespace.Registry
	logger            log.Logger
	config            *stream.Config

	// Routes a call to the host that owns a shard. A step spanning two
	// executions cannot resolve both through the local controller, which
	// refuses a shard this host does not own, so the far half goes back out
	// through the service and lands wherever it belongs.
	routed streampb.StreamServiceClient
}

func newHandler(
	shardController shard.Controller,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	config *stream.Config,
	routed streampb.StreamServiceClient,
) *handler {
	return &handler{
		shardController:   shardController,
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
		config:            config,
		routed:            routed,
	}
}

// limitsFor resolves the namespace's limits. An id the registry cannot name
// falls back to the defaults; the interceptors have already refused requests
// for namespaces that do not exist.
func (h *handler) limitsFor(namespaceID string) stream.Limits {
	name, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		return stream.DefaultLimits()
	}
	return h.config.LimitsFor(name.String())
}

// withCallerInfo tags the context so the stream's direct persistence calls are
// attributed to the namespace that caused them. Without it they carry no caller
// name, which means they escape namespace rate limiting and priority as well as
// going uncounted in per-namespace metrics. The RPC path sets this via
// interceptors; calls made outside a request handler have to set it themselves.
func (h *handler) withCallerInfo(ctx context.Context, namespaceID string) context.Context {
	name, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		return ctx
	}
	return headers.SetCallerInfo(ctx, headers.NewCallerInfo(
		name.String(), headers.CallerTypeAPI, ""))
}

// refFor builds a reference to a stream. A supplied run ID lets the engine skip
// resolving the current run, which is otherwise a persistence lookup on every
// call and dominates the cost of an otherwise cheap read.
func refFor(namespaceID, streamID string) chasm.ComponentRef {
	return refForRun(namespaceID, streamID, "")
}

func refForRun(namespaceID, streamID, runID string) chasm.ComponentRef {
	return chasm.NewComponentRef[*stream.Stream](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  streamID,
		RunID:       runID,
	})
}

// workflowRef builds a reference to the execution that owns an attached
// stream. An attached stream is a subcomponent, so it has no id of its own and
// everything about it is reached through its owner.
//
// An empty runID means the current run. A caller that supplies one is pinning:
// an owned stream does not carry across continue-as-new, so the successor's
// stream of the same name is a different, empty one, and a caller that meant
// the predecessor would otherwise be redirected to it without being told.
func workflowRef(namespaceID, workflowID, runID string) chasm.ComponentRef {
	return chasm.NewComponentRef[*chasmworkflow.Workflow](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  workflowID,
		RunID:       runID,
	})
}

// ownedStreamName resolves a name the caller left empty the same way a publish
// command does, so a reader addresses the default stream by omission just as a
// writer creates it by omission.
func ownedStreamName(name string) string {
	if name == "" {
		return chasmworkflow.DefaultStreamName
	}
	return name
}

func (h *handler) CreateStream(
	ctx context.Context,
	req *streampb.CreateStreamRequest,
) (*streampb.CreateStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	if in.GetStreamId() == "" {
		return nil, serviceerror.NewInvalidArgument("stream id is required")
	}

	result, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: in.GetStreamId()},
		func(mctx chasm.MutableContext, input *streampb.CreateStreamInput) (*stream.Stream, error) {
			return stream.NewStream(mctx, stream.NewStreamRequest{Lifecycle: input.GetLifecycle()})
		},
		in,
	)
	if err != nil {
		return nil, err
	}
	return &streampb.CreateStreamResponse{
		FrontendResponse: &streampb.CreateStreamOutput{RunId: result.ExecutionKey.RunID},
	}, nil
}

func (h *handler) AddMessages(
	ctx context.Context,
	req *streampb.AddMessagesRequest,
) (*streampb.AddMessagesResponse, error) {
	in := req.GetFrontendRequest()
	if len(in.GetMessages()) == 0 {
		return nil, serviceerror.NewInvalidArgument("no messages to append")
	}
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	// The batch and the frontier commit in one transition, and the execution
	// serializes transitions, so a producer that names no expected offset takes
	// whatever offset it lands at. A retried sequence is answered by the
	// producer table, not by a pin on the head.
	addReq := stream.AddMessagesRequest{
		Messages:   in.GetMessages(),
		ProducerID: in.GetProducerId(),
		Sequence:   in.GetSequence(),
		Limits:     h.limitsFor(req.GetNamespaceId()),
	}
	if in.GetUseExpectedOffset() {
		expected := in.GetExpectedOffset()
		addReq.ExpectedOffset = &expected
	}

	result, _, err := chasm.UpdateComponent(ctx,
		refForRun(req.GetNamespaceId(), in.GetStreamId(), in.GetRunId()),
		(*stream.Stream).AddMessages, addReq)
	if err != nil {
		return nil, err
	}

	return &streampb.AddMessagesResponse{
		FrontendResponse: &streampb.AddMessagesOutput{
			FirstOffset:  result.FirstOffset,
			NextOffset:   result.NextOffset,
			Count:        result.Count,
			Deduplicated: result.Deduplicated,
		},
	}, nil
}

// AddWorkflowMessages appends to a stream a workflow owns, from outside that
// workflow.
//
// The workflow's own publishes ride its Workflow Task and cost no transition of
// their own. This producer is off-shard, so it pays one transition on the
// owning execution per batch, and batching is what keeps that cheap. It is the
// path a model activity streaming tokens takes, where the workflow is only
// bracketing what the activity produces.
//
// One transition does everything: the stream is created on first write, and
// the workflow's own publishes are serialized against this append by the
// execution, so neither producer needs to pin the head against the other.
func (h *handler) AddWorkflowMessages(
	ctx context.Context,
	req *streampb.AddWorkflowMessagesRequest,
) (*streampb.AddWorkflowMessagesResponse, error) {
	in := req.GetFrontendRequest()
	if len(in.GetMessages()) == 0 {
		return nil, serviceerror.NewInvalidArgument("no messages to append")
	}
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	name := ownedStreamName(in.GetStreamName())
	addReq := stream.AddMessagesRequest{
		Messages:   in.GetMessages(),
		ProducerID: in.GetProducerId(),
		Sequence:   in.GetSequence(),
		Limits:     h.limitsFor(req.GetNamespaceId()),
	}
	result, _, err := chasm.UpdateComponent(ctx,
		workflowRef(req.GetNamespaceId(), in.GetWorkflowId(), in.GetOwnerRunId()),
		func(
			wf *chasmworkflow.Workflow, mctx chasm.MutableContext, r stream.AddMessagesRequest,
		) (stream.AddMessagesResult, error) {
			return wf.AppendToOwnedStream(mctx, name, r)
		}, addReq)
	if err != nil {
		return nil, err
	}

	return &streampb.AddWorkflowMessagesResponse{
		FrontendResponse: &streampb.AddMessagesOutput{
			FirstOffset:  result.FirstOffset,
			NextOffset:   result.NextOffset,
			Count:        result.Count,
			Deduplicated: result.Deduplicated,
		},
	}, nil
}

func (h *handler) FinishWriting(
	ctx context.Context,
	req *streampb.FinishWritingRequest,
) (*streampb.FinishWritingResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	_, _, err := chasm.UpdateComponent(
		ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()),
		func(s *stream.Stream, mctx chasm.MutableContext, producerID string) (struct{}, error) {
			return struct{}{}, s.FinishWriting(mctx, producerID)
		},
		in.GetProducerId(),
	)
	if err != nil {
		return nil, err
	}
	return &streampb.FinishWritingResponse{FrontendResponse: &streampb.FinishWritingOutput{}}, nil
}

// SubscribeWorkflow registers a workflow as a consumer of a stream it owns.
//
// The cursor is written into the workflow's own state, not the stream's, which
// is what lets every later advance commit with the event that records it. Only
// a stream the workflow owns can be subscribed here: reaching one in another
// execution needs that stream's frontier, and reading it from inside the
// consuming workflow's transaction is a separate problem.
func (h *handler) SubscribeWorkflow(
	ctx context.Context,
	req *streampb.SubscribeWorkflowRequest,
) (*streampb.SubscribeWorkflowResponse, error) {
	in := req.GetFrontendRequest()

	if in.GetStreamId() != "" {
		return h.subscribeToExternalStream(ctx, req.GetNamespaceId(), in)
	}

	limits := h.limitsFor(req.GetNamespaceId())
	startOffset, _, err := chasm.UpdateComponent(
		ctx,
		workflowRef(req.GetNamespaceId(), in.GetWorkflowId(), in.GetOwnerRunId()),
		func(
			wf *chasmworkflow.Workflow, mctx chasm.MutableContext, input *streampb.SubscribeWorkflowInput,
		) (int64, error) {
			return wf.SubscribeToOwnedStream(
				mctx, ownedStreamName(input.GetStreamName()), input.GetStartOffset(), limits)
		},
		in,
	)
	if err != nil {
		return nil, err
	}

	return &streampb.SubscribeWorkflowResponse{
		FrontendResponse: &streampb.SubscribeWorkflowOutput{StartOffset: startOffset},
	}, nil
}

// subscribeToExternalStream registers a cursor against a stream in another
// execution.
//
// The pin goes on the stream before the cursor goes on the workflow, and the
// order is the guarantee: interrupted after the first write there is a pin
// holding storage nothing reads, which costs space. Interrupted after the
// other order there would be a cursor with no pin, and truncation would be free
// to take a range that cursor still points at.
func (h *handler) subscribeToExternalStream(
	ctx context.Context,
	namespaceID string,
	in *streampb.SubscribeWorkflowInput,
) (*streampb.SubscribeWorkflowResponse, error) {
	// The pin is keyed by the consuming run, so the run is resolved first. It
	// also pins the cursor write below to that run, so a run that ends between
	// the two steps cannot leave the pin on one run and the cursor on another.
	consumerRunID, err := chasm.ReadComponent(ctx,
		workflowRef(namespaceID, in.GetWorkflowId(), in.GetOwnerRunId()),
		func(_ *chasmworkflow.Workflow, cctx chasm.Context, _ struct{}) (string, error) {
			return cctx.ExecutionKey().RunID, nil
		}, struct{}{})
	if err != nil {
		return nil, err
	}

	// The stream half goes out and comes back on the shard that owns it. This
	// handler was routed to the consuming workflow, so the stream may well be
	// somewhere else, and resolving it here would fail on any cluster with more
	// than one history host.
	//
	// The pin still lands before the cursor, which is the guarantee: interrupted
	// between them there is a pin holding storage nothing reads, which costs
	// space, where the other order would leave a cursor with no pin and let
	// truncation take a range it still points at.
	registered, err := h.routed.RegisterStreamConsumer(ctx, &streampb.RegisterStreamConsumerRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &streampb.RegisterStreamConsumerInput{
			Namespace:          in.GetNamespace(),
			StreamId:           in.GetStreamId(),
			ConsumerWorkflowId: in.GetWorkflowId(),
			ConsumerRunId:      consumerRunID,
			StartOffset:        in.GetStartOffset(),
		},
	})
	if err != nil {
		return nil, err
	}
	pin := registered.GetFrontendResponse()

	startOffset, _, err := chasm.UpdateComponent(
		ctx,
		workflowRef(namespaceID, in.GetWorkflowId(), consumerRunID),
		func(wf *chasmworkflow.Workflow, mctx chasm.MutableContext, offset int64) (int64, error) {
			return wf.SubscribeToExternalStream(mctx, chasmworkflow.ExternalStreamSubscription{
				StreamID:    in.GetStreamId(),
				StartOffset: offset,
				KnownHead:   pin.GetKnownHead(),
			})
		},
		pin.GetStartOffset(),
	)
	if err != nil {
		return nil, err
	}

	return &streampb.SubscribeWorkflowResponse{
		FrontendResponse: &streampb.SubscribeWorkflowOutput{StartOffset: startOffset},
	}, nil
}

// RegisterStreamConsumer takes the pin, on the shard that owns the stream.
//
// Internal. Called by SubscribeWorkflow and by the workflow task completion
// path, both of which run on the consumer's shard and so cannot reach the
// stream themselves. The start offset is resolved inside the transition that
// records it, against the same frontier it hands back, so the consumer records
// facts rather than readings.
func (h *handler) RegisterStreamConsumer(
	ctx context.Context,
	req *streampb.RegisterStreamConsumerRequest,
) (*streampb.RegisterStreamConsumerResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	limits := h.limitsFor(req.GetNamespaceId())
	pin, _, err := chasm.UpdateComponent(
		ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()),
		func(
			s *stream.Stream, mctx chasm.MutableContext, offset int64,
		) (*streampb.RegisterStreamConsumerOutput, error) {
			startOffset, err := s.RegisterConsumer(mctx, stream.ConsumerRegistration{
				ConsumerID:   externalConsumerID(in.GetConsumerWorkflowId(), in.GetConsumerRunId()),
				WorkflowID:   in.GetConsumerWorkflowId(),
				RunID:        in.GetConsumerRunId(),
				Offset:       offset,
				External:     true,
				MaxConsumers: limits.MaxConsumersPerStream,
			})
			if err != nil {
				return nil, err
			}
			return &streampb.RegisterStreamConsumerOutput{
				StartOffset: startOffset,
				KnownHead:   s.State.GetHeadOffset(),
			}, nil
		},
		in.GetStartOffset(),
	)
	if err != nil {
		return nil, err
	}
	return &streampb.RegisterStreamConsumerResponse{FrontendResponse: pin}, nil
}

// externalConsumerID names a workflow run's pin on a stream in another
// execution. Keyed by run, so a later run of the same workflow id registers
// fresh instead of inheriting a closed run's floor.
func externalConsumerID(workflowID, runID string) string {
	return "workflow:" + workflowID + "/" + runID
}

// consumerProbe is what one run says about a subscription: whether the run is
// still open, whether it holds a cursor for the stream, and where that cursor
// began, which is the floor a re-keyed pin has to hold.
type consumerProbe struct {
	runID       string
	closed      bool
	consumes    bool
	startOffset int64
}

// probeConsumer reads a run without touching it. A run that is gone reads as
// closed, since the notify task only needs to know whether pushing at it can
// achieve anything.
func (h *handler) probeConsumer(
	ctx context.Context,
	namespaceID, workflowID, runID, streamID string,
) (consumerProbe, error) {
	probe, err := chasm.ReadComponent(ctx, workflowRef(namespaceID, workflowID, runID),
		func(wf *chasmworkflow.Workflow, cctx chasm.Context, _ struct{}) (consumerProbe, error) {
			p := consumerProbe{
				runID:  cctx.ExecutionKey().RunID,
				closed: !cctx.ExecutionInfo().CloseTime.IsZero(),
			}
			if field, ok := wf.StreamCursors[streamID]; ok {
				p.consumes = true
				p.startOffset = field.Get(cctx).StartOffset()
			}
			return p, nil
		}, struct{}{})
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return consumerProbe{runID: runID, closed: true}, nil
	}
	return probe, err
}

func (h *handler) pushHead(
	ctx context.Context,
	namespaceID, workflowID, runID, streamID string,
	head int64,
) error {
	_, _, err := chasm.UpdateComponent(
		ctx,
		workflowRef(namespaceID, workflowID, runID),
		func(wf *chasmworkflow.Workflow, mctx chasm.MutableContext, at int64) (struct{}, error) {
			return struct{}{}, wf.AdvanceKnownHead(mctx, streamID, at)
		},
		head,
	)
	return err
}

// AdvanceConsumerHead tells one consumer that the frontier moved, on the shard
// that owns that consumer.
//
// Internal. Called by the notify task, which runs on the stream's shard and so
// cannot reach a consumer living anywhere else. The task pins a run, and a run
// ends: the answer then says whether a successor carries the subscription, so
// the stream re-keys its pin, or nothing does, so the stream releases it.
func (h *handler) AdvanceConsumerHead(
	ctx context.Context,
	req *streampb.AdvanceConsumerHeadRequest,
) (*streampb.AdvanceConsumerHeadResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	namespaceID, workflowID, streamID := req.GetNamespaceId(), in.GetWorkflowId(), in.GetStreamId()

	pinned, err := h.probeConsumer(ctx, namespaceID, workflowID, in.GetOwnerRunId(), streamID)
	if err != nil {
		return nil, err
	}
	out := &streampb.AdvanceConsumerHeadOutput{}
	if !pinned.closed {
		if !pinned.consumes {
			out.ConsumerClosed = true
			return &streampb.AdvanceConsumerHeadResponse{FrontendResponse: out}, nil
		}
		err := h.pushHead(ctx, namespaceID, workflowID, pinned.runID, streamID, in.GetHeadOffset())
		if err != nil {
			return nil, err
		}
		return &streampb.AdvanceConsumerHeadResponse{FrontendResponse: out}, nil
	}

	// The pinned run is over. A continue-as-new carries the subscription to the
	// current run, and that is the only run worth pushing at.
	current, err := h.probeConsumer(ctx, namespaceID, workflowID, "", streamID)
	if err != nil {
		return nil, err
	}
	if current.closed || current.runID == pinned.runID || !current.consumes {
		out.ConsumerClosed = true
		return &streampb.AdvanceConsumerHeadResponse{FrontendResponse: out}, nil
	}
	err = h.pushHead(ctx, namespaceID, workflowID, current.runID, streamID, in.GetHeadOffset())
	if err != nil {
		return nil, err
	}
	out.SuccessorRunId = current.runID
	out.SuccessorStartOffset = current.startOffset
	return &streampb.AdvanceConsumerHeadResponse{FrontendResponse: out}, nil
}

func (h *handler) PollMessages(
	ctx context.Context,
	req *streampb.PollMessagesRequest,
) (*streampb.PollMessagesResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	ref := refForRun(req.GetNamespaceId(), in.GetStreamId(), in.GetRunId())
	from := in.GetFromOffset()

	state, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return nil, err
	}

	// Blocking is only worth it once the reader is genuinely caught up.
	if in.GetWaitNewMessages() && from == state.GetHeadOffset() && !state.GetClosed() {
		// The window is re-read below, so only the blocking matters here.
		if _, err := h.waitForMessages(ctx, ref, from, state); err != nil {
			return nil, err
		}
	}

	wreq := stream.WindowRequest{From: from, MaxMessages: in.GetMaxMessages(), Topics: in.GetTopics()}
	w, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).ReadWindow, wreq)
	if err != nil {
		return nil, err
	}
	out, err := formatWindow(w, wreq)
	if err != nil {
		return nil, err
	}
	return &streampb.PollMessagesResponse{FrontendResponse: out}, nil
}

// PollWorkflowMessages reads a stream a workflow owns.
//
// An attached stream is reached through its owner, so this call routes on the
// workflow id and both the frontier and the batches come out of the owner's
// component.
func (h *handler) PollWorkflowMessages(
	ctx context.Context,
	req *streampb.PollWorkflowMessagesRequest,
) (*streampb.PollWorkflowMessagesResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	ref := workflowRef(req.GetNamespaceId(), in.GetWorkflowId(), in.GetOwnerRunId())
	name := ownedStreamName(in.GetStreamName())
	from := in.GetFromOffset()

	state, err := h.ownedStreamState(ctx, ref, name)
	if err != nil {
		return nil, err
	}

	if in.GetWaitNewMessages() && from == state.GetHeadOffset() && !state.GetClosed() {
		if _, err := h.waitForOwnedMessages(ctx, ref, name, from, state); err != nil {
			return nil, err
		}
	}

	wreq := stream.WindowRequest{From: from, MaxMessages: in.GetMaxMessages(), Topics: in.GetTopics()}
	w, err := chasm.ReadComponent(ctx, ref, readOwnedWindow,
		ownedWindowRequest{Name: name, Window: wreq})
	if err != nil {
		return nil, err
	}
	out, err := formatWindow(w, wreq)
	if err != nil {
		return nil, err
	}
	return &streampb.PollWorkflowMessagesResponse{FrontendResponse: out}, nil
}

// formatWindow turns a component read into the wire response. The read happens
// in the component, so the frontier and the bytes it was served with cannot
// disagree.
//
// The reader is advanced only over offsets that were examined. CollectMessages
// steps past every message it filtered out, so a filtered page that matched
// nothing still moves the reader; a window whose batches stop short of its end
// must not be reported as read to the end.
func formatWindow(w stream.Window, req stream.WindowRequest) (*streampb.PollMessagesOutput, error) {
	out := &streampb.PollMessagesOutput{
		NextOffset:  req.From,
		HeadOffset:  w.State.GetHeadOffset(),
		Closed:      w.State.GetClosed(),
		CloseReason: w.State.GetCloseReason(),
		RunId:       w.RunID,
	}
	if req.From == w.State.GetHeadOffset() {
		return out, nil
	}

	messages, next, err := stream.CollectMessages(
		w.Blobs, w.Starts, req.From, w.To, w.Limit, req.Topics)
	if err != nil {
		return nil, err
	}
	out.Messages = messages
	out.NextOffset = next
	return out, nil
}

// ownedWindowRequest names which attached stream to read and what to read.
type ownedWindowRequest struct {
	Name   string
	Window stream.WindowRequest
}

// readOwnedWindow reads a stream attached to a workflow.
//
// A closed execution can take no more publishes, from its own Workflow Task or
// from anywhere else, so its stream is finished whether or not a producer said
// so. Without that a reader tailing a workflow that ended stays parked forever.
func readOwnedWindow(
	wf *chasmworkflow.Workflow,
	cctx chasm.Context,
	req ownedWindowRequest,
) (stream.Window, error) {
	s := wf.OwnedStream(cctx, req.Name)
	if s == nil {
		// Nothing published yet, which reads as an empty stream so a reader can
		// attach before the first append.
		return stream.Window{
			State: &streampb.StreamState{Closed: !cctx.ExecutionInfo().CloseTime.IsZero()},
			To:    req.Window.From,
		}, nil
	}
	w, err := s.ReadWindow(cctx, req.Window)
	if err != nil {
		return stream.Window{}, err
	}
	if !cctx.ExecutionInfo().CloseTime.IsZero() {
		w.State.Closed = true
	}
	return w, nil
}

// ownedStreamState snapshots an attached stream through the component that
// owns it. A stream the workflow has not published to yet reads as an empty
// one, so a reader may attach before the first event.
func (h *handler) ownedStreamState(
	ctx context.Context,
	ref chasm.ComponentRef,
	name string,
) (*streampb.StreamState, error) {
	state, err := chasm.ReadComponent(ctx, ref, readOwnedStream, name)
	if err != nil {
		return nil, err
	}
	return state, nil
}

// readOwnedStream snapshots an attached stream and reports whether anything
// can still be added to it.
//
// A closed execution can take no more publishes, from its own Workflow Task or
// from anywhere else, so its stream is finished whether or not a producer said
// so. Without this a reader tailing a workflow that ended stays parked forever.
func readOwnedStream(
	wf *chasmworkflow.Workflow,
	cctx chasm.Context,
	name string,
) (*streampb.StreamState, error) {
	state, err := wf.OwnedStreamState(cctx, name)
	if err != nil {
		return nil, err
	}
	if state == nil {
		state = &streampb.StreamState{}
	}
	if !cctx.ExecutionInfo().CloseTime.IsZero() {
		state.Closed = true
	}
	return state, nil
}

// waitForMessages blocks until the head passes the reader's offset or the
// stream closes. On the server's long-poll timeout it returns the state it last
// saw, so the caller gets an empty response and polls again rather than an
// error it would have to distinguish from a real failure.
func (h *handler) waitForMessages(
	ctx context.Context,
	ref chasm.ComponentRef,
	from int64,
	current *streampb.StreamState,
) (*streampb.StreamState, error) {
	pollCtx, cancel := contextutil.WithDeadlineBuffer(
		ctx, stream.LongPollTimeout, stream.LongPollBuffer)
	defer cancel()

	state, _, err := chasm.PollComponent(pollCtx, ref,
		func(s *stream.Stream, _ chasm.Context, offset int64) (*streampb.StreamState, bool, error) {
			// Monotonic, as PollComponent requires: the head only advances and
			// closed never clears.
			if !pollSatisfied(s.State, offset) {
				return nil, false, nil
			}
			return common.CloneProto(s.State), true, nil
		}, from)
	return pollOutcome(pollCtx, ctx, state, err, current)
}

// waitForOwnedMessages is waitForMessages against a stream reached through its
// owner. The predicate has to re-resolve the stream on every evaluation,
// because what the poll observes is the owning execution.
func (h *handler) waitForOwnedMessages(
	ctx context.Context,
	ref chasm.ComponentRef,
	name string,
	from int64,
	current *streampb.StreamState,
) (*streampb.StreamState, error) {
	pollCtx, cancel := contextutil.WithDeadlineBuffer(
		ctx, stream.LongPollTimeout, stream.LongPollBuffer)
	defer cancel()

	state, _, err := chasm.PollComponent(pollCtx, ref,
		func(
			wf *chasmworkflow.Workflow, cctx chasm.Context, offset int64,
		) (*streampb.StreamState, bool, error) {
			owned, err := readOwnedStream(wf, cctx, name)
			if err != nil {
				return nil, false, err
			}
			if !pollSatisfied(owned, offset) {
				return nil, false, nil
			}
			return owned, true, nil
		}, from)
	return pollOutcome(pollCtx, ctx, state, err, current)
}

// pollSatisfied is the monotonic condition PollComponent requires: the head
// only advances and closed never clears.
func pollSatisfied(state *streampb.StreamState, from int64) bool {
	return state.GetHeadOffset() > from || state.GetClosed()
}

// pollOutcome turns a long-poll result into the state the reader should be
// served.
func pollOutcome(
	pollCtx, callerCtx context.Context,
	state *streampb.StreamState,
	err error,
	current *streampb.StreamState,
) (*streampb.StreamState, error) {
	if err != nil {
		if pollCtx.Err() != nil && callerCtx.Err() == nil {
			// Our long-poll budget expired, not the caller's. Hand back the
			// state we already had so the reader gets an empty response and
			// polls again, rather than an error it has to tell apart from a
			// real failure.
			return current, nil
		}
		return nil, err
	}
	if state == nil {
		return current, nil
	}
	return state, nil
}

func (h *handler) DescribeStream(
	ctx context.Context,
	req *streampb.DescribeStreamRequest,
) (*streampb.DescribeStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	state, err := chasm.ReadComponent(ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()), (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return nil, err
	}
	return &streampb.DescribeStreamResponse{
		FrontendResponse: &streampb.DescribeStreamOutput{State: state},
	}, nil
}

// DescribeWorkflowStream reports the frontier of a stream a workflow owns. A
// reader needs it to start at the tail rather than at the beginning, which an
// attached stream offers no other way to find.
func (h *handler) DescribeWorkflowStream(
	ctx context.Context,
	req *streampb.DescribeWorkflowStreamRequest,
) (*streampb.DescribeWorkflowStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	state, err := h.ownedStreamState(ctx,
		workflowRef(req.GetNamespaceId(), in.GetWorkflowId(), in.GetOwnerRunId()),
		ownedStreamName(in.GetStreamName()))
	if err != nil {
		return nil, err
	}
	return &streampb.DescribeWorkflowStreamResponse{
		FrontendResponse: &streampb.DescribeStreamOutput{State: state},
	}, nil
}

func (h *handler) CloseStream(
	ctx context.Context,
	req *streampb.CloseStreamRequest,
) (*streampb.CloseStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	_, _, err := chasm.UpdateComponent(
		ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()),
		func(s *stream.Stream, mctx chasm.MutableContext, reason *commonpb.Payload) (struct{}, error) {
			return struct{}{}, s.CloseAndSchedule(mctx, reason)
		},
		in.GetReason(),
	)
	if err != nil {
		return nil, err
	}
	return &streampb.CloseStreamResponse{FrontendResponse: &streampb.CloseStreamOutput{}}, nil
}

func (h *handler) TruncateStream(
	ctx context.Context,
	req *streampb.TruncateStreamRequest,
) (*streampb.TruncateStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	if _, _, err := chasm.UpdateComponent(
		ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()),
		func(s *stream.Stream, mctx chasm.MutableContext, newBase int64) (struct{}, error) {
			return struct{}{}, s.Truncate(mctx, newBase)
		},
		in.GetNewBaseOffset(),
	); err != nil {
		return nil, err
	}
	return &streampb.TruncateStreamResponse{FrontendResponse: &streampb.TruncateStreamOutput{}}, nil
}

// ListStreams is intentionally not implemented here. It queries visibility, so
// it has no business ID to route on and the frontend answers it directly.

func (h *handler) DeleteStream(
	ctx context.Context,
	req *streampb.DeleteStreamRequest,
) (*streampb.DeleteStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	key := chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: in.GetStreamId()}

	// A workflow consuming the stream recorded ranges its replay will ask for.
	// Deleting under it succeeds now and fails that workflow later, so the
	// caller has to say it means it. Checked and then deleted rather than in
	// one transition, which leaves a window for a subscription that lands in
	// between; that consumer finds out at its next task, with a cause.
	if !in.GetForce() {
		state, err := chasm.ReadComponent(ctx, refFor(key.NamespaceID, key.BusinessID),
			(*stream.Stream).Snapshot, struct{}{})
		if err != nil {
			return nil, err
		}
		for id, consumer := range state.GetConsumers() {
			if consumer.GetActive() {
				return nil, serviceerror.NewFailedPreconditionf(
					"stream %q is consumed by workflow %q (%s); set force to delete it anyway",
					key.BusinessID, consumer.GetWorkflowId(), id)
			}
		}
	}

	// The payload is component state, so deleting the execution takes it too.
	err := chasm.DeleteExecution[*stream.Stream](ctx, key, chasm.DeleteExecutionRequest{})
	if err != nil {
		return nil, err
	}
	return &streampb.DeleteStreamResponse{FrontendResponse: &streampb.DeleteStreamOutput{}}, nil
}
