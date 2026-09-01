package service

import (
	"context"
	"sync"

	"github.com/dgryski/go-farm"
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
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
)

type handler struct {
	streampb.UnimplementedStreamServiceServer

	shardController   shard.Controller
	namespaceRegistry namespace.Registry
	logger            log.Logger

	// Appends to one stream are serialized here. The node has to be durable
	// before the frontier advances, which means writing it outside the
	// transition that advances the frontier, and two concurrent writers could
	// then commit in a different order than they wrote. Whichever node carried
	// the higher transaction ID would win the read regardless of which writer
	// actually committed.
	//
	// Serializing removes the interleaving. It is a stopgap: the real fix is to
	// stage the node inside the CHASM transaction so write and commit order
	// cannot diverge. Until then this also means appends are only safe within
	// one process, which holds because these RPCs route to the shard owner.
	//
	// Striped rather than one lock per stream. The key comes from request input
	// before the stream is known to exist, so a per-stream map would grow once
	// per distinct id a caller names, including ids that resolve to nothing.
	// Unrelated streams sharing a stripe only serialize with each other.
	appendLk [appendStripes]sync.Mutex

	tail *stream.TailCache
}

func newHandler(
	shardController shard.Controller,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
) *handler {
	return &handler{
		shardController:   shardController,
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
		tail:              stream.NewTailCache(stream.TailCacheBytesPerStream, stream.TailCacheMaxStreams),
	}
}

func streamKey(namespaceID, streamID string) string {
	return namespaceID + "/" + streamID
}

// logKey identifies the cached bytes by the log they came from, not by the name
// the caller used to reach it. A stream id can be reused: delete or close one
// and create another with the same id, and the new stream starts at offset 0
// again. Keyed by name, the old stream's entries would still match, and a
// reader of the new stream would be served bytes from the old one.
func logKey(namespaceID, collectionID string) string {
	return namespaceID + "/" + collectionID
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

func (h *handler) lockStream(namespaceID, streamID string) func() {
	mu := &h.appendLk[appendStripe(streamKey(namespaceID, streamID))]
	mu.Lock()
	return mu.Unlock
}

// Sized well above the per-host stream count that would make collisions matter.
// Appends to one stream are serialized anyway, so a collision costs only the
// unrelated stream's concurrency, never correctness.
const appendStripes = 2048

func appendStripe(key string) uint32 {
	return farm.Fingerprint32([]byte(key)) % appendStripes
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
func workflowRef(namespaceID, workflowID string) chasm.ComponentRef {
	return chasm.NewComponentRef[*chasmworkflow.Workflow](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  workflowID,
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

// reclaim deletes buckets that a committed truncation put out of reach. It runs
// after the commit, so a failure here leaves storage to reclaim later rather
// than data a reader can still ask for but no longer find.
// logStore is the slice of a shard this package needs. Declared narrowly so the
// package does not depend on the history service, which would make the workflow
// library unable to import it.
type logStore interface {
	GetShardID() int32
	GetExecutionManager() persistence.ExecutionManager
}

func (h *handler) reclaim(
	ctx context.Context,
	shardCtx logStore,
	namespaceID, collectionID string,
	buckets []int64,
) {
	for _, b := range buckets {
		if err := stream.DeleteBucket(ctx, shardCtx.GetExecutionManager(), shardCtx.GetShardID(),
			namespaceID, collectionID, b); err != nil {
			h.logger.Warn("failed to reclaim a truncated stream bucket",
				tag.NewStringTag("collection-id", collectionID),
				tag.NewInt64("bucket", b),
				tag.Error(err))
		}
	}
}

func (h *handler) CreateStream(
	ctx context.Context,
	req *streampb.CreateStreamRequest,
) (*streampb.CreateStreamResponse, error) {
	in := req.GetFrontendRequest()
	if in.GetStreamId() == "" {
		return nil, serviceerror.NewInvalidArgument("stream id is required")
	}

	result, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: in.GetStreamId()},
		func(mctx chasm.MutableContext, input *streampb.CreateStreamInput) (*stream.Stream, error) {
			return stream.NewStream(mctx, stream.NewStreamRequest{
				CollectionID: mctx.ExecutionKey().RunID,
				Lifecycle:    input.GetLifecycle(),
			})
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

	unlock := h.lockStream(req.GetNamespaceId(), in.GetStreamId())
	defer unlock()

	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	shardCtx, err := h.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(req.GetNamespaceId()), in.GetStreamId())
	if err != nil {
		return nil, err
	}

	ref := refForRun(req.GetNamespaceId(), in.GetStreamId(), in.GetRunId())
	state, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return nil, err
	}

	txnID, err := shardCtx.GenerateTaskID()
	if err != nil {
		return nil, err
	}
	if txnID <= state.GetLastTxnId() {
		txnID = state.GetLastTxnId() + 1
	}

	addReq := stream.AddMessagesRequest{
		Messages:   in.GetMessages(),
		ProducerID: in.GetProducerId(),
		Sequence:   in.GetSequence(),
		OwnerEpoch: in.GetOwnerEpoch(),
		TxnID:      txnID,
	}
	if in.GetUseExpectedOffset() {
		expected := in.GetExpectedOffset()
		addReq.ExpectedOffset = &expected
	} else {
		// Without a caller-supplied expectation, pin to the head we just read.
		// The transition then fails rather than appending at an offset whose
		// node we did not write.
		head := state.GetHeadOffset()
		addReq.ExpectedOffset = &head
	}

	// Dry run against the state we read, so the node is written at the offsets
	// the commit will claim. The transition below recomputes it identically.
	staged := &stream.Stream{State: state}
	preview, err := staged.AddMessages(nil, addReq)
	if err != nil {
		return nil, err
	}
	if !preview.Deduplicated {
		for _, op := range preview.Appends {
			if err := stream.WriteAppend(ctx, shardCtx.GetExecutionManager(), shardCtx.GetShardID(),
				req.GetNamespaceId(), state.GetCollectionId(), op); err != nil {
				return nil, err
			}
		}
	}

	result, _, err := chasm.UpdateComponent(ctx, ref, (*stream.Stream).AddMessages, addReq)
	if err != nil {
		return nil, err
	}

	// Only after the commit. A write whose commit failed can be superseded by a
	// retry carrying different bytes at the same offsets, and caching it would
	// serve those bytes to a reader that must never see them.
	if !result.Deduplicated {
		for _, op := range preview.Appends {
			h.tail.Put(logKey(req.GetNamespaceId(), state.GetCollectionId()),
				result.FirstOffset, result.NextOffset, op.Blob)
		}
	}
	h.reclaim(ctx, shardCtx, req.GetNamespaceId(), state.GetCollectionId(), result.ReclaimableBuckets)

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
func (h *handler) AddWorkflowMessages(
	ctx context.Context,
	req *streampb.AddWorkflowMessagesRequest,
) (*streampb.AddWorkflowMessagesResponse, error) {
	in := req.GetFrontendRequest()
	if len(in.GetMessages()) == 0 {
		return nil, serviceerror.NewInvalidArgument("no messages to append")
	}

	name := ownedStreamName(in.GetStreamName())

	// Keyed on the owner and the name, which is what identifies the log here.
	unlock := h.lockStream(req.GetNamespaceId(), in.GetWorkflowId()+"/"+name)
	defer unlock()

	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	shardCtx, err := h.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(req.GetNamespaceId()), in.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	ref := workflowRef(req.GetNamespaceId(), in.GetWorkflowId())

	state, err := chasm.ReadComponent(ctx, ref,
		func(wf *chasmworkflow.Workflow, cctx chasm.Context, streamName string) (*streampb.StreamState, error) {
			return wf.OwnedStreamState(cctx, streamName)
		}, name)
	if err != nil {
		return nil, err
	}
	if state == nil {
		// Nothing has published yet, so the collection id and bucket size this
		// write needs do not exist. Creating the stream is a transition, and
		// only the first writer ever pays it.
		state, _, err = chasm.UpdateComponent(ctx, ref,
			(*chasmworkflow.Workflow).EnsureOwnedStream, name)
		if err != nil {
			return nil, err
		}
	}

	txnID, err := shardCtx.GenerateTaskID()
	if err != nil {
		return nil, err
	}
	if txnID <= state.GetLastTxnId() {
		txnID = state.GetLastTxnId() + 1
	}

	head := state.GetHeadOffset()
	addReq := stream.AddMessagesRequest{
		Messages:   in.GetMessages(),
		ProducerID: in.GetProducerId(),
		Sequence:   in.GetSequence(),
		TxnID:      txnID,
		// Pinned to the head just read, so a workflow task that published
		// between the read and the commit fails this append rather than
		// letting it claim offsets whose node it did not write.
		ExpectedOffset: &head,
	}

	// Dry run against the state we read, so the node is written at the offsets
	// the commit will claim, exactly as the standalone path does.
	staged := &stream.Stream{State: state}
	preview, err := staged.AddMessages(nil, addReq)
	if err != nil {
		return nil, err
	}
	if !preview.Deduplicated {
		for _, op := range preview.Appends {
			if err := stream.WriteAppend(ctx, shardCtx.GetExecutionManager(), shardCtx.GetShardID(),
				req.GetNamespaceId(), state.GetCollectionId(), op); err != nil {
				return nil, err
			}
		}
	}

	result, _, err := chasm.UpdateComponent(ctx, ref,
		func(wf *chasmworkflow.Workflow, mctx chasm.MutableContext, r stream.AddMessagesRequest) (stream.AddMessagesResult, error) {
			return wf.AppendToOwnedStream(mctx, name, r)
		}, addReq)
	if err != nil {
		return nil, err
	}

	// Only after the commit, for the same reason as the standalone path: a
	// write whose commit failed can be superseded by a retry carrying different
	// bytes at the same offsets.
	if !result.Deduplicated {
		for _, op := range preview.Appends {
			h.tail.Put(logKey(req.GetNamespaceId(), state.GetCollectionId()),
				result.FirstOffset, result.NextOffset, op.Blob)
		}
	}
	h.reclaim(ctx, shardCtx, req.GetNamespaceId(), state.GetCollectionId(), result.ReclaimableBuckets)

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

	startOffset, _, err := chasm.UpdateComponent(
		ctx,
		workflowRef(req.GetNamespaceId(), in.GetWorkflowId()),
		func(wf *chasmworkflow.Workflow, mctx chasm.MutableContext, input *streampb.SubscribeWorkflowInput) (int64, error) {
			return wf.SubscribeToOwnedStream(
				mctx, ownedStreamName(input.GetStreamName()), input.GetStartOffset())
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
	streamID := in.GetStreamId()

	state, err := chasm.ReadComponent(ctx,
		refFor(namespaceID, streamID), (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return nil, err
	}

	startOffset := in.GetStartOffset()
	if startOffset < 0 {
		startOffset = state.GetHeadOffset()
	}
	if startOffset < state.GetBaseOffset() {
		return nil, serviceerror.NewFailedPreconditionf(
			"offset %d is below the stream's floor of %d", startOffset, state.GetBaseOffset())
	}

	consumerID := "workflow:" + in.GetWorkflowId()
	if _, _, err := chasm.UpdateComponent(
		ctx,
		refFor(namespaceID, streamID),
		func(s *stream.Stream, mctx chasm.MutableContext, offset int64) (struct{}, error) {
			return struct{}{}, s.RegisterConsumer(mctx, consumerID, in.GetWorkflowId(), "", offset, true)
		},
		startOffset,
	); err != nil {
		return nil, err
	}

	registered, _, err := chasm.UpdateComponent(
		ctx,
		workflowRef(namespaceID, in.GetWorkflowId()),
		func(wf *chasmworkflow.Workflow, mctx chasm.MutableContext, offset int64) (int64, error) {
			return wf.SubscribeToExternalStream(mctx, chasmworkflow.ExternalStreamSubscription{
				StreamID:     streamID,
				CollectionID: state.GetCollectionId(),
				BucketSize:   state.GetBucketSize(),
				StartOffset:  offset,
				KnownHead:    state.GetHeadOffset(),
			})
		},
		startOffset,
	)
	if err != nil {
		return nil, err
	}

	return &streampb.SubscribeWorkflowResponse{
		FrontendResponse: &streampb.SubscribeWorkflowOutput{StartOffset: registered},
	}, nil
}

func (h *handler) PollMessages(
	ctx context.Context,
	req *streampb.PollMessagesRequest,
) (*streampb.PollMessagesResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	shardCtx, err := h.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(req.GetNamespaceId()), in.GetStreamId())
	if err != nil {
		return nil, err
	}

	ref := refForRun(req.GetNamespaceId(), in.GetStreamId(), in.GetRunId())
	from := in.GetFromOffset()

	state, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return nil, err
	}

	// Blocking is only worth it once the reader is genuinely caught up.
	if in.GetWaitNewMessages() && from == state.GetHeadOffset() && !state.GetClosed() {
		state, err = h.waitForMessages(ctx, ref, from, state)
		if err != nil {
			return nil, err
		}
	}

	out, err := h.readWindow(ctx, shardCtx, req.GetNamespaceId(), state, from,
		in.GetMaxMessages(), in.GetTopics())
	if err != nil {
		return nil, err
	}
	return &streampb.PollMessagesResponse{FrontendResponse: out}, nil
}

// PollWorkflowMessages reads a stream a workflow owns.
//
// Everything that addresses the stream comes from its owner: the shard, the
// frontier, and the collection the log nodes were written under. That is also
// why the log read below needs no special case. An attached stream's nodes are
// written under the owner's shard, which is the shard this call routed to.
func (h *handler) PollWorkflowMessages(
	ctx context.Context,
	req *streampb.PollWorkflowMessagesRequest,
) (*streampb.PollWorkflowMessagesResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	shardCtx, err := h.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(req.GetNamespaceId()), in.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	ref := workflowRef(req.GetNamespaceId(), in.GetWorkflowId())
	name := ownedStreamName(in.GetStreamName())
	from := in.GetFromOffset()

	state, err := h.ownedStreamState(ctx, ref, name)
	if err != nil {
		return nil, err
	}

	if in.GetWaitNewMessages() && from == state.GetHeadOffset() && !state.GetClosed() {
		state, err = h.waitForOwnedMessages(ctx, ref, name, from, state)
		if err != nil {
			return nil, err
		}
	}

	out, err := h.readWindow(ctx, shardCtx, req.GetNamespaceId(), state, from,
		in.GetMaxMessages(), in.GetTopics())
	if err != nil {
		return nil, err
	}
	return &streampb.PollWorkflowMessagesResponse{FrontendResponse: out}, nil
}

// readWindow serves a reader's window out of a frontier the caller resolved.
// Standalone and attached streams differ only in where that frontier comes
// from, so nothing past it is aware of the difference.
func (h *handler) readWindow(
	ctx context.Context,
	shardCtx logStore,
	namespaceID string,
	state *streampb.StreamState,
	from int64,
	maxMessages int32,
	topics []string,
) (*streampb.PollMessagesOutput, error) {
	if from < state.GetBaseOffset() {
		return nil, serviceerror.NewFailedPreconditionf(
			"offset %d has been truncated, the stream starts at %d", from, state.GetBaseOffset())
	}
	if from > state.GetHeadOffset() {
		return nil, serviceerror.NewInvalidArgumentf(
			"offset %d is past the stream head %d", from, state.GetHeadOffset())
	}

	out := &streampb.PollMessagesOutput{
		NextOffset:  from,
		HeadOffset:  state.GetHeadOffset(),
		Closed:      state.GetClosed(),
		CloseReason: state.GetCloseReason(),
	}
	if from == state.GetHeadOffset() {
		return out, nil
	}

	limit := int(maxMessages)
	if limit <= 0 {
		limit = stream.DefaultMaxMessagesPerPoll
	}

	// Clip the read to what the caller can be given. One offset is one message,
	// so this bound is exact. Without it a poll for a single message off a large
	// stream reads every batch from the offset to the head before trimming, and
	// the whole stream lands in memory on the history host.
	//
	// A topic filter can leave the page short of the limit. That is fine: the
	// response carries next_offset, so the caller reads on from there.
	to := min(state.GetHeadOffset(), from+int64(limit))

	// The frontier always comes from the component, so the cache can only save
	// a read, never widen what the reader is allowed to see.
	key := logKey(namespaceID, state.GetCollectionId())
	blobs, startOffsets, cached := h.tail.Get(key, from, to)
	if !cached {
		var err error
		blobs, startOffsets, err = stream.ReadRange(ctx, shardCtx.GetExecutionManager(),
			shardCtx.GetShardID(), namespaceID, state.GetCollectionId(), state.GetBucketSize(),
			from, to, 0)
		if err != nil {
			return nil, err
		}
	}

	messages, next, err := stream.CollectMessages(blobs, startOffsets, from, to, limit, topics)
	if err != nil {
		return nil, err
	}
	if next < to && len(messages) == 0 && len(topics) > 0 {
		// A page that filtered everything out still has to advance, or the
		// caller loops forever on the same offsets. Limited to a filtered read
		// on purpose: for any other reason a page comes back short, moving the
		// reader past offsets it was never given would hide the short read.
		next = to
	}
	out.Messages = messages
	out.NextOffset = next
	return out, nil
}

// ownedStreamState snapshots an attached stream through the component that
// owns it. A stream the workflow has not published to yet reads as an empty
// one, so a reader may attach before the first event.
func (h *handler) ownedStreamState(
	ctx context.Context,
	ref chasm.ComponentRef,
	name string,
) (*streampb.StreamState, error) {
	state, err := chasm.ReadComponent(ctx, ref,
		func(wf *chasmworkflow.Workflow, cctx chasm.Context, streamName string) (*streampb.StreamState, error) {
			return wf.OwnedStreamState(cctx, streamName)
		}, name)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return &streampb.StreamState{}, nil
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
	pollCtx, cancel := contextutil.WithDeadlineBuffer(ctx, stream.LongPollTimeout, stream.LongPollBuffer)
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
	pollCtx, cancel := contextutil.WithDeadlineBuffer(ctx, stream.LongPollTimeout, stream.LongPollBuffer)
	defer cancel()

	state, _, err := chasm.PollComponent(pollCtx, ref,
		func(wf *chasmworkflow.Workflow, cctx chasm.Context, offset int64) (*streampb.StreamState, bool, error) {
			owned, err := wf.OwnedStreamState(cctx, name)
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
		workflowRef(req.GetNamespaceId(), in.GetWorkflowId()),
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

	shardCtx, err := h.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(req.GetNamespaceId()), in.GetStreamId())
	if err != nil {
		return nil, err
	}

	reclaimable, _, err := chasm.UpdateComponent(
		ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()),
		func(s *stream.Stream, mctx chasm.MutableContext, newBase int64) ([]int64, error) {
			return s.Truncate(mctx, newBase)
		},
		in.GetNewBaseOffset(),
	)
	if err != nil {
		return nil, err
	}

	if len(reclaimable) > 0 {
		state, err := chasm.ReadComponent(ctx,
			refFor(req.GetNamespaceId(), in.GetStreamId()), (*stream.Stream).Snapshot, struct{}{})
		if err == nil {
			h.reclaim(ctx, shardCtx, req.GetNamespaceId(), state.GetCollectionId(), reclaimable)
		}
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
	key := chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: in.GetStreamId()}

	// The log has to go before the execution that names it. Read the state to
	// find the buckets while the execution is still there to be read.
	ref := chasm.NewComponentRef[*stream.Stream](key)
	state, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return nil, err
	}
	shardCtx, err := h.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(req.GetNamespaceId()), in.GetStreamId())
	if err != nil {
		return nil, err
	}
	deleteLogBuckets(h.withCallerInfo(ctx, req.GetNamespaceId()), shardCtx, h.logger,
		req.GetNamespaceId(), state)

	if err := chasm.DeleteExecution[*stream.Stream](ctx, key, chasm.DeleteExecutionRequest{}); err != nil {
		return nil, err
	}
	return &streampb.DeleteStreamResponse{FrontendResponse: &streampb.DeleteStreamOutput{}}, nil
}
