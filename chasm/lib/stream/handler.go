package stream

import (
	"context"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"google.golang.org/protobuf/proto"
)

type handler struct {
	streampb.UnimplementedStreamServiceServer

	shardController shard.Controller
	logger          log.Logger

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
	appendMu sync.Mutex
	appendLk map[string]*sync.Mutex

	tail *tailCache
}

func newHandler(shardController shard.Controller, logger log.Logger) *handler {
	return &handler{
		shardController: shardController,
		logger:          logger,
		appendLk:        make(map[string]*sync.Mutex),
		tail:            newTailCache(tailCacheBytesPerStream, tailCacheMaxStreams),
	}
}

func streamKey(namespaceID, streamID string) string {
	return namespaceID + "/" + streamID
}

func (h *handler) lockStream(namespaceID, streamID string) func() {
	key := streamKey(namespaceID, streamID)
	h.appendMu.Lock()
	mu, ok := h.appendLk[key]
	if !ok {
		mu = &sync.Mutex{}
		h.appendLk[key] = mu
	}
	h.appendMu.Unlock()

	mu.Lock()
	return mu.Unlock
}

func refFor(namespaceID, streamID string) chasm.ComponentRef {
	return chasm.NewComponentRef[*Stream](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  streamID,
	})
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
		func(mctx chasm.MutableContext, input *streampb.CreateStreamInput) (*Stream, error) {
			return NewStream(mctx, NewStreamRequest{
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

	shardCtx, err := h.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(req.GetNamespaceId()), in.GetStreamId())
	if err != nil {
		return nil, err
	}

	ref := refFor(req.GetNamespaceId(), in.GetStreamId())
	state, err := chasm.ReadComponent(ctx, ref, (*Stream).snapshot, struct{}{})
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

	addReq := AddMessagesRequest{
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
	staged := &Stream{State: state}
	preview, err := staged.AddMessages(nil, addReq)
	if err != nil {
		return nil, err
	}
	if !preview.Deduplicated {
		for _, op := range preview.Appends {
			if err := WriteAppend(ctx, shardCtx.GetExecutionManager(), shardCtx.GetShardID(),
				req.GetNamespaceId(), state.GetCollectionId(), op); err != nil {
				return nil, err
			}
		}
	}

	result, _, err := chasm.UpdateComponent(ctx, ref, (*Stream).AddMessages, addReq)
	if err != nil {
		return nil, err
	}

	// Only after the commit. A write whose commit failed can be superseded by a
	// retry carrying different bytes at the same offsets, and caching it would
	// serve those bytes to a reader that must never see them.
	if !result.Deduplicated {
		for _, op := range preview.Appends {
			h.tail.put(streamKey(req.GetNamespaceId(), in.GetStreamId()),
				result.FirstOffset, result.NextOffset, op.Blob)
		}
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

func (h *handler) FinishWriting(
	ctx context.Context,
	req *streampb.FinishWritingRequest,
) (*streampb.FinishWritingResponse, error) {
	in := req.GetFrontendRequest()
	_, _, err := chasm.UpdateComponent(
		ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()),
		func(s *Stream, mctx chasm.MutableContext, producerID string) (struct{}, error) {
			return struct{}{}, s.FinishWriting(mctx, producerID)
		},
		in.GetProducerId(),
	)
	if err != nil {
		return nil, err
	}
	return &streampb.FinishWritingResponse{FrontendResponse: &streampb.FinishWritingOutput{}}, nil
}

func (h *handler) PollMessages(
	ctx context.Context,
	req *streampb.PollMessagesRequest,
) (*streampb.PollMessagesResponse, error) {
	in := req.GetFrontendRequest()

	shardCtx, err := h.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(req.GetNamespaceId()), in.GetStreamId())
	if err != nil {
		return nil, err
	}

	ref := refFor(req.GetNamespaceId(), in.GetStreamId())
	from := in.GetFromOffset()

	state, err := chasm.ReadComponent(ctx, ref, (*Stream).snapshot, struct{}{})
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
		return &streampb.PollMessagesResponse{FrontendResponse: out}, nil
	}

	maxMessages := int(in.GetMaxMessages())
	if maxMessages <= 0 {
		maxMessages = defaultMaxMessagesPerPoll
	}

	// The frontier always comes from the component, so the cache can only save
	// a read, never widen what the reader is allowed to see.
	key := streamKey(req.GetNamespaceId(), in.GetStreamId())
	blobs, startOffsets, cached := h.tail.get(key, from, state.GetHeadOffset())
	if !cached {
		blobs, startOffsets, err = ReadRange(ctx, shardCtx.GetExecutionManager(), shardCtx.GetShardID(),
			req.GetNamespaceId(), state.GetCollectionId(), state.GetBucketSize(),
			from, state.GetHeadOffset(), 0)
		if err != nil {
			return nil, err
		}
	}

	messages, next, err := collectMessages(blobs, startOffsets, from, state.GetHeadOffset(),
		maxMessages, in.GetTopics())
	if err != nil {
		return nil, err
	}
	if next < state.GetHeadOffset() && len(messages) == 0 {
		// A page that filtered everything out still has to advance, or the
		// caller loops forever on the same offsets.
		next = state.GetHeadOffset()
	}
	out.Messages = messages
	out.NextOffset = next
	return &streampb.PollMessagesResponse{FrontendResponse: out}, nil
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
	pollCtx, cancel := contextutil.WithDeadlineBuffer(ctx, longPollTimeout, longPollBuffer)
	defer cancel()

	state, _, err := chasm.PollComponent(pollCtx, ref,
		func(s *Stream, _ chasm.Context, offset int64) (*streampb.StreamState, bool, error) {
			// Monotonic, as PollComponent requires: the head only advances and
			// closed never clears.
			satisfied := s.State.GetHeadOffset() > offset || s.State.GetClosed()
			if !satisfied {
				return nil, false, nil
			}
			return common.CloneProto(s.State), true, nil
		}, from)
	if err != nil {
		if pollCtx.Err() != nil && ctx.Err() == nil {
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

// collectMessages decodes the batches covering a range and trims to the
// requested window. Decoding happens only here and only on the batches a read
// actually touches; the store never interprets them, and user payloads stay
// opaque because the codec runs in the SDK.
func collectMessages(
	blobs []*commonpb.DataBlob,
	startOffsets []int64,
	from int64,
	head int64,
	maxMessages int,
	topics []string,
) ([]*streampb.StreamMessage, int64, error) {
	wanted := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		wanted[t] = struct{}{}
	}

	var out []*streampb.StreamMessage
	next := from
	for i, blob := range blobs {
		var batch streampb.StreamMessageBatch
		if err := proto.Unmarshal(blob.GetData(), &batch); err != nil {
			return nil, 0, err
		}
		for j, msg := range batch.GetMessages() {
			offset := startOffsets[i] + int64(j)
			if offset < from || offset >= head {
				continue
			}
			if len(out) >= maxMessages {
				return out, next, nil
			}
			next = offset + 1
			if len(wanted) > 0 {
				if _, ok := wanted[msg.GetTopic()]; !ok {
					continue
				}
			}
			out = append(out, msg)
		}
	}
	return out, next, nil
}

func (h *handler) DescribeStream(
	ctx context.Context,
	req *streampb.DescribeStreamRequest,
) (*streampb.DescribeStreamResponse, error) {
	in := req.GetFrontendRequest()
	state, err := chasm.ReadComponent(ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()), (*Stream).snapshot, struct{}{})
	if err != nil {
		return nil, err
	}
	return &streampb.DescribeStreamResponse{
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
		func(s *Stream, mctx chasm.MutableContext, reason *commonpb.Payload) (struct{}, error) {
			return struct{}{}, s.Close(mctx, reason)
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
	_, _, err := chasm.UpdateComponent(
		ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()),
		func(s *Stream, mctx chasm.MutableContext, newBase int64) (struct{}, error) {
			return struct{}{}, s.Truncate(mctx, newBase)
		},
		in.GetNewBaseOffset(),
	)
	if err != nil {
		return nil, err
	}
	return &streampb.TruncateStreamResponse{FrontendResponse: &streampb.TruncateStreamOutput{}}, nil
}

func (h *handler) DeleteStream(
	ctx context.Context,
	req *streampb.DeleteStreamRequest,
) (*streampb.DeleteStreamResponse, error) {
	in := req.GetFrontendRequest()
	if err := chasm.DeleteExecution[*Stream](ctx, chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  in.GetStreamId(),
	}, chasm.DeleteExecutionRequest{}); err != nil {
		return nil, err
	}
	return &streampb.DeleteStreamResponse{FrontendResponse: &streampb.DeleteStreamOutput{}}, nil
}
