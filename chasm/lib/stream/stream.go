package stream

import (
	"crypto/sha256"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
)

// Stream is a durable, offset-addressed append-only sequence. It holds only the
// frontier: the payload bytes live in the log (see log.go), so this state is
// O(producers + consumers) no matter how long the stream gets.
//
// Appending never schedules a workflow task. A stream item is data produced by
// an execution, not a decision input to it, so nothing in a workflow's state
// machine advances because one arrived.
type Stream struct {
	chasm.UnimplementedComponent

	State *streampb.StreamState
}

type NewStreamRequest struct {
	CollectionID string
	BucketSize   int64
	Lifecycle    *streampb.StreamLifecycle
}

type AddMessagesRequest struct {
	Messages []*streampb.StreamMessage

	// Optional idempotency. A producer supplies either an identity and
	// sequence, or an expected offset, or neither and accepts at-least-once.
	ProducerID     string
	Sequence       int64
	ExpectedOffset *int64

	// Optional fencing. Rejected if below the stream's current epoch.
	OwnerEpoch int64

	// Monotonic transaction ID from the shard generator (shard.GenerateTaskID).
	// It must come from there rather than from stream state: a retry has to
	// carry a higher ID than the attempt it replaces, and a counter derived
	// from committed state would hand the retry the same one.
	TxnID int64
}

type AddMessagesResult struct {
	FirstOffset int64
	NextOffset  int64
	Count       int64

	// True when a retry matched a recorded producer sequence, so nothing was
	// appended and the original offsets are returned.
	Deduplicated bool

	// Staged nodes for the caller to persist before the frontier is observable.
	// Empty when deduplicated.
	Appends []LogAppend
}

func NewStream(_ chasm.MutableContext, req NewStreamRequest) (*Stream, error) {
	bucketSize := req.BucketSize
	if bucketSize <= 0 {
		bucketSize = DefaultBucketSize
	}
	return &Stream{
		State: &streampb.StreamState{
			CollectionId: req.CollectionID,
			BucketSize:   bucketSize,
			Lifecycle:    req.Lifecycle,
			Producers:    make(map[string]*streampb.ProducerCursor),
			Consumers:    make(map[string]*streampb.ConsumerCursor),
		},
	}, nil
}

func (s *Stream) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	if s.State.Closed {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

// AddMessages assigns a contiguous offset range and stages the bytes. It does
// not persist: the caller writes the staged nodes and only then is the new
// frontier observable, which is the ordering that makes a torn append invisible
// rather than corrupting.
func (s *Stream) AddMessages(
	_ chasm.MutableContext,
	req AddMessagesRequest,
) (AddMessagesResult, error) {
	if s.State.Closed {
		return AddMessagesResult{}, serviceerror.NewFailedPrecondition("stream is closed")
	}
	if len(req.Messages) == 0 {
		return AddMessagesResult{}, serviceerror.NewInvalidArgument("no messages to append")
	}

	blob, err := marshalBatch(req.Messages)
	if err != nil {
		return AddMessagesResult{}, err
	}
	hash := contentHash(blob.Data)

	if replay, err := s.checkProducer(req, hash); err != nil || replay != nil {
		if err != nil {
			return AddMessagesResult{}, err
		}
		return *replay, nil
	}

	if req.OwnerEpoch != 0 && req.OwnerEpoch < s.State.OwnerEpoch {
		return AddMessagesResult{}, serviceerror.NewFailedPrecondition("producer has been fenced")
	}

	if req.ExpectedOffset != nil && *req.ExpectedOffset != s.State.HeadOffset {
		return AddMessagesResult{}, serviceerror.NewAlreadyExistsf(
			"expected offset %d but stream head is %d", *req.ExpectedOffset, s.State.HeadOffset)
	}

	first := s.State.HeadOffset
	count := int64(len(req.Messages))
	if BucketOf(first, s.State.BucketSize) != BucketOf(first+count-1, s.State.BucketSize) {
		// A node may not straddle a bucket, because a bucket is a storage
		// partition. Splitting is the caller's job for now; rejecting keeps
		// the invariant explicit rather than silently producing a bad node.
		return AddMessagesResult{}, serviceerror.NewInvalidArgumentf(
			"batch of %d at offset %d crosses a bucket boundary", count, first)
	}

	txnID := req.TxnID
	if txnID <= s.State.LastTxnId {
		return AddMessagesResult{}, serviceerror.NewInvalidArgumentf(
			"transaction id %d must exceed the last committed id %d", txnID, s.State.LastTxnId)
	}

	appendOp := LogAppend{
		Bucket:      BucketOf(first, s.State.BucketSize),
		NodeID:      NodeIDOf(first, s.State.BucketSize),
		TxnID:       txnID,
		PrevTxnID:   s.State.LastTxnId,
		Blob:        blob,
		IsNewBucket: NodeIDOf(first, s.State.BucketSize) == 1,
	}

	s.State.HeadOffset = first + count
	s.State.LastTxnId = txnID
	if req.ProducerID != "" {
		s.State.Producers[req.ProducerID] = &streampb.ProducerCursor{
			Seq:         req.Sequence,
			FirstOffset: first,
			Count:       count,
			ContentHash: hash,
		}
	}

	return AddMessagesResult{
		FirstOffset: first,
		NextOffset:  s.State.HeadOffset,
		Count:       count,
		Appends:     []LogAppend{appendOp},
	}, nil
}

// checkProducer applies per-producer idempotency. It returns a replay result
// when the request is a genuine retry, and an error when it is not a retry but
// cannot be accepted either.
func (s *Stream) checkProducer(req AddMessagesRequest, hash []byte) (*AddMessagesResult, error) {
	if req.ProducerID == "" {
		return nil, nil
	}
	cursor := s.State.Producers[req.ProducerID]
	if cursor == nil {
		return nil, nil
	}
	if cursor.Fenced {
		return nil, serviceerror.NewFailedPrecondition("producer has finished writing to this stream")
	}
	if req.Sequence > cursor.Seq {
		return nil, nil
	}
	if req.Sequence < cursor.Seq {
		return nil, serviceerror.NewInvalidArgumentf(
			"stale producer sequence %d, last accepted is %d", req.Sequence, cursor.Seq)
	}
	// Same sequence. Identical content is a retry; different content is a
	// client bug, and returning the recorded offsets would report success while
	// silently dropping the caller's data.
	if !equalHash(cursor.ContentHash, hash) {
		return nil, serviceerror.NewInvalidArgumentf(
			"producer sequence %d already used with different content", req.Sequence)
	}
	return &AddMessagesResult{
		FirstOffset:  cursor.FirstOffset,
		NextOffset:   cursor.FirstOffset + cursor.Count,
		Count:        cursor.Count,
		Deduplicated: true,
	}, nil
}

// FinishWriting ends one producer's writes without closing the stream, so other
// producers carry on. Weaker than Close on purpose.
func (s *Stream) FinishWriting(_ chasm.MutableContext, producerID string) error {
	if producerID == "" {
		return serviceerror.NewInvalidArgument("producer id is required")
	}
	cursor := s.State.Producers[producerID]
	if cursor == nil {
		cursor = &streampb.ProducerCursor{Seq: -1}
		s.State.Producers[producerID] = cursor
	}
	cursor.Fenced = true
	return nil
}

// Close seals the stream. It does not delete it: a closed stream stays readable
// through retention, which is what removes the shutdown handshake the current
// signal-based implementation forces on users.
func (s *Stream) Close(_ chasm.MutableContext, reason *commonpb.Payload) error {
	if s.State.Closed {
		return nil
	}
	s.State.Closed = true
	s.State.CloseReason = reason
	return nil
}

// Truncate advances the readable floor. It cannot pass a registered in-workflow
// consumer, because that consumer's history records an offset range it must
// still be able to re-read on replay.
func (s *Stream) Truncate(_ chasm.MutableContext, newBase int64) error {
	if newBase < s.State.BaseOffset {
		return serviceerror.NewInvalidArgumentf(
			"cannot truncate backwards from %d to %d", s.State.BaseOffset, newBase)
	}
	if newBase > s.State.HeadOffset {
		return serviceerror.NewInvalidArgumentf(
			"cannot truncate past head offset %d", s.State.HeadOffset)
	}
	if pin, ok := s.consumerPin(); ok && newBase > pin {
		return serviceerror.NewFailedPreconditionf(
			"cannot truncate past offset %d, which an active consumer still needs", pin)
	}
	s.State.BaseOffset = newBase
	return nil
}

// consumerPin is the lowest offset any active in-workflow consumer still needs.
func (s *Stream) consumerPin() (int64, bool) {
	var pin int64
	found := false
	for _, c := range s.State.Consumers {
		if !c.Active {
			continue
		}
		if !found || c.Offset < pin {
			pin = c.Offset
			found = true
		}
	}
	return pin, found
}

func marshalBatch(messages []*streampb.StreamMessage) (*commonpb.DataBlob, error) {
	data, err := proto.Marshal(&streampb.StreamMessageBatch{Messages: messages})
	if err != nil {
		return nil, err
	}
	return &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         data,
	}, nil
}

func contentHash(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func equalHash(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
