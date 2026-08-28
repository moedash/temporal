package stream

import (
	"crypto/sha256"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	// Present so streams are listable. Operators need to find them the same way
	// they find workflows, and without this the only way to reach a stream is
	// to already know its ID.
	Visibility chasm.Field[*chasm.Visibility]
}

type NewStreamRequest struct {
	CollectionID string
	BucketSize   int64
	Lifecycle    *streampb.StreamLifecycle

	// Attached means the stream is a subcomponent of another execution rather
	// than a root. CHASM requires a visibility component to be an immediate
	// child of the root, so an attached stream carries none and is found
	// through its owner instead of through ListStreams.
	Attached bool
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

	// Buckets the message cap pushed below the readable floor. Safe to delete
	// once this transition commits, never before.
	ReclaimableBuckets []int64
}

func NewStream(ctx chasm.MutableContext, req NewStreamRequest) (*Stream, error) {
	bucketSize := req.BucketSize
	if bucketSize <= 0 {
		bucketSize = DefaultBucketSize
	}
	visibility := chasm.NewEmptyField[*chasm.Visibility]()
	if !req.Attached {
		visibility = chasm.NewComponentField(ctx, chasm.NewVisibility(ctx))
	}
	return &Stream{
		Visibility: visibility,
		State: &streampb.StreamState{
			CollectionId: req.CollectionID,
			BucketSize:   bucketSize,
			Lifecycle:    req.Lifecycle,
			Producers:    make(map[string]*streampb.ProducerCursor),
			Consumers:    make(map[string]*streampb.ConsumerCursor),
		},
	}, nil
}

// ContextMetadata satisfies chasm.RootComponent. A stream carries no metadata
// worth propagating to the request context.
func (s *Stream) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

// Terminate seals the stream so a forced shutdown does not leave it accepting
// writes. Data already appended stays readable, because consumers may have read
// it and the stream is append-only.
func (s *Stream) Terminate(
	mctx chasm.MutableContext,
	req chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	reason := &commonpb.Payload{Data: []byte(req.Reason)}
	return chasm.TerminateComponentResponse{}, s.CloseAndSchedule(mctx, reason)
}

// Snapshot returns a copy of the frontier for read paths. It is a copy because
// the caller reads it outside the transition that produced it.
func (s *Stream) Snapshot(_ chasm.Context, _ struct{}) (*streampb.StreamState, error) {
	return common.CloneProto(s.State), nil
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
	mctx chasm.MutableContext,
	req AddMessagesRequest,
) (AddMessagesResult, error) {
	if s.State.Closed {
		return AddMessagesResult{}, serviceerror.NewFailedPrecondition("stream is closed")
	}
	if len(req.Messages) == 0 {
		return AddMessagesResult{}, serviceerror.NewInvalidArgument("no messages to append")
	}
	if len(req.Messages) > MaxMessagesPerBatch {
		return AddMessagesResult{}, serviceerror.NewInvalidArgumentf(
			"batch of %d exceeds the limit of %d messages", len(req.Messages), MaxMessagesPerBatch)
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

	// After the retry check, so a known producer is never rejected for room.
	if err := s.checkProducerRoom(req.ProducerID); err != nil {
		return AddMessagesResult{}, err
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
		if s.State.Producers == nil {
			s.State.Producers = make(map[string]*streampb.ProducerCursor)
		}
		s.State.Producers[req.ProducerID] = &streampb.ProducerCursor{
			Seq:         req.Sequence,
			FirstOffset: first,
			Count:       count,
			ContentHash: hash,
		}
	}

	result := AddMessagesResult{
		FirstOffset:        first,
		NextOffset:         s.State.HeadOffset,
		Count:              count,
		Appends:            []LogAppend{appendOp},
		ReclaimableBuckets: s.applyCap(),
	}
	s.notifyConsumers(mctx)
	return result, nil
}

// notifyConsumers schedules the wake for consumers this append left behind.
//
// Only a workflow in another execution needs it. One consuming a stream it owns
// sees the new frontier while closing its own transaction, so waking it through
// a task would only duplicate a decision already made locally.
func (s *Stream) notifyConsumers(mctx chasm.MutableContext) {
	// The append path previews itself against a detached copy to work out which
	// log node to write, and that preview has no transition to attach a task
	// to. Only the real transition, which carries a context, schedules one.
	if mctx == nil {
		return
	}
	for _, consumer := range s.State.Consumers {
		if consumer.GetExternal() && consumer.GetActive() && consumer.GetOffset() < s.State.HeadOffset {
			mctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: mctx.Now(s)}, &streampb.StreamNotifyConsumersTask{})
			return
		}
	}
}

// checkProducerRoom keeps the dedup table bounded.
//
// Entries whose whole batch sits below the floor go first: a retry of a batch
// that truncation already removed cannot be served its recorded offsets
// anyway, so the entry has no use left.
func (s *Stream) checkProducerRoom(producerID string) error {
	if producerID == "" {
		return nil
	}
	if _, known := s.State.Producers[producerID]; known {
		return nil
	}
	if len(s.State.Producers) < MaxProducersPerStream {
		return nil
	}
	for id, cursor := range s.State.Producers {
		if cursor.GetFirstOffset()+cursor.GetCount() <= s.State.GetBaseOffset() {
			delete(s.State.Producers, id)
		}
	}
	if len(s.State.Producers) >= MaxProducersPerStream {
		return serviceerror.NewInvalidArgumentf(
			"stream already tracks %d producers, which is the limit", MaxProducersPerStream)
	}
	return nil
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
	if s.State.Producers == nil {
		s.State.Producers = make(map[string]*streampb.ProducerCursor)
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
// Close returns when retention deletion should be scheduled, or the zero time
// if the stream was already closed or has no retention configured. Scheduling
// is the caller's job, which keeps the component a pure state transition and
// testable without a live context.
func (s *Stream) Close(now time.Time, reason *commonpb.Payload) time.Time {
	if s.State.Closed {
		return time.Time{}
	}
	s.State.Closed = true
	s.State.CloseReason = reason
	s.State.CloseTime = timestamppb.New(now)

	retention := s.State.GetLifecycle().GetRetention().AsDuration()
	if retention <= 0 {
		return time.Time{}
	}
	return now.Add(retention)
}

// CloseAndSchedule closes the stream and arms retention if it asked for it.
func (s *Stream) CloseAndSchedule(mctx chasm.MutableContext, reason *commonpb.Payload) error {
	if at := s.Close(mctx.Now(s), reason); !at.IsZero() {
		mctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: at}, &streampb.StreamRetentionTask{})
	}
	return nil
}

// Truncate advances the readable floor. It cannot pass a registered in-workflow
// consumer, because that consumer's history records an offset range it must
// still be able to re-read on replay.
func (s *Stream) Truncate(_ chasm.MutableContext, newBase int64) ([]int64, error) {
	if newBase < s.State.BaseOffset {
		return nil, serviceerror.NewInvalidArgumentf(
			"cannot truncate backwards from %d to %d", s.State.BaseOffset, newBase)
	}
	if newBase > s.State.HeadOffset {
		return nil, serviceerror.NewInvalidArgumentf(
			"cannot truncate past head offset %d", s.State.HeadOffset)
	}
	if pin, ok := s.consumerPin(); ok && newBase > pin {
		return nil, serviceerror.NewFailedPreconditionf(
			"cannot truncate past offset %d, which an active consumer still needs", pin)
	}
	reclaimable := ReclaimableBuckets(s.State.BaseOffset, newBase, s.State.BucketSize)
	s.State.BaseOffset = newBase
	return reclaimable, nil
}

// applyCap advances the readable floor when the stream is over its message cap.
// Evaluated at the end of a successful append rather than by a sweeper: the
// append transition is already writing, so folding the check into it costs
// nothing and keeps the cap tight instead of eventually true.
func (s *Stream) applyCap() []int64 {
	maxItems := s.State.GetLifecycle().GetMaxItems()
	if maxItems <= 0 {
		return nil
	}
	readable := s.State.HeadOffset - s.State.BaseOffset
	if readable <= maxItems {
		return nil
	}
	newBase := s.State.HeadOffset - maxItems
	if pin, ok := s.consumerPin(); ok && newBase > pin {
		// A workflow consumer still needs this range, so the cap yields to it.
		// Storage grows rather than a consumer losing data it recorded a cursor
		// for and must be able to re-read on replay.
		newBase = pin
	}
	if newBase <= s.State.BaseOffset {
		return nil
	}
	reclaimable := ReclaimableBuckets(s.State.BaseOffset, newBase, s.State.BucketSize)
	s.State.BaseOffset = newBase
	return reclaimable
}

// RegisterConsumer pins the stream's readable floor at offset on behalf of an
// in-workflow consumer, so truncation and the message cap cannot take a range
// the consumer has not read yet.
//
// Without this the interlock in Truncate and applyCap has nothing to consult:
// a consumer's cursor lives in its own execution, and the stream cannot see it.
func (s *Stream) RegisterConsumer(
	_ chasm.MutableContext,
	consumerID string,
	workflowID string,
	runID string,
	offset int64,
	external bool,
) error {
	if consumerID == "" {
		return serviceerror.NewInvalidArgument("consumer id is required")
	}
	if offset < s.State.BaseOffset {
		return serviceerror.NewFailedPreconditionf(
			"offset %d is below the stream's floor of %d", offset, s.State.BaseOffset)
	}
	if _, known := s.State.Consumers[consumerID]; !known &&
		len(s.State.Consumers) >= MaxConsumersPerStream {
		return serviceerror.NewInvalidArgumentf(
			"stream already has %d consumers, which is the limit", MaxConsumersPerStream)
	}
	if s.State.Consumers == nil {
		s.State.Consumers = make(map[string]*streampb.ConsumerCursor)
	}
	if existing, ok := s.State.Consumers[consumerID]; ok {
		existing.Active = true
		return nil
	}
	s.State.Consumers[consumerID] = &streampb.ConsumerCursor{
		WorkflowId: workflowID,
		RunId:      runID,
		Offset:     offset,
		Active:     true,
		External:   external,
	}
	return nil
}

// AdvanceConsumer moves a consumer's pin forward as it reads. It never moves
// backwards: the floor is what lets a recorded range still be re-read, so
// lowering it would give back a guarantee already written to History.
func (s *Stream) AdvanceConsumer(_ chasm.MutableContext, consumerID string, offset int64) {
	consumer, ok := s.State.Consumers[consumerID]
	if !ok || offset <= consumer.Offset {
		return
	}
	consumer.Offset = offset
}

// DeregisterConsumer releases the floor a consumer was holding.
func (s *Stream) DeregisterConsumer(_ chasm.MutableContext, consumerID string) {
	if consumer, ok := s.State.Consumers[consumerID]; ok {
		consumer.Active = false
	}
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
