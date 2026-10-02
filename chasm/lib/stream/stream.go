package stream

import (
	"bytes"
	"maps"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payload"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Stream is a durable, offset-addressed append-only sequence. State holds the
// frontier and the producer and consumer tables, so it is O(producers +
// consumers) no matter how long the stream gets; the payload lives in Batches.
//
// Appending never schedules a workflow task. A stream item is data produced by
// an execution, not a decision input to it, so nothing in a workflow's state
// machine advances because one arrived.
type Stream struct {
	chasm.UnimplementedComponent

	State *streamlib.StreamState

	// Batches holds the payload, each keyed by the offset it starts at. They are
	// data nodes, so they replicate with the component and are reclaimed with
	// it, and a retry addresses the same key rather than racing it.
	//
	// They also live in mutable state, so the payload counts against the
	// execution size limit of whatever execution holds the stream.
	Batches chasm.Map[int64, *commonpb.DataBlob]

	// Present so streams are listable. Operators need to find them the same way
	// they find workflows, and without this the only way to reach a stream is
	// to already know its ID.
	Visibility chasm.Field[*chasm.Visibility]
}

type NewStreamRequest struct {
	Lifecycle *streamlib.StreamLifecycle

	// Budget bounds what the stream may hold. Set for a stream a workflow
	// owns, whose batches are the owning execution's mutable state.
	Budget *streamlib.StreamBudget

	// Attached means the stream is a subcomponent of another execution rather
	// than a root. CHASM requires a visibility component to be an immediate
	// child of the root, so an attached stream carries none and is found
	// through its owner instead of through ListStreams.
	Attached bool

	// StartOffset is where the stream's offsets begin. Zero for a new stream.
	// A run created by a reset inherits a cursor whose position is written in
	// its History, so the stream it goes on reading has to continue that
	// offset space rather than start over at zero.
	StartOffset int64
}

type AddMessagesRequest struct {
	Records []*streamlib.StreamRecord

	// Optional idempotency. A producer supplies either an identity and
	// sequence, or an expected offset, or neither and accepts at-least-once.
	//
	// The dedup table keeps one entry per producer id, so a producer may have
	// one append in flight at a time: sequences have to arrive in order, and a
	// retry of an earlier sequence after a later one committed is refused.
	// Pipelining needs a distinct producer id per lane.
	ProducerID     string
	Sequence       int64
	ExpectedOffset *int64

	// The namespace's limits, resolved by the caller. A zero value means the
	// defaults.
	Limits Limits

	// What the other streams of the same owner already hold, so the per-owner
	// aggregate can be checked here alongside this stream's own budget. Zero
	// for a standalone stream, which has no siblings.
	SiblingBytes int64
}

type AddMessagesResult struct {
	FirstOffset int64
	NextOffset  int64
	Count       int64

	// True when a retry matched a recorded producer sequence, so nothing was
	// appended and the original offsets are returned.
	Deduplicated bool

	// The bytes this append wrote, so a caller can prime a cache without
	// reading them back. Nil when deduplicated.
	Blob *commonpb.DataBlob
}

func NewStream(ctx chasm.MutableContext, req NewStreamRequest) (*Stream, error) {
	if req.StartOffset < 0 {
		return nil, serviceerror.NewInvalidArgument("start offset cannot be negative")
	}
	visibility := chasm.NewEmptyField[*chasm.Visibility]()
	if !req.Attached {
		visibility = chasm.NewComponentField(ctx, chasm.NewVisibility(ctx))
	}
	return &Stream{
		Visibility: visibility,
		Batches:    make(chasm.Map[int64, *commonpb.DataBlob]),
		State: &streamlib.StreamState{
			HeadOffset: req.StartOffset,
			BaseOffset: req.StartOffset,
			Lifecycle:  req.Lifecycle,
			Budget:     req.Budget,
			Producers:  make(map[string]*streamlib.ProducerCursor),
			Consumers:  make(map[string]*streamlib.ConsumerCursor),
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
	// Encoded rather than wrapped raw: a payload without encoding metadata is
	// not decodable by any SDK data converter, so the reason would reach a
	// reader as opaque bytes.
	return chasm.TerminateComponentResponse{},
		s.CloseAndSchedule(mctx, payload.EncodeString(req.Reason))
}

// Snapshot returns a copy of the frontier for read paths. It is a copy because
// the caller reads it outside the transition that produced it.
func (s *Stream) Snapshot(_ chasm.Context, _ struct{}) (*streamlib.StreamState, error) {
	return common.CloneProto(s.State), nil
}

func (s *Stream) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	if s.State.Closed {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

// AddMessages assigns a contiguous offset range and writes the bytes into the
// component, so the payload and the frontier commit in one transaction. A torn
// append is therefore not a state anyone can observe.
func (s *Stream) AddMessages(
	mctx chasm.MutableContext,
	req AddMessagesRequest,
) (AddMessagesResult, error) {
	if s.State.Closed {
		return AddMessagesResult{}, Refusal(ReasonStreamClosed, "stream is closed")
	}
	if len(req.Records) == 0 {
		return AddMessagesResult{}, serviceerror.NewInvalidArgument("no records to append")
	}
	if len(req.Records) > MaxRecordsPerBatch {
		return AddMessagesResult{}, serviceerror.NewInvalidArgumentf(
			"batch of %d exceeds the limit of %d records", len(req.Records), MaxRecordsPerBatch)
	}
	limits := req.Limits.withDefaults()
	if err := checkBatchBytes(req.Records, limits); err != nil {
		return AddMessagesResult{}, err
	}
	settled := settleKinds(req.Records)
	// Stamped with the transition's time, which is what the retention age is
	// measured from. Without a context there is no clock, and an unstamped
	// batch never ages.
	var now time.Time
	if mctx != nil {
		now = mctx.Now(s)
	}
	blob, err := marshalBatch(settled, now)
	if err != nil {
		return AddMessagesResult{}, err
	}
	// Only a producer that named itself can repeat, so only then is a
	// fingerprint taken, and only then is a declared content hash read. A
	// workflow's own publish names no producer: the task is its boundary, and
	// its codec may have encoded the declared value on the way in.
	var hash []byte
	if req.ProducerID != "" {
		if hash, err = batchFingerprint(settled); err != nil {
			return AddMessagesResult{}, err
		}
	}

	if replay, err := s.checkProducer(req, hash); err != nil || replay != nil {
		if err != nil {
			return AddMessagesResult{}, err
		}
		return *replay, nil
	}

	// After the retry check, so a known producer is never rejected for room.
	if err := s.checkProducerRoom(req.ProducerID, limits.MaxProducersPerStream); err != nil {
		return AddMessagesResult{}, err
	}
	if err := s.checkBudget(
		limits, req.SiblingBytes, int64(len(req.Records)), int64(len(blob.Data))); err != nil {
		return AddMessagesResult{}, err
	}

	// Before anything is written, because the alternative is to write and then
	// discover the cap can only be met by deleting bytes a consumer's committed
	// History still refers to. Refusing the write is the honest half of that
	// choice: capacity may constrain what is admitted, and may not quietly take
	// back a workflow's ability to replay a decision it already made.
	if err := s.checkCapRoom(int64(len(req.Records))); err != nil {
		return AddMessagesResult{}, err
	}
	if err := s.checkByteCap(int64(len(blob.Data))); err != nil {
		return AddMessagesResult{}, err
	}

	if req.ExpectedOffset != nil && *req.ExpectedOffset != s.State.HeadOffset {
		return AddMessagesResult{}, serviceerror.NewAlreadyExistsf(
			"expected offset %d but stream head is %d", *req.ExpectedOffset, s.State.HeadOffset)
	}

	first := s.State.HeadOffset
	count := int64(len(req.Records))

	if s.Batches == nil {
		s.Batches = make(chasm.Map[int64, *commonpb.DataBlob])
	}
	s.Batches[first] = chasm.NewDataField(mctx, blob)

	s.State.HeadOffset = first + count
	s.State.AppendedBytes += int64(len(blob.Data))
	s.State.HeldBytes += int64(len(blob.Data))
	s.State.ChangeSequence++
	if req.ProducerID != "" {
		if s.State.Producers == nil {
			s.State.Producers = make(map[string]*streamlib.ProducerCursor)
		}
		s.State.Producers[req.ProducerID] = &streamlib.ProducerCursor{
			Seq:         req.Sequence,
			FirstOffset: first,
			Count:       count,
			ContentHash: hash,
		}
	}

	s.applyCap(mctx)
	result := AddMessagesResult{
		FirstOffset: first,
		NextOffset:  s.State.HeadOffset,
		Count:       count,
		Blob:        blob,
	}
	s.notifyConsumers(mctx)
	s.scheduleAgeCheck(mctx)
	return result, nil
}

// scheduleAgeCheck arms the retention age check for a stream whose lifecycle
// has one. One task is outstanding at a time: the check re-arms itself while
// the stream holds records and lowers the flag when it holds none, so the
// next append arms it again. The first check waits a full retention, since
// nothing can have aged before then.
func (s *Stream) scheduleAgeCheck(mctx chasm.MutableContext) {
	if mctx == nil || s.State.AgeTaskPending {
		return
	}
	retention := s.State.GetLifecycle().GetRetention().AsDuration()
	if retention <= 0 {
		return
	}
	s.State.AgeTaskPending = true
	mctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: mctx.Now(s).Add(retention)},
		&streamlib.StreamAgeTask{})
}

// RunAgeCheck is the age task's transition: reclaim what has aged past the
// retention, then re-arm for the next batch to age or stand down when nothing
// is held. Re-arming waits at least the recheck interval, so a stream that
// keeps taking records is checked on that cadence rather than per batch.
func (s *Stream) RunAgeCheck(mctx chasm.MutableContext, recheck time.Duration) error {
	now := mctx.Now(s)
	next, err := s.TruncateAged(mctx, now)
	if err != nil {
		return err
	}
	if s.State.Closed || s.held() == 0 {
		s.State.AgeTaskPending = false
		return nil
	}
	at := now.Add(recheck)
	if next.After(at) {
		at = next
	}
	s.State.AgeTaskPending = true
	mctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: at}, &streamlib.StreamAgeTask{})
	return nil
}

// TruncateAged advances the floor past every batch older than the lifecycle's
// retention as of now, and reports when the next batch ages out, or now when
// an aged batch is held by an active consumer's floor and has to be asked
// about again. The zero time means nothing is waiting to age.
//
// A closed stream is left alone: its retention counts down to deletion from
// the close instead, and a consumer draining it is owed the tail.
func (s *Stream) TruncateAged(mctx chasm.MutableContext, now time.Time) (time.Time, error) {
	retention := s.State.GetLifecycle().GetRetention().AsDuration()
	if retention <= 0 || s.State.Closed {
		return time.Time{}, nil
	}
	floor, _, pinned := s.replayFloor()
	newBase := s.State.BaseOffset
	var next time.Time
	starts := s.batchStarts()
	for i, start := range starts {
		end := s.State.HeadOffset
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		if end <= s.State.BaseOffset {
			continue
		}
		appended, ok, err := s.batchAppendedAt(mctx, start)
		if err != nil {
			return time.Time{}, err
		}
		if !ok {
			// Written before batches were stamped. It never ages, and
			// nothing behind it can be reached either.
			break
		}
		if expires := appended.Add(retention); expires.After(now) {
			next = expires
			break
		}
		if pinned && end > floor {
			// Aged, but a workflow's History still refers to it. The floor
			// moves up to the consumer's and no further; asked again later,
			// in case the consumer has let go by then.
			newBase = max(newBase, min(end, floor))
			next = now
			break
		}
		newBase = end
	}
	if newBase > s.State.BaseOffset {
		s.State.BaseOffset = newBase
		s.reclaim(mctx, newBase)
	}
	return next, nil
}

// batchAppendedAt reads when the batch keyed by start was written, reporting
// false for a batch written before batches carried the time.
func (s *Stream) batchAppendedAt(ctx chasm.Context, start int64) (time.Time, bool, error) {
	field, ok := s.Batches[start]
	if !ok {
		return time.Time{}, false, nil
	}
	var batch streamlib.StreamRecordBatch
	if err := proto.Unmarshal(field.Get(ctx).GetData(), &batch); err != nil {
		return time.Time{}, false, err
	}
	if batch.GetAppendedAt() == nil {
		return time.Time{}, false, nil
	}
	return batch.GetAppendedAt().AsTime(), true, nil
}

// notifyConsumers schedules the wake for consumers this append left behind.
//
// Only a workflow in another execution needs it. One consuming a stream it owns
// sees the new frontier while closing its own transaction, so waking it through
// a task would only duplicate a decision already made locally.
func (s *Stream) notifyConsumers(mctx chasm.MutableContext) {
	// Without a context there is no transition to attach the task to. The
	// component's own unit tests drive the transitions that way.
	if mctx == nil {
		return
	}
	// One task outstanding at a time. The task reads the head when it runs and
	// clears the flag before it reads, so an append that lands after the clear
	// schedules the next one and nothing is missed.
	if s.State.NotifyPending {
		return
	}
	for _, consumer := range s.State.Consumers {
		if consumer.GetExternal() && consumer.GetActive() && consumer.GetOffset() < s.State.HeadOffset {
			s.State.NotifyPending = true
			mctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: mctx.Now(s)},
				&streamlib.StreamNotifyConsumersTask{})
			return
		}
	}
}

// TakeNotifySnapshot is the notify task's read of the stream. Clearing the
// pending flag in the same transition that reads the head is what makes the
// coalescing safe: any append that commits after this one sees the flag down
// and schedules its own task.
func (s *Stream) TakeNotifySnapshot(
	_ chasm.MutableContext, _ struct{},
) (*streamlib.StreamState, error) {
	s.State.NotifyPending = false
	return common.CloneProto(s.State), nil
}

// checkProducerRoom keeps the dedup table bounded.
//
// Entries whose whole batch sits below the floor go first: a retry of a batch
// that truncation already removed cannot be served its recorded offsets
// anyway, so the entry has no use left.
func (s *Stream) checkProducerRoom(producerID string, maxProducers int) error {
	if producerID == "" {
		return nil
	}
	if _, known := s.State.Producers[producerID]; known {
		return nil
	}
	if len(s.State.Producers) < maxProducers {
		return nil
	}
	for id, cursor := range s.State.Producers {
		if cursor.GetFirstOffset()+cursor.GetCount() <= s.State.GetBaseOffset() {
			delete(s.State.Producers, id)
		}
	}
	if len(s.State.Producers) >= maxProducers {
		return serviceerror.NewInvalidArgumentf(
			"stream already tracks %d producers, which is the limit", maxProducers)
	}
	return nil
}

// held is how many records the stream still has. Offsets are global and a
// stream can begin above zero, so the head alone is a position rather than an
// amount.
func (s *Stream) held() int64 {
	return s.State.HeadOffset - s.State.BaseOffset
}

// checkBudget refuses an append a budgeted stream cannot hold.
//
// Refused rather than reclaimed: the budget exists because the batches are
// mutable state of the execution that owns the stream, and the alternative to
// refusing here is the execution size limit terminating that workflow later,
// with nothing naming the stream as the cause.
func (s *Stream) checkBudget(limits Limits, siblingBytes, count, size int64) error {
	budget := s.State.GetBudget()
	if budget == nil {
		return nil
	}
	// The per-stream budget bounds one stream, and one execution can own many.
	// Multiplied out they come to far more than the execution size limit, so
	// the aggregate is what keeps that limit from terminating the workflow.
	if limit := int64(limits.OwnedStreamsMaxBytesPerWorkflow); limit > 0 &&
		siblingBytes+s.State.AppendedBytes+size > limit {
		return serviceerror.NewResourceExhaustedf(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
			"the workflow's streams hold %d of their shared budget of %d bytes; the append "+
				"of %d does not fit",
			siblingBytes+s.State.AppendedBytes, limit, size)
	}
	if limit := budget.GetMaxItems(); limit > 0 && s.held()+count > limit {
		return serviceerror.NewResourceExhaustedf(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
			"stream holds %d of its budget of %d records; the append of %d does not fit",
			s.held(), limit, count)
	}
	if limit := budget.GetMaxBytes(); limit > 0 && s.State.AppendedBytes+size > limit {
		return serviceerror.NewResourceExhaustedf(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
			"stream holds %d of its budget of %d bytes; the append of %d does not fit",
			s.State.AppendedBytes, limit, size)
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
		return nil, Refusal(ReasonProducerStaleSequence,
			"stale producer sequence %d, last accepted for producer %q is %d; a producer id "+
				"carries one append at a time, so use a separate id per concurrent lane",
			req.Sequence, req.ProducerID, cursor.Seq)
	}
	// Same sequence. Identical content is a retry; different content is a
	// client bug, and returning the recorded offsets would report success while
	// silently dropping the caller's data.
	if !bytes.Equal(cursor.ContentHash, hash) {
		return nil, Refusal(ReasonProducerConflict,
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
		s.State.Producers = make(map[string]*streamlib.ProducerCursor)
	}
	cursor := s.State.Producers[producerID]
	if cursor == nil {
		cursor = &streamlib.ProducerCursor{Seq: -1}
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
	s.State.ChangeSequence++

	retention := s.State.GetLifecycle().GetRetention().AsDuration()
	if retention <= 0 {
		return time.Time{}
	}
	return now.Add(retention)
}

// CloseAndSchedule closes the stream and arms retention if it asked for it.
func (s *Stream) CloseAndSchedule(mctx chasm.MutableContext, reason *commonpb.Payload) error {
	if at := s.Close(mctx.Now(s), reason); !at.IsZero() {
		mctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: at}, &streamlib.StreamRetentionTask{})
	}
	return nil
}

// Truncate advances the readable floor.
//
// It stops at an active consumer's replay floor. A workflow that consumed a
// range recorded that range in its History and can be asked to replay from it,
// so those bytes are part of its recovery rather than spare capacity. Dropping
// them succeeds here and fails much later, during a replay nobody is watching,
// which is the worst place to find out.
//
// The floor is released by deregistering the consumer, which is an act someone
// takes deliberately. An operator who means to drop the bytes anyway does that
// first, and then this call goes through.
//
// A consumer that is behind but not active is not protected. Reading from
// below the base is an error naming where the stream now starts, the same
// answer a log with a retention window gives anywhere else.
func (s *Stream) Truncate(mctx chasm.MutableContext, newBase int64) error {
	if newBase < s.State.BaseOffset {
		return serviceerror.NewInvalidArgumentf(
			"cannot truncate backwards from %d to %d", s.State.BaseOffset, newBase)
	}
	if newBase > s.State.HeadOffset {
		return serviceerror.NewInvalidArgumentf(
			"cannot truncate past head offset %d", s.State.HeadOffset)
	}
	if floor, holder, pinned := s.replayFloor(); pinned && newBase > floor {
		return serviceerror.NewFailedPreconditionf(
			"cannot truncate to %d: consumer %q still depends on offset %d and above "+
				"to replay; deregister it first if those records are no longer needed",
			newBase, holder, floor)
	}
	s.State.BaseOffset = newBase
	s.reclaim(mctx, newBase)
	return nil
}

// replayFloor is the oldest offset any active consumer's History still depends
// on, and who is holding it. Named, because a refusal that does not say which
// consumer to look at leaves the operator with nothing to act on.
func (s *Stream) replayFloor() (int64, string, bool) {
	var floor int64
	var holder string
	found := false
	for id, c := range s.State.Consumers {
		if !c.GetActive() {
			continue
		}
		if !found || c.GetReplayFloor() < floor {
			floor = c.GetReplayFloor()
			holder = id
			found = true
		}
	}
	return floor, holder, found
}

// reclaim drops batches lying entirely below the readable floor. A batch
// straddling the floor stays, because the offsets above it are still readable.
//
// Each dropped batch is read once for its size, so the held bytes stay exact.
// That read is the price of keeping the state free of a per-batch index, and
// it is paid on a batch that is being deleted anyway.
func (s *Stream) reclaim(ctx chasm.Context, newBase int64) {
	starts := s.batchStarts()
	for i, start := range starts {
		end := s.State.HeadOffset
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		if end > newBase {
			return
		}
		s.State.HeldBytes = max(0, s.State.HeldBytes-s.batchBytes(ctx, start))
		delete(s.Batches, start)
	}
}

// batchBytes is the stored size of the batch keyed by start.
func (s *Stream) batchBytes(ctx chasm.Context, start int64) int64 {
	field, ok := s.Batches[start]
	if !ok {
		return 0
	}
	return int64(len(field.Get(ctx).GetData()))
}

// batchStarts returns the batch keys in offset order. Reading and reclaiming
// both need where a batch ends, which is where the next one begins.
func (s *Stream) batchStarts() []int64 {
	return slices.Sorted(maps.Keys(s.Batches))
}

// WindowRequest asks for whatever a reader can be given from an offset.
type WindowRequest struct {
	From int64
	// When set, where the read begins instead of From, resolved against the
	// same view the read is served from so it cannot race with truncation.
	Start       *streampb.StreamStartPosition
	MaxMessages int32
	Topics      []string
}

// Window is one read's worth: the frontier it was served against, the batches
// covering the range, and the range itself.
type Window struct {
	State  *streamlib.StreamState
	Blobs  []*commonpb.DataBlob
	Starts []int64
	// Where the read began, which is the request's From unless it named a
	// start position.
	From  int64
	To    int64
	Limit int
	// The execution holding the stream, so a slice built from this window can
	// say which run it came from.
	RunID string
}

// ReadWindow serves a read from the component, so the frontier and the bytes
// come from one view. Read separately they can disagree, because the frontier
// moves while the bytes are being fetched.
func (s *Stream) ReadWindow(ctx chasm.Context, req WindowRequest) (Window, error) {
	if req.Start != nil {
		from, err := s.resolveStart(req.Start)
		if err != nil {
			return Window{}, err
		}
		req.From = from
	}
	if req.From < s.State.BaseOffset {
		return Window{}, Refusal(ReasonCursorBelowFloor,
			"offset %d has been truncated, the stream starts at %d", req.From, s.State.BaseOffset)
	}
	if req.From > s.State.HeadOffset {
		return Window{}, serviceerror.NewInvalidArgumentf(
			"offset %d is past the stream head %d", req.From, s.State.HeadOffset)
	}

	// Clamped at both ends. The caller picks the page size, and an unclamped
	// one lets a single poll ask the store to materialise the whole stream.
	limit := int(req.MaxMessages)
	if limit <= 0 || limit > DefaultMaxMessagesPerPoll {
		limit = DefaultMaxMessagesPerPoll
	}
	w := Window{
		State: common.CloneProto(s.State),
		From:  req.From,
		To:    req.From,
		Limit: limit,
		RunID: ctx.ExecutionKey().RunID,
	}
	if req.From == s.State.HeadOffset {
		return w, nil
	}

	// Clip to what the caller can actually be given. One offset is one record,
	// so the bound is exact. Without it a poll for a single record off a long
	// stream materialises every batch to the head before trimming.
	w.To = min(s.State.HeadOffset, req.From+int64(limit))
	blobs, starts, err := s.ReadBatches(ctx, req.From, w.To, 0)
	if err != nil {
		return Window{}, err
	}
	w.Blobs, w.Starts = blobs, starts
	return w, nil
}

// ReadBatches returns the batches covering [from, to), oldest first, alongside
// the offset each one starts at. A read landing mid-batch gets the batch
// holding it, because a consumer asks for an offset rather than for a batch.
// Blobs come back unparsed: decoding user payloads is the SDK's job.
func (s *Stream) ReadBatches(
	ctx chasm.Context,
	from int64,
	to int64,
	maxBatches int,
) ([]*commonpb.DataBlob, []int64, error) {
	if from >= to {
		return nil, nil, nil
	}
	var blobs []*commonpb.DataBlob
	var starts []int64
	all := s.batchStarts()
	for i, start := range all {
		end := s.State.HeadOffset
		if i+1 < len(all) {
			end = all[i+1]
		}
		if end <= from {
			continue
		}
		if start >= to {
			break
		}
		blobs = append(blobs, s.Batches[start].Get(ctx))
		starts = append(starts, start)
		if maxBatches > 0 && len(blobs) >= maxBatches {
			break
		}
	}
	return blobs, starts, nil
}

// applyCap advances the readable floor when the stream is over its record cap.
// Evaluated at the end of a successful append rather than by a sweeper: the
// append transition is already writing, so folding the check into it costs
// nothing and keeps the cap tight instead of eventually true.
func (s *Stream) applyCap(ctx chasm.Context) {
	maxItems := s.State.GetLifecycle().GetMaxItems()
	if maxItems <= 0 {
		return
	}
	readable := s.State.HeadOffset - s.State.BaseOffset
	if readable <= maxItems {
		return
	}
	newBase := s.State.HeadOffset - maxItems
	// Clamped rather than refused, because refusing belongs to admission and
	// has already happened: checkCapRoom turned away the append that would have
	// needed this. Reaching the clamp means a consumer registered after the
	// records were written, and keeping its bytes is still the right answer.
	if floor, _, pinned := s.replayFloor(); pinned && newBase > floor {
		newBase = floor
	}
	if newBase <= s.State.BaseOffset {
		return
	}
	s.State.BaseOffset = newBase
	s.reclaim(ctx, newBase)
}

// checkByteCap refuses an append that would take the held bytes past the
// lifecycle's byte cap.
//
// Refused rather than reclaimed, unlike the record cap: reclaiming by bytes
// would have to read the oldest batches to learn what dropping them frees, on
// the hot path of every append. Room comes back as the record cap, an explicit
// truncation or the retention age reclaims behind the floor, and the refusal
// is the same one a budgeted stream gives, so a client handles both alike.
func (s *Stream) checkByteCap(size int64) error {
	maxBytes := s.State.GetLifecycle().GetMaxBytes()
	if maxBytes <= 0 || s.State.HeldBytes+size <= maxBytes {
		return nil
	}
	return serviceerror.NewResourceExhaustedf(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
		"stream holds %d of its cap of %d bytes; the append of %d does not fit",
		s.State.HeldBytes, maxBytes, size)
}

// checkCapRoom refuses an append the cap could only absorb by dropping bytes an
// active consumer still needs.
//
// A capped stream with no consumer behaves as before: the oldest records go.
// The refusal only arrives when honouring the cap and honouring a recorded
// consumption are the same records, and it names the consumer so the operator
// knows what to do about it.
//
// The floor is the subscription's start and does not move while the consumer
// is active, because replay re-reads every range the consumer's History
// recorded, back to that start. So on a stream with a subscriber the cap is a
// lifetime quota rather than a rolling window, and a consumer that has read
// everything still holds the writers. Deregistering the consumer releases it.
// The two promises cannot both hold, and the one kept is that a workflow can
// always replay a decision it already made.
func (s *Stream) checkCapRoom(count int64) error {
	maxItems := s.State.GetLifecycle().GetMaxItems()
	if maxItems <= 0 {
		return nil
	}
	wantBase := s.State.HeadOffset + count - maxItems
	if wantBase <= s.State.BaseOffset {
		return nil
	}
	floor, holder, pinned := s.replayFloor()
	if !pinned || wantBase <= floor {
		return nil
	}
	return serviceerror.NewResourceExhaustedf(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_UNSPECIFIED,
		"stream is at its cap of %d records and consumer %q still depends on "+
			"offset %d and above to replay; the append would have to delete those "+
			"records to make room",
		maxItems, holder, floor)
}

// ConsumerRegistration describes a consumer being registered on a stream.
type ConsumerRegistration struct {
	ConsumerID string
	WorkflowID string
	RunID      string
	// Where a new consumer starts, resolved against the registering
	// transition's frontier. One already registered keeps its position.
	Start    *streampb.StreamStartPosition
	External bool
	// Zero means the default.
	MaxConsumers int
}

// RegisterConsumer records an in-workflow consumer, so appends know who to wake
// and retention knows what it may not delete. It returns the offset the
// consumer reads from: the resolved start for a new consumer, and the current
// read position for one that is already registered.
//
// Resolving the start here, against the frontier the same transaction sees, is
// what makes the recorded start a fact rather than a reading taken a moment
// earlier.
//
// The floor it records is where the subscription started, not where it has read
// to. The ranges this consumer already took are written into its History, and a
// replay is asked to reproduce them, so the bytes behind the read position are
// the ones a recovery needs. Deregistering releases the floor.
func (s *Stream) RegisterConsumer(_ chasm.MutableContext, reg ConsumerRegistration) (int64, error) {
	if reg.ConsumerID == "" {
		return 0, serviceerror.NewInvalidArgument("consumer id is required")
	}
	maxConsumers := reg.MaxConsumers
	if maxConsumers <= 0 {
		maxConsumers = MaxConsumersPerStream
	}
	offset, err := s.resolveStart(reg.Start)
	if err != nil {
		return 0, err
	}
	if offset < s.State.BaseOffset {
		return 0, Refusal(ReasonCursorBelowFloor,
			"offset %d is below the stream's floor of %d", offset, s.State.BaseOffset)
	}
	if _, known := s.State.Consumers[reg.ConsumerID]; !known &&
		len(s.State.Consumers) >= maxConsumers {
		return 0, serviceerror.NewInvalidArgumentf(
			"stream already has %d consumers, which is the limit", maxConsumers)
	}
	if s.State.Consumers == nil {
		s.State.Consumers = make(map[string]*streamlib.ConsumerCursor)
	}
	// One workflow id has one open run, so an entry for another run of this
	// workflow belongs to a closed run. Its floor would otherwise hold storage
	// for a replay nobody can ask for, and the new run would inherit it.
	for id, c := range s.State.Consumers {
		if id != reg.ConsumerID && c.GetExternal() &&
			c.GetWorkflowId() == reg.WorkflowID && c.GetRunId() != reg.RunID {
			delete(s.State.Consumers, id)
		}
	}
	if existing, ok := s.State.Consumers[reg.ConsumerID]; ok {
		// A consumer coming back after its floor was released can find the
		// stream has moved past what its History refers to. Saying so here is
		// the only chance to say it before the workflow depends on it again.
		if existing.GetReplayFloor() < s.State.BaseOffset {
			return 0, Refusal(ReasonCursorBelowFloor,
				"consumer %q recorded offset %d, and the stream now starts at %d",
				reg.ConsumerID, existing.GetReplayFloor(), s.State.BaseOffset)
		}
		existing.Active = true
		return existing.GetOffset(), nil
	}
	s.State.Consumers[reg.ConsumerID] = &streamlib.ConsumerCursor{
		WorkflowId:  reg.WorkflowID,
		RunId:       reg.RunID,
		Offset:      offset,
		Active:      true,
		External:    reg.External,
		ReplayFloor: offset,
	}
	return offset, nil
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

// DeregisterConsumer releases the floor a consumer was holding, so retention
// and the record cap can reach its records again. The entry stays, inactive,
// so the same consumer coming back is refused if the stream has moved past
// what its History refers to.
func (s *Stream) DeregisterConsumer(_ chasm.MutableContext, consumerID string) {
	if consumer, ok := s.State.Consumers[consumerID]; ok {
		consumer.Active = false
	}
}

// ForgetConsumer drops a consumer whose run is closed, releasing its floor.
//
// A closed run never takes another workflow task, so nothing will ask for a
// range it has not already been given. It can still be replayed: a query
// against a completed consumer runs on a worker that may never have seen it,
// and a reset can branch from it. Neither is served once truncation has taken
// the bytes, so both are refused with the offset the stream now starts at
// rather than answered with a hole.
func (s *Stream) ForgetConsumer(_ chasm.MutableContext, consumerID string) {
	delete(s.State.Consumers, consumerID)
}

// settleKinds returns the batch with every unspecified kind read as data. A
// producer that never heard of kinds leaves the field at its zero value, and
// every reader would otherwise have to agree on what that means.
//
// The records belong to the caller's request, which on the command path is the
// worker's own proto, so a record that needs settling is copied rather than
// written through. Settled before the batch is marshalled, so a retry hashes
// the same bytes.
func settleKinds(records []*streamlib.StreamRecord) []*streamlib.StreamRecord {
	out := make([]*streamlib.StreamRecord, len(records))
	for i, m := range records {
		if m.GetKind() != streampb.STREAM_RECORD_KIND_UNSPECIFIED {
			out[i] = m
			continue
		}
		settled := common.CloneProto(m)
		settled.Kind = streampb.STREAM_RECORD_KIND_DATA
		out[i] = settled
	}
	return out
}

// marshalBatch serializes a batch for storage, stamped with when it was
// appended. The stamp is not part of the producer's fingerprint, which is
// taken over the records alone, so a retry hashes the same however late it
// arrives.
func marshalBatch(records []*streamlib.StreamRecord, appendedAt time.Time) (*commonpb.DataBlob, error) {
	batch := &streamlib.StreamRecordBatch{Records: records}
	if !appendedAt.IsZero() {
		batch.AppendedAt = timestamppb.New(appendedAt)
	}
	data, err := marshalDeterministic(batch)
	if err != nil {
		return nil, err
	}
	return &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         data,
	}, nil
}

func marshalDeterministic(m proto.Message) ([]byte, error) {
	return (proto.MarshalOptions{Deterministic: true}).Marshal(m)
}
