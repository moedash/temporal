package stream

import (
	"bytes"
	"crypto/sha256"
	"maps"
	"slices"
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

// Stream is a durable, offset-addressed append-only sequence. State holds the
// frontier and the producer and consumer tables, so it is O(producers +
// consumers) no matter how long the stream gets; the payload lives in Batches.
//
// Appending never schedules a workflow task. A stream item is data produced by
// an execution, not a decision input to it, so nothing in a workflow's state
// machine advances because one arrived.
type Stream struct {
	chasm.UnimplementedComponent

	State *streampb.StreamState

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
	Lifecycle *streampb.StreamLifecycle

	// Budget bounds what the stream may hold. Set for a stream a workflow
	// owns, whose batches are the owning execution's mutable state.
	Budget *streampb.StreamBudget

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

	// The namespace's limits, resolved by the caller. A zero value means the
	// defaults.
	Limits Limits
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
	visibility := chasm.NewEmptyField[*chasm.Visibility]()
	if !req.Attached {
		visibility = chasm.NewComponentField(ctx, chasm.NewVisibility(ctx))
	}
	return &Stream{
		Visibility: visibility,
		Batches:    make(chasm.Map[int64, *commonpb.DataBlob]),
		State: &streampb.StreamState{
			Lifecycle: req.Lifecycle,
			Budget:    req.Budget,
			Producers: make(map[string]*streampb.ProducerCursor),
			Consumers: make(map[string]*streampb.ConsumerCursor),
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

// AddMessages assigns a contiguous offset range and writes the bytes into the
// component, so the payload and the frontier commit in one transaction. A torn
// append is therefore not a state anyone can observe.
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
	limits := req.Limits.withDefaults()
	if err := checkBatchBytes(req.Messages, limits); err != nil {
		return AddMessagesResult{}, err
	}
	// A message with no kind is data. Delivery to a workflow drops anything
	// that is not, so a producer leaving the field at its zero value would get
	// an offset for a message no subscriber ever sees. Settled before the batch
	// is marshalled, so a retry hashes the same bytes.
	for _, m := range req.Messages {
		if m.GetKind() == streampb.STREAM_MESSAGE_KIND_UNSPECIFIED {
			m.Kind = streampb.STREAM_MESSAGE_KIND_DATA
		}
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

	// After the retry check, so a known producer is never rejected for room.
	if err := s.checkProducerRoom(req.ProducerID, limits.MaxProducersPerStream); err != nil {
		return AddMessagesResult{}, err
	}
	if err := s.checkBudget(int64(len(req.Messages)), int64(len(blob.Data))); err != nil {
		return AddMessagesResult{}, err
	}

	// Before anything is written, because the alternative is to write and then
	// discover the cap can only be met by deleting bytes a consumer's committed
	// History still refers to. Refusing the write is the honest half of that
	// choice: capacity may constrain what is admitted, and may not quietly take
	// back a workflow's ability to replay a decision it already made.
	if err := s.checkCapRoom(int64(len(req.Messages))); err != nil {
		return AddMessagesResult{}, err
	}

	if req.ExpectedOffset != nil && *req.ExpectedOffset != s.State.HeadOffset {
		return AddMessagesResult{}, serviceerror.NewAlreadyExistsf(
			"expected offset %d but stream head is %d", *req.ExpectedOffset, s.State.HeadOffset)
	}

	first := s.State.HeadOffset
	count := int64(len(req.Messages))

	if s.Batches == nil {
		s.Batches = make(chasm.Map[int64, *commonpb.DataBlob])
	}
	s.Batches[first] = chasm.NewDataField(mctx, blob)

	s.State.HeadOffset = first + count
	s.State.AppendedBytes += int64(len(blob.Data))
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

	s.applyCap()
	result := AddMessagesResult{
		FirstOffset: first,
		NextOffset:  s.State.HeadOffset,
		Count:       count,
		Blob:        blob,
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
				&streampb.StreamNotifyConsumersTask{})
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
) (*streampb.StreamState, error) {
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

// checkBudget refuses an append a budgeted stream cannot hold.
//
// Refused rather than reclaimed: the budget exists because the batches are
// mutable state of the execution that owns the stream, and the alternative to
// refusing here is the execution size limit terminating that workflow later,
// with nothing naming the stream as the cause.
func (s *Stream) checkBudget(count int64, size int64) error {
	budget := s.State.GetBudget()
	if budget == nil {
		return nil
	}
	if limit := budget.GetMaxItems(); limit > 0 && s.State.HeadOffset+count > limit {
		return serviceerror.NewResourceExhaustedf(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
			"stream holds %d of its budget of %d messages; the append of %d does not fit",
			s.State.HeadOffset, limit, count)
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
		return nil, serviceerror.NewInvalidArgumentf(
			"stale producer sequence %d, last accepted is %d", req.Sequence, cursor.Seq)
	}
	// Same sequence. Identical content is a retry; different content is a
	// client bug, and returning the recorded offsets would report success while
	// silently dropping the caller's data.
	if !bytes.Equal(cursor.ContentHash, hash) {
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
func (s *Stream) Truncate(_ chasm.MutableContext, newBase int64) error {
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
				"to replay; deregister it first if those messages are no longer needed",
			newBase, holder, floor)
	}
	s.State.BaseOffset = newBase
	s.reclaim(newBase)
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
func (s *Stream) reclaim(newBase int64) {
	starts := s.batchStarts()
	for i, start := range starts {
		end := s.State.HeadOffset
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		if end > newBase {
			return
		}
		delete(s.Batches, start)
	}
}

// batchStarts returns the batch keys in offset order. Reading and reclaiming
// both need where a batch ends, which is where the next one begins.
func (s *Stream) batchStarts() []int64 {
	return slices.Sorted(maps.Keys(s.Batches))
}

// WindowRequest asks for whatever a reader can be given from an offset.
type WindowRequest struct {
	From        int64
	MaxMessages int32
	Topics      []string
}

// Window is one read's worth: the frontier it was served against, the batches
// covering the range, and the range itself.
type Window struct {
	State  *streampb.StreamState
	Blobs  []*commonpb.DataBlob
	Starts []int64
	To     int64
	Limit  int
	// The execution holding the stream, so a slice built from this window can
	// say which run it came from.
	RunID string
}

// ReadWindow serves a read from the component, so the frontier and the bytes
// come from one view. Read separately they can disagree, because the frontier
// moves while the bytes are being fetched.
func (s *Stream) ReadWindow(ctx chasm.Context, req WindowRequest) (Window, error) {
	if req.From < s.State.BaseOffset {
		return Window{}, serviceerror.NewFailedPreconditionf(
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
		To:    req.From,
		Limit: limit,
		RunID: ctx.ExecutionKey().RunID,
	}
	if req.From == s.State.HeadOffset {
		return w, nil
	}

	// Clip to what the caller can actually be given. One offset is one message,
	// so the bound is exact. Without it a poll for a single message off a long
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

// applyCap advances the readable floor when the stream is over its message cap.
// Evaluated at the end of a successful append rather than by a sweeper: the
// append transition is already writing, so folding the check into it costs
// nothing and keeps the cap tight instead of eventually true.
func (s *Stream) applyCap() {
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
	// messages were written, and keeping its bytes is still the right answer.
	if floor, _, pinned := s.replayFloor(); pinned && newBase > floor {
		newBase = floor
	}
	if newBase <= s.State.BaseOffset {
		return
	}
	s.State.BaseOffset = newBase
	s.reclaim(newBase)
}

// checkCapRoom refuses an append the cap could only absorb by dropping bytes an
// active consumer still needs.
//
// A capped stream with no consumer behaves as before: the oldest messages go.
// The refusal only arrives when honouring the cap and honouring a recorded
// consumption are the same messages, and it names the consumer so the operator
// knows what to do about it.
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
		"stream is at its cap of %d messages and consumer %q still depends on "+
			"offset %d and above to replay; the append would have to delete those "+
			"messages to make room",
		maxItems, holder, floor)
}

// ConsumerRegistration describes a consumer being registered on a stream.
type ConsumerRegistration struct {
	ConsumerID string
	WorkflowID string
	RunID      string
	// Negative means the head as of the registering transition.
	Offset   int64
	External bool
	// Zero means the default.
	MaxConsumers int
}

// RegisterConsumer records an in-workflow consumer, so appends know who to wake
// and retention knows what it may not delete. It returns the offset the
// consumer reads from: the resolved start for a new consumer, and the current
// read position for one that is already registered.
//
// A negative offset means the head as of this transition. Resolving it here,
// against the frontier the same transaction sees, is what makes the recorded
// start a fact rather than a reading taken a moment earlier.
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
	offset := reg.Offset
	if offset < 0 {
		offset = s.State.HeadOffset
	}
	if offset < s.State.BaseOffset {
		return 0, serviceerror.NewFailedPreconditionf(
			"offset %d is below the stream's floor of %d", offset, s.State.BaseOffset)
	}
	if _, known := s.State.Consumers[reg.ConsumerID]; !known &&
		len(s.State.Consumers) >= maxConsumers {
		return 0, serviceerror.NewInvalidArgumentf(
			"stream already has %d consumers, which is the limit", maxConsumers)
	}
	if s.State.Consumers == nil {
		s.State.Consumers = make(map[string]*streampb.ConsumerCursor)
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
			return 0, serviceerror.NewFailedPreconditionf(
				"consumer %q recorded offset %d, and the stream now starts at %d",
				reg.ConsumerID, existing.GetReplayFloor(), s.State.BaseOffset)
		}
		existing.Active = true
		return existing.GetOffset(), nil
	}
	s.State.Consumers[reg.ConsumerID] = &streampb.ConsumerCursor{
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
// and the message cap can reach its messages again. The entry stays, inactive,
// so the same consumer coming back is refused if the stream has moved past
// what its History refers to.
func (s *Stream) DeregisterConsumer(_ chasm.MutableContext, consumerID string) {
	if consumer, ok := s.State.Consumers[consumerID]; ok {
		consumer.Active = false
	}
}

// ForgetConsumer drops a consumer whose run is closed. A closed run never
// replays and never comes back, so unlike a deregistration there is nothing
// left to refuse later.
func (s *Stream) ForgetConsumer(_ chasm.MutableContext, consumerID string) {
	delete(s.State.Consumers, consumerID)
}

func marshalBatch(messages []*streampb.StreamMessage) (*commonpb.DataBlob, error) {
	// The serialized batch is also the producer's deduplication fingerprint, and
	// protobuf map iteration order is not stable. A record carrying payload or
	// message metadata would otherwise hash differently on a retry and be
	// refused as a conflicting duplicate of itself.
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(
		&streampb.StreamMessageBatch{Messages: messages})
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
