package stream

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// Cursor is a consuming workflow's position in a stream.
//
// It is a subcomponent of the consumer, not of the stream. That placement is
// the whole point: the consumer's mutable state and its History events commit
// in one transaction, so folding a delivered range into the cursor lands
// atomically with the event that records the range. Holding the cursor on the
// stream instead would make every advance a cross-execution write, and a crash
// between the two writes would either redeliver a range or skip it silently.
type Cursor struct {
	chasm.UnimplementedComponent

	State *streampb.WorkflowStreamCursor
}

type NewCursorRequest struct {
	StreamID string
	// External marks a stream in another execution, whose frontier this
	// workflow is told about rather than reads.
	External     bool
	CollectionID string
	BucketSize   int64

	// Where to start reading. Resolving "from the tail" against the stream's
	// head happens before this is called, so the value recorded here is already
	// a fact rather than a reading that would differ on replay.
	StartOffset int64
}

func NewCursor(_ chasm.MutableContext, req NewCursorRequest) (*Cursor, error) {
	if req.StreamID == "" {
		return nil, serviceerror.NewInvalidArgument("stream id is required")
	}
	if req.CollectionID == "" {
		return nil, serviceerror.NewInvalidArgument("collection id is required")
	}
	if req.BucketSize <= 0 {
		return nil, serviceerror.NewInvalidArgument("bucket size must be positive")
	}
	if req.StartOffset < 0 {
		return nil, serviceerror.NewInvalidArgument("start offset cannot be negative")
	}

	return &Cursor{
		State: &streampb.WorkflowStreamCursor{
			StreamId:     req.StreamID,
			CollectionId: req.CollectionID,
			BucketSize:   req.BucketSize,
			Offset:       req.StartOffset,
			External:     req.External,
			KnownHead:    req.StartOffset,
		},
	}, nil
}

// A cursor lives as long as the workflow holding it. Deregistration is an
// explicit act, not a state the component reaches on its own.
func (c *Cursor) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// Offset is the next offset that has not yet been delivered and folded in.
func (c *Cursor) Offset() int64 {
	return c.State.Offset
}

func (c *Cursor) StreamID() string {
	return c.State.StreamId
}

func (c *Cursor) CollectionID() string {
	return c.State.CollectionId
}

func (c *Cursor) BucketSize() int64 {
	return c.State.BucketSize
}

// StagePending records the range attached to the workflow task now in flight.
//
// A redelivery overwrites whatever was staged before. That is safe because a
// range only becomes history when the task completes: if the previous task
// failed or timed out, nothing was recorded, so the replacement range is the
// first one the workflow will ever have observed at this point.
func (c *Cursor) StagePending(_ chasm.MutableContext, from int64, to int64) error {
	if from < c.State.Offset {
		return serviceerror.NewInvalidArgumentf(
			"cannot deliver from offset %d, cursor is already at %d", from, c.State.Offset)
	}
	if to < from {
		return serviceerror.NewInvalidArgumentf("range end %d precedes range start %d", to, from)
	}

	c.State.PendingFrom = from
	c.State.PendingTo = to
	c.State.HasPending = true
	return nil
}

// Pending reports the staged range. The second result distinguishes "no task in
// flight" from "a task in flight that was given nothing", which are different
// facts: the latter must still be recorded.
func (c *Cursor) Pending() (from int64, to int64, ok bool) {
	if !c.State.HasPending {
		return 0, 0, false
	}
	return c.State.PendingFrom, c.State.PendingTo, true
}

// Commit folds the staged range into the cursor and returns it for recording.
// Caller writes the returned range onto the event that closes the task, in the
// same transaction that persists this advance.
func (c *Cursor) Commit(_ chasm.MutableContext) (from int64, to int64, ok bool) {
	if !c.State.HasPending {
		return 0, 0, false
	}

	from, to = c.State.PendingFrom, c.State.PendingTo
	c.State.Offset = to
	c.State.PendingFrom = 0
	c.State.PendingTo = 0
	c.State.HasPending = false
	return from, to, true
}

// Abandon drops a staged range without advancing, for a task that will never
// complete. The next delivery re-reads from the unchanged cursor.
func (c *Cursor) Abandon(_ chasm.MutableContext) {
	c.State.PendingFrom = 0
	c.State.PendingTo = 0
	c.State.HasPending = false
}

// IsExternal reports whether the stream lives in another execution.
func (c *Cursor) IsExternal() bool {
	return c.State.External
}

// KnownHead is the stream's frontier as last pushed to this workflow.
func (c *Cursor) KnownHead() int64 {
	return c.State.KnownHead
}

// AdvanceKnownHead moves the recorded frontier forward. It never moves back: a
// stale push arriving after a fresher one must not hide offsets already known
// to exist.
func (c *Cursor) AdvanceKnownHead(_ chasm.MutableContext, head int64) {
	if head > c.State.KnownHead {
		c.State.KnownHead = head
	}
}
