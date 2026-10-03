package stream

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// DefaultStreamName is the stream an owner's writer or reader addresses when it
// names none.
const DefaultStreamName = "output"

// Owner is an execution that holds streams by key. An attached stream has no
// id of its own, so every read and write from outside the execution goes
// through one of these, whatever kind of execution it is.
type Owner interface {
	// OwnedStream returns the stream under key, or nil when nothing has
	// created it yet.
	OwnedStream(ctx chasm.Context, key string) *Stream

	// OwnedStreamEnded reports that nothing more can be appended under key
	// because the owner itself is finished, whether or not the stream was
	// closed. A reader tailing it is released instead of parked forever.
	OwnedStreamEnded(ctx chasm.Context, key string) bool

	// AppendToOwnedStream appends on behalf of a writer outside the execution,
	// creating the stream on first write.
	AppendToOwnedStream(
		mctx chasm.MutableContext, key string, req AddMessagesRequest,
	) (AddMessagesResult, error)
}

// OwnedStreams is the map of streams one execution owns. Every kind of owner
// keeps one, so the create path and the shared budget are written once.
type OwnedStreams struct {
	Streams *chasm.Map[string, *Stream]

	// Kind names the owner in refusals, so a caller learns which execution
	// ran out of room.
	Kind string
}

// Named returns the stream under key, creating it on first use. Implicit
// creation is deliberate: an owner publishing to its own output should not
// have to coordinate with anyone about who creates it.
func (o OwnedStreams) Named(
	ctx chasm.MutableContext,
	key string,
	limits Limits,
) (*Stream, error) {
	if *o.Streams == nil {
		*o.Streams = make(chasm.Map[string, *Stream])
	}
	if field, ok := (*o.Streams)[key]; ok {
		return field.Get(ctx), nil
	}
	limits = limits.withDefaults()

	// Checked only on the create path, so an existing stream is never refused
	// for room. The key arrives from the caller and every distinct one adds a
	// component to this execution's mutable state, so without a bound an
	// outside writer can grow that state until the size limit ends the owner.
	if len(*o.Streams) >= limits.MaxOwnedStreamsPerWorkflow {
		return nil, serviceerror.NewFailedPreconditionf(
			"%s already owns %d streams, the limit", o.Kind, limits.MaxOwnedStreamsPerWorkflow)
	}

	// Budgeted, because the batches live in this execution's mutable state and
	// the size limit on that ends the execution instead of refusing.
	created, err := NewStream(ctx, NewStreamRequest{
		Attached: true,
		Budget: &streamlib.StreamBudget{
			MaxItems: int64(limits.OwnedStreamMaxItems),
			MaxBytes: int64(limits.OwnedStreamMaxBytes),
		},
	})
	if err != nil {
		return nil, err
	}
	(*o.Streams)[key] = chasm.NewComponentField(ctx, created)
	return created, nil
}

// Get returns the stream under key, or nil when nothing has created it.
func (o OwnedStreams) Get(ctx chasm.Context, key string) *Stream {
	field, ok := (*o.Streams)[key]
	if !ok {
		return nil
	}
	return field.Get(ctx)
}

// SiblingBytes is what every other stream of this owner holds.
//
// The per-stream budget bounds one stream, and an outside writer can name as
// many as MaxOwnedStreamsPerWorkflow allows. Their sum is what the execution
// size limit sees, so an append has to be measured against the sum.
//
// Skipped for the common case of a single stream, where the sum is the stream
// itself and reading the others would load state nothing else needs.
func (o OwnedStreams) SiblingBytes(ctx chasm.Context, key string) int64 {
	if len(*o.Streams) < 2 {
		return 0
	}
	var total int64
	for other, field := range *o.Streams {
		if other == key {
			continue
		}
		total += field.Get(ctx).State.GetAppendedBytes()
	}
	return total
}

// Append appends under key, creating the stream on first write, and measures
// the batch against the owner's shared budget as well as the stream's own.
func (o OwnedStreams) Append(
	mctx chasm.MutableContext,
	key string,
	req AddMessagesRequest,
) (AddMessagesResult, error) {
	s, err := o.Named(mctx, key, req.Limits)
	if err != nil {
		return AddMessagesResult{}, err
	}
	req.SiblingBytes = o.SiblingBytes(mctx, key)
	return s.AddMessages(mctx, req)
}

// CheckStreamName refuses a name too long to become a key in mutable state.
func CheckStreamName(name string) error {
	if len(name) > MaxStreamNameLength {
		return serviceerror.NewInvalidArgumentf(
			"stream name is %d characters, over the %d limit", len(name), MaxStreamNameLength)
	}
	return nil
}
