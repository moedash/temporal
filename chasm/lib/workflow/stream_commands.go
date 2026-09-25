package workflow

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// DefaultStreamName is the stream a command addresses when it names none.
const DefaultStreamName = "output"

// streamNamed returns the workflow's stream of that name, creating it on first
// use. Implicit creation is deliberate: a workflow publishing to its own output
// should not have to coordinate with anyone about who creates it.
func (w *Workflow) streamNamed(
	ctx chasm.MutableContext,
	name string,
	limits stream.Limits,
) (*stream.Stream, error) {
	if w.Streams == nil {
		w.Streams = make(chasm.Map[string, *stream.Stream])
	}
	if field, ok := w.Streams[name]; ok {
		return field.Get(ctx), nil
	}

	// Checked only on the create path, so an existing stream is never refused
	// for room. The name arrives from the caller and every distinct one adds a
	// component to this execution's mutable state, so without a bound an
	// outside writer can grow that state until the size limit terminates the
	// workflow.
	if len(name) > stream.MaxStreamNameLength {
		return nil, serviceerror.NewInvalidArgumentf(
			"stream name is %d characters, over the %d limit", len(name), stream.MaxStreamNameLength)
	}
	if len(w.Streams) >= limits.MaxOwnedStreamsPerWorkflow {
		return nil, serviceerror.NewFailedPreconditionf(
			"workflow already owns %d streams, the limit", limits.MaxOwnedStreamsPerWorkflow)
	}

	// Budgeted, because the batches live in this execution's mutable state and
	// the size limit on that terminates the workflow instead of refusing.
	created, err := stream.NewStream(ctx, stream.NewStreamRequest{
		Attached: true,
		Budget: &streamlib.StreamBudget{
			MaxItems: int64(limits.OwnedStreamMaxItems),
			MaxBytes: int64(limits.OwnedStreamMaxBytes),
		},
	})
	if err != nil {
		return nil, err
	}
	w.Streams[name] = chasm.NewComponentField(ctx, created)
	return created, nil
}
