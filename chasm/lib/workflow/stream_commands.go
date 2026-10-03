package workflow

import (
	"net/url"
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
)

// DefaultStreamName is the stream a command addresses when it names none.
const DefaultStreamName = stream.DefaultStreamName

// activityStreamPrefix reserves the part of the workflow's stream map that
// holds streams owned by its activities. An activity scheduled by a workflow
// is not a component of its own, so its streams live in the workflow's map
// until it is, and nothing outside the server sees these keys.
const activityStreamPrefix = "activity/"

// ActivityStreamKey is the key a stream owned by one of this workflow's
// activities is held under.
//
// The activity id is escaped because it may itself contain a slash, and
// without that ("a/b", "c") and ("a", "b/c") would share a key.
func ActivityStreamKey(activityID, name string) string {
	return activityStreamPrefix + url.PathEscape(activityID) + "/" + name
}

// IsActivityStreamKey reports whether a name falls in the reserved part of the
// map, which only the activity addressing may reach.
func IsActivityStreamKey(name string) bool {
	return strings.HasPrefix(name, activityStreamPrefix)
}

// CheckWorkflowStreamName refuses a name the workflow itself may not use,
// because it is too long or because it would reach an activity's stream.
func CheckWorkflowStreamName(name string) error {
	if IsActivityStreamKey(name) {
		return serviceerror.NewInvalidArgumentf(
			"stream names starting with %q are reserved for streams activities own",
			activityStreamPrefix)
	}
	return stream.CheckStreamName(name)
}

func (w *Workflow) ownedStreams() stream.OwnedStreams {
	return stream.OwnedStreams{Streams: &w.Streams, Kind: "workflow"}
}

// streamNamed returns the workflow's stream of that name, creating it on first
// use. Implicit creation is deliberate: a workflow publishing to its own output
// should not have to coordinate with anyone about who creates it.
func (w *Workflow) streamNamed(
	ctx chasm.MutableContext,
	name string,
	limits stream.Limits,
) (*stream.Stream, error) {
	if err := CheckWorkflowStreamName(name); err != nil {
		return nil, err
	}
	return w.ownedStreams().Named(ctx, name, limits)
}

// siblingStreamBytes is what every other stream this workflow owns holds,
// streams its activities own included, since they live in the same state.
func (w *Workflow) siblingStreamBytes(ctx chasm.Context, name string) int64 {
	return w.ownedStreams().SiblingBytes(ctx, name)
}
