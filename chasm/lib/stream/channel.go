package stream

import (
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/payload"
)

// A change to a stream notifies the notification channel named by the stream,
// so callback and polling listeners learn of a native stream the way they
// learn of an external one. The name is derived from the stream's identity
// alone, so a client derives the same name without asking the server. The
// workflow consumers of a stream keep their cursors and are not told twice.

// ChannelNamePrefix starts the name of every channel a stream drives.
const ChannelNamePrefix = "stream/"

// ChannelName is the independent channel a standalone stream notifies.
func ChannelName(streamID string) string {
	return ChannelNamePrefix + streamID
}

// OwnedChannelName is the channel, linked to the owning execution, that a
// stream a workflow or a standalone activity owns notifies.
func OwnedChannelName(name string) string {
	return ChannelNamePrefix + name
}

// ActivityChannelName is the channel, linked to the workflow run, that a
// stream an activity of that workflow owns notifies. The activity has no
// state of its own to hold a channel, so the name carries the activity id.
func ActivityChannelName(activityID, name string) string {
	return ChannelNamePrefix + activityID + "/" + name
}

// OwnedChannelAddress is where a stream an execution owns announces its
// changes: the channel's name and the execution the channel is linked to. A
// client derives it from the stream's owner alone, so the server and the
// SDKs agree on it without asking.
func OwnedChannelAddress(owner *streamlib.StreamOwner, name string) (string, *commonpb.Execution) {
	linkedTo := &commonpb.Execution{
		Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
		BusinessId: owner.GetId(),
		RunId:      owner.GetRunId(),
	}
	switch owner.GetKind() {
	case streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY:
		return ActivityChannelName(owner.GetActivityId(), name), linkedTo
	case streamlib.STREAM_OWNER_KIND_ACTIVITY:
		linkedTo.Type = enumspb.EXECUTION_TYPE_ACTIVITY
		return OwnedChannelName(name), linkedTo
	default:
		return OwnedChannelName(name), linkedTo
	}
}

// ClosedMetadataKey is set, to true, on the notification for a close.
const ClosedMetadataKey = "closed"

// ClosedMetadata is the metadata of a close notification.
func ClosedMetadata() map[string]*commonpb.Payload {
	// A bool encodes without error.
	closed, _ := payload.Encode(true)
	return map[string]*commonpb.Payload{ClosedMetadataKey: closed}
}

// ChangeNotification describes the stream's latest change for its channel:
// the change sequence is the counter, the head after the change is the
// position in the form a native cursor names one, and a close says so.
//
// The run is what a native cursor pairs the offset with: the owning run of an
// attached stream, and the stream's own id for a standalone one, which has no
// run a client would know.
func (s *Stream) ChangeNotification(channel, run string) *channelpb.Notification {
	n := &channelpb.Notification{
		Channel:  channel,
		Counter:  s.State.GetChangeSequence(),
		Position: EncodePosition(run, s.State.GetHeadOffset()),
	}
	if s.State.GetClosed() {
		n.Metadata = ClosedMetadata()
	}
	return n
}

// EncodePosition is a native position as a client's cursor names it.
func EncodePosition(run string, offset int64) []byte {
	return []byte(run + ":" + strconv.FormatInt(offset, 10))
}

// ScheduleChannelNotify arms a task to hand the latest change to the stream's
// channel, for a standalone stream, whose channel is an execution of its own.
// One task is outstanding at a time and reads the latest change when it runs.
// Tasks of one execution may run in any order, and a change arriving at the
// channel below one it already holds is folded away, so a task per change
// would lose the earlier ones from the ring; one task at a time keeps the
// counters the channel sees in order, with a burst arriving as its newest.
func (s *Stream) ScheduleChannelNotify(
	mctx chasm.MutableContext,
	channel, run string,
	limits Limits,
) {
	if mctx == nil || limits.ChannelNotifyOff || s.State.GetChannelNotifyPending() {
		return
	}
	s.State.ChannelNotifyPending = true
	mctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: mctx.Now(s)},
		&streamlib.StreamNotifyChannelTask{Channel: channel, Run: run})
}

// TakeChannelChange is the channel task's read of the stream. Lowering the
// flag in the transition that reads the change is what makes the coalescing
// safe: a change that commits after this one sees the flag down and arms the
// next task.
func (s *Stream) TakeChannelChange(
	_ chasm.MutableContext,
	task *streamlib.StreamNotifyChannelTask,
) (*channelpb.Notification, error) {
	s.State.ChannelNotifyPending = false
	return s.ChangeNotification(task.GetChannel(), task.GetRun()), nil
}
