package stream

import (
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
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

// OwnedChannelName is the channel, linked to the owning run, that a stream a
// workflow owns notifies.
func OwnedChannelName(name string) string {
	return ChannelNamePrefix + name
}

// ActivityChannelName is the channel a stream an activity owns notifies: linked
// to the workflow run for an activity a workflow scheduled, independent for a
// standalone activity, which has no linked channels of its own.
func ActivityChannelName(activityID, name string) string {
	return ChannelNamePrefix + activityID + "/" + name
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
// channel, for a stream whose channel is an execution of its own: a
// standalone stream's, or that of a stream a standalone activity owns. One
// task is outstanding at a time and reads the latest change when it runs.
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
