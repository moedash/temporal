package workflow

import (
	"net/url"
	"strings"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	"go.temporal.io/server/chasm/lib/stream"
)

// notifyStreamChannel tells the channel linked to this run under the stream's
// name about the stream's latest change, creating the channel on the first.
// The run itself is not woken: it learns of a stream it owns through its
// cursor, and a Workflow Task per append would be a cost the stream did not
// have before the channel. The callback listeners are handed the change and
// the ring keeps it for pollers.
func (w *Workflow) notifyStreamChannel(
	mctx chasm.MutableContext,
	key string,
	s *stream.Stream,
	limits stream.Limits,
) error {
	if limits.ChannelNotifyOff {
		return nil
	}
	name := streamChannelName(key)
	// One channel per stream, over and above what the run links by hand, so
	// a stream that fits is never refused its channel for room.
	c, err := w.LinkedChannelOrNew(mctx, name,
		limits.MaxOwnedStreamsPerWorkflow+channel.DefaultMaxLinkedChannelsPerWorkflow)
	if err != nil {
		return err
	}
	_, err = c.NotifyLinkedListeners(mctx, s.ChangeNotification(name, mctx.ExecutionKey().RunID),
		channel.Limits{LinkedRetainedNotifications: limits.ChannelRetainedNotifications})
	return err
}

// streamChannelName is the channel a stream held under the key notifies: the
// stream's own name for one the workflow owns, the activity and the name for
// one of its activities owns.
func streamChannelName(key string) string {
	if !IsActivityStreamKey(key) {
		return stream.OwnedChannelName(key)
	}
	escaped, name, _ := strings.Cut(strings.TrimPrefix(key, activityStreamPrefix), "/")
	activityID, err := url.PathUnescape(escaped)
	if err != nil {
		activityID = escaped
	}
	return stream.ActivityChannelName(activityID, name)
}
