package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"go.temporal.io/server/chasm/lib/stream"
)

// A standalone activity holds linked channels as a workflow run does: the
// channel each of its streams announces changes on, and any channel a client
// addresses to the activity, live in the activity's own state and die with
// it. The activity is not among its channels' listeners. A workflow run is
// woken through its next Workflow Task, and an activity has no event like it
// to carry a notification, so a notify reaches the callback listeners and the
// ring only. An activity that wants to know of a channel polls it.

var _ channel.LinkedOwner = (*Activity)(nil)

func (a *Activity) linkedChannels() channel.LinkedChannels {
	return channel.LinkedChannels{Channels: &a.LinkedChannels, Kind: "activity"}
}

// LinkedChannel returns the channel linked to this activity under the name.
func (a *Activity) LinkedChannel(ctx chasm.Context, name string) (*channel.Channel, bool) {
	return a.linkedChannels().Get(ctx, name)
}

// LinkedChannelOrNew returns the linked channel, creating it when the
// activity has none by that name. The limit bounds how many one activity
// holds; zero means the default.
func (a *Activity) LinkedChannelOrNew(
	mctx chasm.MutableContext,
	name string,
	limit int,
) (*channel.Channel, error) {
	return a.linkedChannels().GetOrNew(mctx, name, limit)
}

// NotifyLinkedChannel accepts a notification on the channel linked to this
// activity under the notification's channel name, creating the channel on
// first use.
func (a *Activity) NotifyLinkedChannel(
	mctx chasm.MutableContext,
	n *channelpb.Notification,
	limits channel.Limits,
) (channel.LinkedNotifyResult, error) {
	return a.linkedChannels().Notify(mctx, n, limits)
}

// notifyStreamChannel tells the channel linked to this activity under the
// stream's name about the stream's latest change, creating the channel on
// the first. The callback listeners are handed the change and the ring keeps
// it for pollers.
func (a *Activity) notifyStreamChannel(
	mctx chasm.MutableContext,
	key string,
	s *stream.Stream,
	limits stream.Limits,
) error {
	if limits.ChannelNotifyOff {
		return nil
	}
	name := stream.OwnedChannelName(key)
	// One channel per stream, over and above what a client links by hand, so
	// a stream that fits is never refused its channel for room.
	c, err := a.LinkedChannelOrNew(mctx, name,
		limits.MaxOwnedStreamsPerWorkflow+channel.DefaultMaxLinkedChannelsPerWorkflow)
	if err != nil {
		return err
	}
	_, err = c.NotifyLinkedListeners(mctx, s.ChangeNotification(name, mctx.ExecutionKey().RunID),
		channel.Limits{LinkedRetainedNotifications: limits.ChannelRetainedNotifications})
	return err
}

// closeOwnedStreams ends every stream the activity owns and announces the
// close on each stream's channel. Called when the activity reaches a
// terminal status, which is when a reader tailing it has to be released. A
// retry is not terminal, so the next attempt keeps writing to the same
// streams.
func (a *Activity) closeOwnedStreams(mctx chasm.MutableContext, limits stream.Limits) error {
	for key, field := range a.Streams {
		s := field.Get(mctx)
		if s.State.GetClosed() {
			continue
		}
		if err := s.CloseAndSchedule(mctx, nil); err != nil {
			return err
		}
		if err := a.notifyStreamChannel(mctx, key, s, limits); err != nil {
			return err
		}
	}
	return nil
}

// streamLimits resolves the namespace's stream limits, or the defaults where
// the library carries no stream config, as when the component is driven on
// its own.
func (a *Activity) streamLimits(ctx chasm.Context) stream.Limits {
	actCtx, ok := ctx.Value(ctxKeyActivityContext).(*activityContext)
	if !ok || actCtx.streamConfig == nil || ctx.NamespaceEntry() == nil {
		return stream.DefaultLimits()
	}
	return actCtx.streamConfig.LimitsFor(ctx.NamespaceEntry().Name().String())
}
