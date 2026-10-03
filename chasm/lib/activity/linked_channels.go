package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
)

// A standalone activity holds linked channels as a workflow run does: any
// channel a client addresses to the activity lives in the activity's own
// state and dies with it. The activity is not among its channels' listeners.
// A workflow run is woken through its next Workflow Task, and an activity has
// no event like it to carry a notification, so a notify reaches the callback
// listeners and the ring only. An activity that wants to know of a channel
// polls it.

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
