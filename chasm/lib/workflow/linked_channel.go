package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
)

var _ channel.LinkedOwner = (*Workflow)(nil)

func (w *Workflow) linkedChannels() channel.LinkedChannels {
	return channel.LinkedChannels{Channels: &w.LinkedChannels, Kind: "workflow"}
}

// LinkedChannel returns the channel linked to this run under the name.
func (w *Workflow) LinkedChannel(ctx chasm.Context, name string) (*channel.Channel, bool) {
	return w.linkedChannels().Get(ctx, name)
}

// LinkedChannelCount is how many channels are linked to this run.
func (w *Workflow) LinkedChannelCount() int {
	return w.linkedChannels().Count()
}

// LinkedChannelOrNew returns the linked channel, creating it when the run
// has none by that name. The limit bounds how many one run holds, since each
// is state the run carries; zero means the default.
func (w *Workflow) LinkedChannelOrNew(
	mctx chasm.MutableContext,
	name string,
	limit int,
) (*channel.Channel, error) {
	return w.linkedChannels().GetOrNew(mctx, name, limit)
}

// NotifyLinkedChannel accepts a notification on the channel linked to this run
// under the notification's channel name, creating the channel on first use.
func (w *Workflow) NotifyLinkedChannel(
	mctx chasm.MutableContext,
	n *channelpb.Notification,
	limits channel.Limits,
) (channel.LinkedNotifyResult, error) {
	return w.linkedChannels().Notify(mctx, n, limits)
}

// LinkedChannelHolds reports whether the channel linked under the name already
// holds a notification at this counter or above for the owner. A channel the
// run does not have holds nothing.
func (w *Workflow) LinkedChannelHolds(ctx chasm.Context, name string, counter int64) bool {
	c, ok := w.LinkedChannel(readOnly(ctx), name)
	return ok && c.OwnerHolds(counter)
}
