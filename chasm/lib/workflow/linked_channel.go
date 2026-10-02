package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/channel"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
)

// LinkedChannel returns the channel linked to this run under the name.
func (w *Workflow) LinkedChannel(ctx chasm.Context, name string) (*channel.Channel, bool) {
	field, ok := w.LinkedChannels[name]
	if !ok {
		return nil, false
	}
	return field.Get(ctx), true
}

// LinkedChannelCount is how many channels are linked to this run.
func (w *Workflow) LinkedChannelCount() int {
	return len(w.LinkedChannels)
}

// LinkedChannelOrNew returns the linked channel, creating it when the run
// has none by that name. The limit bounds how many one run holds, since each
// is state the run carries; zero means the default.
func (w *Workflow) LinkedChannelOrNew(
	mctx chasm.MutableContext,
	name string,
	limit int,
) (*channel.Channel, error) {
	if field, ok := w.LinkedChannels[name]; ok {
		return field.Get(mctx), nil
	}
	if limit <= 0 {
		limit = channel.DefaultMaxLinkedChannelsPerWorkflow
	}
	if len(w.LinkedChannels) >= limit {
		return nil, serviceerror.NewResourceExhaustedf(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			"workflow already holds %d linked notification channels, which is the limit",
			len(w.LinkedChannels))
	}
	if w.LinkedChannels == nil {
		w.LinkedChannels = make(chasm.Map[string, *channel.Channel])
	}
	c := channel.NewLinkedChannel(mctx)
	w.LinkedChannels[name] = chasm.NewComponentField(mctx, c)
	return c, nil
}

// NotifyLinkedChannel accepts a notification on the channel linked to this run
// under the notification's channel name, creating the channel on first use.
func (w *Workflow) NotifyLinkedChannel(
	mctx chasm.MutableContext,
	n *channelpb.Notification,
	limits channel.Limits,
) (channel.LinkedNotifyResult, error) {
	c, err := w.LinkedChannelOrNew(mctx, n.GetChannel(), limits.MaxLinkedChannelsPerWorkflow)
	if err != nil {
		return channel.LinkedNotifyResult{}, err
	}
	return c.NotifyLinked(mctx, n, limits)
}

// LinkedChannelHolds reports whether the channel linked under the name already
// holds a notification at this counter or above for the owner. A channel the
// run does not have holds nothing.
func (w *Workflow) LinkedChannelHolds(ctx chasm.Context, name string, counter int64) bool {
	c, ok := w.LinkedChannel(readOnly(ctx), name)
	return ok && c.OwnerHolds(counter)
}
