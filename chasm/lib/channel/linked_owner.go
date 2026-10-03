package channel

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
)

// LinkedOwner is an execution that holds linked channels in its own state: a
// workflow run. The channel service reads and
// writes the owner through this, whatever its archetype.
type LinkedOwner interface {
	chasm.Component
	LinkedChannel(ctx chasm.Context, name string) (*Channel, bool)
	LinkedChannelOrNew(mctx chasm.MutableContext, name string, limit int) (*Channel, error)
	NotifyLinkedChannel(
		mctx chasm.MutableContext,
		n *channelpb.Notification,
		limits Limits,
	) (LinkedNotifyResult, error)
}

// ExecutionOf names the execution a context belongs to as the public
// Execution: its archetype's type, its business id and its run.
func ExecutionOf(ctx chasm.Context) *commonpb.Execution {
	key := ctx.ExecutionKey()
	return &commonpb.Execution{
		Type:       ctx.ExecutionInfo().ExecutionType,
		BusinessId: key.BusinessID,
		RunId:      key.RunID,
	}
}

// OwnerListens reports whether the execution holding a linked channel is one
// of its listeners. A workflow run is: a notification rides its next
// scheduled Workflow Task.
func OwnerListens(ctx chasm.Context) bool {
	return ctx.ExecutionInfo().ExecutionType != enumspb.EXECUTION_TYPE_ACTIVITY
}

// LinkedChannels is the map of channels one execution holds. Every kind of
// owner keeps one, so the create path and the bound are written once.
type LinkedChannels struct {
	Channels *chasm.Map[string, *Channel]
	// Kind names the owner in refusals, so a caller learns which execution
	// ran out of room.
	Kind string
}

// Get returns the channel held under the name.
func (l LinkedChannels) Get(ctx chasm.Context, name string) (*Channel, bool) {
	field, ok := (*l.Channels)[name]
	if !ok {
		return nil, false
	}
	return field.Get(ctx), true
}

// Count is how many channels the owner holds.
func (l LinkedChannels) Count() int {
	return len(*l.Channels)
}

// GetOrNew returns the channel held under the name, creating it when the
// owner has none by that name. The limit bounds how many one owner holds,
// since each is state the owner carries; zero means the default.
func (l LinkedChannels) GetOrNew(
	mctx chasm.MutableContext,
	name string,
	limit int,
) (*Channel, error) {
	if c, ok := l.Get(mctx, name); ok {
		return c, nil
	}
	if limit <= 0 {
		limit = DefaultMaxLinkedChannelsPerWorkflow
	}
	if l.Count() >= limit {
		return nil, serviceerror.NewResourceExhaustedf(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			"%s already holds %d linked notification channels, which is the limit",
			l.Kind, l.Count())
	}
	if *l.Channels == nil {
		*l.Channels = make(chasm.Map[string, *Channel])
	}
	c := NewLinkedChannel(mctx)
	// Detached from the owner's lifecycle so a post handed over in the
	// transaction that closes the owner still goes out. The service still
	// refuses calls on a closed owner.
	(*l.Channels)[name] = chasm.NewComponentField(mctx, c, chasm.ComponentFieldDetached())
	return c, nil
}

// Notify accepts a notification on the channel held under the notification's
// channel name, creating the channel on first use.
func (l LinkedChannels) Notify(
	mctx chasm.MutableContext,
	n *channelpb.Notification,
	limits Limits,
) (LinkedNotifyResult, error) {
	c, err := l.GetOrNew(mctx, n.GetChannel(), limits.MaxLinkedChannelsPerWorkflow)
	if err != nil {
		return LinkedNotifyResult{}, err
	}
	return c.NotifyLinked(mctx, n, limits)
}
