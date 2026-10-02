package channel

import (
	notificationpb "go.temporal.io/api/notification/v1"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"go.temporal.io/server/common"
)

// The stored notification has the public message's fields under the same
// names, so converting is a field copy in either direction.

func FromAPINotification(n *notificationpb.Notification) *channelpb.Notification {
	if n == nil {
		return nil
	}
	n = common.CloneProto(n)
	return &channelpb.Notification{
		Channel:  n.GetChannel(),
		Position: n.GetPosition(),
		Counter:  n.GetCounter(),
		Metadata: n.GetMetadata(),
	}
}

func ToAPINotification(n *channelpb.Notification) *notificationpb.Notification {
	if n == nil {
		return nil
	}
	n = common.CloneProto(n)
	return &notificationpb.Notification{
		Channel:  n.GetChannel(),
		Position: n.GetPosition(),
		Counter:  n.GetCounter(),
		Metadata: n.GetMetadata(),
	}
}

func ToAPINotifications(ns []*channelpb.Notification) []*notificationpb.Notification {
	if len(ns) == 0 {
		return nil
	}
	out := make([]*notificationpb.Notification, len(ns))
	for i, n := range ns {
		out[i] = ToAPINotification(n)
	}
	return out
}

// ToAPIListener converts a listener from describe.
func ToAPIListener(l *channelpb.ChannelListenerInfo) *notificationpb.ChannelListener {
	out := &notificationpb.ChannelListener{
		ListenerId:     l.GetListenerId(),
		RegisteredTime: common.CloneProto(l.GetRegisteredTime()),
	}
	switch variant := l.GetVariant().(type) {
	case *channelpb.ChannelListenerInfo_Workflow_:
		out.Listener = &notificationpb.ChannelListener_Workflow{
			Workflow: &notificationpb.WorkflowListener{
				WorkflowId: variant.Workflow.GetWorkflowId(),
				RunId:      variant.Workflow.GetRunId(),
			},
		}
	case *channelpb.ChannelListenerInfo_Callback:
		out.Listener = &notificationpb.ChannelListener_Callback{
			Callback: common.CloneProto(variant.Callback),
		}
	default:
		// A listener with no variant has nothing more to say.
	}
	return out
}
