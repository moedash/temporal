package channel

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"go.temporal.io/server/common"
)

// The stored notification has the public message's fields under the same
// names, so converting is a field copy in either direction. The owner of a
// linked channel is the one exception: the public message names it as an
// Execution, the stored one as the workflow run it is.

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
		LinkedTo: FromAPIExecution(n.GetLinkedTo()),
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
		LinkedTo: ToAPIExecution(n.GetLinkedTo()),
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

// ToAPIExecution names a workflow run as the public Execution.
func ToAPIExecution(e *commonpb.WorkflowExecution) *commonpb.Execution {
	if e == nil {
		return nil
	}
	return &commonpb.Execution{
		Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
		BusinessId: e.GetWorkflowId(),
		RunId:      e.GetRunId(),
	}
}

// FromAPIExecution reads an Execution as the workflow run it names. The type
// is not checked here: only workflows hold linked channels.
func FromAPIExecution(e *commonpb.Execution) *commonpb.WorkflowExecution {
	if e == nil {
		return nil
	}
	return &commonpb.WorkflowExecution{WorkflowId: e.GetBusinessId(), RunId: e.GetRunId()}
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
