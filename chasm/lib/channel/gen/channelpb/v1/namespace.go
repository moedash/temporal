package channelpb

// Every RPC here carries its namespace inside `frontend_request`, because the
// top-level field is the namespace id the frontend resolves before routing.
// The interceptors find a request's namespace through `GetNamespace() string`
// on the request itself, and the generator cannot point that at a nested
// field, so the getters are written here.

func (x *NotifyChannelRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *RegisterChannelListenerRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *UnregisterChannelListenerRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *PollChannelRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *DescribeChannelRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *RegisterWorkflowListenerRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *UnregisterWorkflowListenerRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *DeliverChannelNotificationRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}
