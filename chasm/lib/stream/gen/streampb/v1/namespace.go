package streampb

// Every RPC here carries its namespace inside `frontend_request` rather than at
// the top level, because the top-level field is the resolved namespace id the
// frontend fills in before routing.
//
// The server's interceptors find a request's namespace by asserting it to
// `interceptor.NamespaceNameGetter`, which wants `GetNamespace() string` on the
// request itself. Without these the assertion falls through to the id getter,
// which at the frontend is still empty, so namespace rate limits, request
// validation, the authorization target, redirection and the long-poll deadline
// all resolve to the empty namespace and silently do nothing.
//
// The generator has no way to express "read it from this nested field", so the
// methods are written here, next to the generated types they belong to.

func (x *CreateStreamRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *AddMessagesRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *FinishWritingRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *SubscribeWorkflowRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *PollMessagesRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *DescribeStreamRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *PollWorkflowMessagesRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *DescribeWorkflowStreamRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *AddWorkflowMessagesRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *RegisterStreamConsumerRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *AdvanceConsumerHeadRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *CloseStreamRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *TruncateStreamRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *ListStreamsRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}

func (x *DeleteStreamRequest) GetNamespace() string {
	return x.GetFrontendRequest().GetNamespace()
}
