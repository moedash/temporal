package service

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/emptypb"
)

// FrontendHandler serves StreamService on the frontend. It resolves the
// namespace name to an ID and forwards to the history shard that owns the
// stream; the layered client does the routing from the business ID.
type FrontendHandler struct {
	streampb.UnimplementedStreamServiceServer

	client            streampb.StreamServiceClient
	namespaceRegistry namespace.Registry
	logger            log.Logger
}

func NewFrontendHandler(
	client streampb.StreamServiceClient,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
) *FrontendHandler {
	return &FrontendHandler{
		client:            client,
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
	}
}

func (h *FrontendHandler) namespaceID(name string) (string, error) {
	if name == "" {
		return "", serviceerror.NewInvalidArgument("namespace is required")
	}
	id, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(name))
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func (h *FrontendHandler) CreateStream(
	ctx context.Context, req *streampb.CreateStreamRequest,
) (*streampb.CreateStreamResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

func (h *FrontendHandler) AddMessages(
	ctx context.Context, req *streampb.AddMessagesRequest,
) (*streampb.AddMessagesResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.AddMessages(ctx, &streampb.AddMessagesRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

func (h *FrontendHandler) FinishWriting(
	ctx context.Context, req *streampb.FinishWritingRequest,
) (*streampb.FinishWritingResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.FinishWriting(ctx, &streampb.FinishWritingRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

func (h *FrontendHandler) SubscribeWorkflow(
	ctx context.Context, req *streampb.SubscribeWorkflowRequest,
) (*streampb.SubscribeWorkflowResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.SubscribeWorkflow(ctx, &streampb.SubscribeWorkflowRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

func (h *FrontendHandler) PollMessages(
	ctx context.Context, req *streampb.PollMessagesRequest,
) (*streampb.PollMessagesResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.PollMessages(ctx, &streampb.PollMessagesRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

func (h *FrontendHandler) PollWorkflowMessages(
	ctx context.Context, req *streampb.PollWorkflowMessagesRequest,
) (*streampb.PollWorkflowMessagesResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.PollWorkflowMessages(ctx, &streampb.PollWorkflowMessagesRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

func (h *FrontendHandler) DescribeWorkflowStream(
	ctx context.Context, req *streampb.DescribeWorkflowStreamRequest,
) (*streampb.DescribeWorkflowStreamResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.DescribeWorkflowStream(ctx, &streampb.DescribeWorkflowStreamRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

func (h *FrontendHandler) DescribeStream(
	ctx context.Context, req *streampb.DescribeStreamRequest,
) (*streampb.DescribeStreamResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.DescribeStream(ctx, &streampb.DescribeStreamRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

func (h *FrontendHandler) CloseStream(
	ctx context.Context, req *streampb.CloseStreamRequest,
) (*streampb.CloseStreamResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.CloseStream(ctx, &streampb.CloseStreamRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

func (h *FrontendHandler) TruncateStream(
	ctx context.Context, req *streampb.TruncateStreamRequest,
) (*streampb.TruncateStreamResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.TruncateStream(ctx, &streampb.TruncateStreamRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

func (h *FrontendHandler) DeleteStream(
	ctx context.Context, req *streampb.DeleteStreamRequest,
) (*streampb.DeleteStreamResponse, error) {
	id, err := h.namespaceID(req.GetFrontendRequest().GetNamespace())
	if err != nil {
		return nil, err
	}
	return h.client.DeleteStream(ctx, &streampb.DeleteStreamRequest{
		NamespaceId: id, FrontendRequest: req.GetFrontendRequest(),
	})
}

// ListStreams answers from visibility rather than from any one stream, so it
// does not route to a shard and is served here rather than on the history side.
func (h *FrontendHandler) ListStreams(
	ctx context.Context, req *streampb.ListStreamsRequest,
) (*streampb.ListStreamsResponse, error) {
	in := req.GetFrontendRequest()
	if in.GetNamespace() == "" {
		return nil, serviceerror.NewInvalidArgument("namespace is required")
	}

	pageSize := int(in.GetPageSize())
	if pageSize <= 0 || pageSize > stream.MaxListPageSize {
		pageSize = stream.MaxListPageSize
	}

	resp, err := chasm.ListExecutions[*stream.Stream, *emptypb.Empty](ctx, &chasm.ListExecutionsRequest{
		NamespaceName: in.GetNamespace(),
		PageSize:      pageSize,
		NextPageToken: in.GetNextPageToken(),
		Query:         in.GetQuery(),
	})
	if err != nil {
		return nil, err
	}

	entries := make([]*streampb.StreamListEntry, 0, len(resp.Executions))
	for _, e := range resp.Executions {
		entries = append(entries, &streampb.StreamListEntry{
			StreamId: e.BusinessID,
			RunId:    e.RunID,
		})
	}
	return &streampb.ListStreamsResponse{
		FrontendResponse: &streampb.ListStreamsOutput{
			Streams:       entries,
			NextPageToken: resp.NextPageToken,
		},
	}, nil
}
