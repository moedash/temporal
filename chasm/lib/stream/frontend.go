package stream

import (
	"context"

	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
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
