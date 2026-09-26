package service

import (
	"context"
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// FrontendHandler serves StreamService on the frontend. It resolves the
// namespace name to an ID, checks what can be checked without the stream, and
// forwards to the history shard that owns the stream; the layered client does
// the routing from the business ID.
//
// The checks are the ones the workflow handler makes for its own ids. A stream
// id becomes an execution's business id and a stream name a key in mutable
// state, so neither may be longer than an id is allowed to be, and an offset
// or page size that history would only clamp or refuse later is refused here
// before the request is routed.
type FrontendHandler struct {
	streampb.UnimplementedStreamServiceServer

	client            streampb.StreamServiceClient
	namespaceRegistry namespace.Registry
	logger            log.Logger
	config            *stream.Config
}

func NewFrontendHandler(
	client streampb.StreamServiceClient,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	config *stream.Config,
) *FrontendHandler {
	return &FrontendHandler{
		client:            client,
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
		config:            config,
	}
}

// RedirectableMethods lists the stream RPCs a cell forwards to the namespace's
// active cell when it is not that cell itself, each with the response it
// answers with, for the redirection interceptor.
//
// The stream lives in the active cell's mutable state, so a call served where
// it happens to land would write to a copy nothing reads or read one that
// stops at the last replication. The two calls History makes on itself are not
// listed: they never reach a frontend legitimately, and this handler answers
// them with Unimplemented wherever they land.
func RedirectableMethods() map[string]func() any {
	return map[string]func() any{
		streampb.StreamService_CreateStream_FullMethodName: func() any {
			return &streampb.CreateStreamResponse{}
		},
		streampb.StreamService_AddMessages_FullMethodName: func() any {
			return &streampb.AddMessagesResponse{}
		},
		streampb.StreamService_FinishWriting_FullMethodName: func() any {
			return &streampb.FinishWritingResponse{}
		},
		streampb.StreamService_SubscribeWorkflow_FullMethodName: func() any {
			return &streampb.SubscribeWorkflowResponse{}
		},
		streampb.StreamService_PollMessages_FullMethodName: func() any {
			return &streampb.PollMessagesResponse{}
		},
		streampb.StreamService_DescribeStream_FullMethodName: func() any {
			return &streampb.DescribeStreamResponse{}
		},
		streampb.StreamService_PollWorkflowMessages_FullMethodName: func() any {
			return &streampb.PollWorkflowMessagesResponse{}
		},
		streampb.StreamService_DescribeWorkflowStream_FullMethodName: func() any {
			return &streampb.DescribeWorkflowStreamResponse{}
		},
		streampb.StreamService_AddWorkflowMessages_FullMethodName: func() any {
			return &streampb.AddWorkflowMessagesResponse{}
		},
		streampb.StreamService_CloseStream_FullMethodName: func() any {
			return &streampb.CloseStreamResponse{}
		},
		streampb.StreamService_TruncateStream_FullMethodName: func() any {
			return &streampb.TruncateStreamResponse{}
		},
		streampb.StreamService_ListStreams_FullMethodName: func() any {
			return &streampb.ListStreamsResponse{}
		},
		streampb.StreamService_DeleteStream_FullMethodName: func() any {
			return &streampb.DeleteStreamResponse{}
		},
	}
}

// namespaceID resolves the namespace and refuses the call when streams are off
// for it.
//
// Every RPC goes through here, so the gate covers the whole surface. It is off
// by default: this registers a large new service on the public frontend, and
// an operator needs a switch for it that is not a rollback.
func (h *FrontendHandler) namespaceID(name string) (string, error) {
	if name == "" {
		return "", serviceerror.NewInvalidArgument("namespace is required")
	}
	if !h.config.EnabledFor(name) {
		return "", serviceerror.NewUnimplementedf(
			"streams are not enabled for namespace: %s", name)
	}
	id, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(name))
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// checkLifecycle settles the lifecycle a caller asked for.
//
// Retention left unset means the namespace's own workflow retention rather
// than forever: a closed stream with no retention schedules no deletion task,
// so its execution and its batches stay in the database for good. Everything
// else in the system gets a retention bound from its namespace, and a stream
// should not be the exception because the caller left a field empty.
func (h *FrontendHandler) checkLifecycle(
	namespaceName string,
	lifecycle *streampb.StreamLifecycle,
) (*streampb.StreamLifecycle, error) {
	ns, err := h.namespaceRegistry.GetNamespace(namespace.Name(namespaceName))
	if err != nil {
		return nil, err
	}
	if lifecycle == nil {
		lifecycle = &streampb.StreamLifecycle{}
	}
	lifecycle = common.CloneProto(lifecycle)

	if lifecycle.GetMaxItems() < 0 {
		return nil, serviceerror.NewInvalidArgumentf(
			"max items cannot be negative, got %d", lifecycle.GetMaxItems())
	}

	retention := lifecycle.GetRetention().AsDuration()
	switch {
	case lifecycle.GetRetention() == nil:
		retention = ns.Retention()
	case retention <= 0:
		return nil, serviceerror.NewInvalidArgumentf(
			"retention must be positive, got %v", retention)
	case retention > ns.Retention():
		return nil, serviceerror.NewInvalidArgumentf(
			"retention of %v is over the namespace's retention of %v", retention, ns.Retention())
	}
	lifecycle.Retention = durationpb.New(retention)
	return lifecycle, nil
}

// checkID refuses an id or name longer than the namespace's id limit. Empty is
// allowed here: some fields mean the default when empty and the ones that do
// not are checked by their handler.
func (h *FrontendHandler) checkID(field, value string) error {
	if len(value) > h.config.MaxIDLength() {
		return serviceerror.NewInvalidArgumentf(
			"%s is %d characters, over the %d limit", field, len(value), h.config.MaxIDLength())
	}
	return nil
}

func checkOffset(field string, value int64) error {
	if value < 0 {
		return serviceerror.NewInvalidArgumentf("%s cannot be negative, got %d", field, value)
	}
	return nil
}

// clampMaxMessages bounds a page the way the history side does, so a caller
// asking for more sees the same page size it would have been given anyway.
func clampMaxMessages(requested int32) int32 {
	if requested <= 0 || requested > stream.DefaultMaxMessagesPerPoll {
		return stream.DefaultMaxMessagesPerPoll
	}
	return requested
}

func (h *FrontendHandler) CreateStream(
	ctx context.Context, req *streampb.CreateStreamRequest,
) (*streampb.CreateStreamResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if in.GetStreamId() == "" {
		return nil, serviceerror.NewInvalidArgument("stream id is required")
	}
	if err := h.checkID("stream id", in.GetStreamId()); err != nil {
		return nil, err
	}
	lifecycle, err := h.checkLifecycle(in.GetNamespace(), in.GetLifecycle())
	if err != nil {
		return nil, err
	}
	in.Lifecycle = lifecycle
	return h.client.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) AddMessages(
	ctx context.Context, req *streampb.AddMessagesRequest,
) (*streampb.AddMessagesResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream id", in.GetStreamId()); err != nil {
		return nil, err
	}
	if err := h.checkID("producer id", in.GetProducerId()); err != nil {
		return nil, err
	}
	return h.client.AddMessages(ctx, &streampb.AddMessagesRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) FinishWriting(
	ctx context.Context, req *streampb.FinishWritingRequest,
) (*streampb.FinishWritingResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream id", in.GetStreamId()); err != nil {
		return nil, err
	}
	if err := h.checkID("producer id", in.GetProducerId()); err != nil {
		return nil, err
	}
	return h.client.FinishWriting(ctx, &streampb.FinishWritingRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) SubscribeWorkflow(
	ctx context.Context, req *streampb.SubscribeWorkflowRequest,
) (*streampb.SubscribeWorkflowResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream id", in.GetStreamId()); err != nil {
		return nil, err
	}
	if err := h.checkID("stream name", in.GetStreamName()); err != nil {
		return nil, err
	}
	if err := h.checkID("workflow id", in.GetWorkflowId()); err != nil {
		return nil, err
	}
	return h.client.SubscribeWorkflow(ctx, &streampb.SubscribeWorkflowRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) PollMessages(
	ctx context.Context, req *streampb.PollMessagesRequest,
) (*streampb.PollMessagesResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream id", in.GetStreamId()); err != nil {
		return nil, err
	}
	if err := checkOffset("from offset", in.GetFromOffset()); err != nil {
		return nil, err
	}
	in.MaxMessages = clampMaxMessages(in.GetMaxMessages())
	return h.client.PollMessages(ctx, &streampb.PollMessagesRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) PollWorkflowMessages(
	ctx context.Context, req *streampb.PollWorkflowMessagesRequest,
) (*streampb.PollWorkflowMessagesResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream name", in.GetStreamName()); err != nil {
		return nil, err
	}
	if err := checkOffset("from offset", in.GetFromOffset()); err != nil {
		return nil, err
	}
	if err := h.checkID("workflow id", in.GetWorkflowId()); err != nil {
		return nil, err
	}
	in.MaxMessages = clampMaxMessages(in.GetMaxMessages())
	return h.client.PollWorkflowMessages(ctx, &streampb.PollWorkflowMessagesRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) DescribeWorkflowStream(
	ctx context.Context, req *streampb.DescribeWorkflowStreamRequest,
) (*streampb.DescribeWorkflowStreamResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream name", in.GetStreamName()); err != nil {
		return nil, err
	}
	if err := h.checkID("workflow id", in.GetWorkflowId()); err != nil {
		return nil, err
	}
	return h.client.DescribeWorkflowStream(ctx, &streampb.DescribeWorkflowStreamRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) AddWorkflowMessages(
	ctx context.Context, req *streampb.AddWorkflowMessagesRequest,
) (*streampb.AddWorkflowMessagesResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream name", in.GetStreamName()); err != nil {
		return nil, err
	}
	if err := h.checkID("workflow id", in.GetWorkflowId()); err != nil {
		return nil, err
	}
	if err := h.checkID("producer id", in.GetProducerId()); err != nil {
		return nil, err
	}
	return h.client.AddWorkflowMessages(ctx, &streampb.AddWorkflowMessagesRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) DescribeStream(
	ctx context.Context, req *streampb.DescribeStreamRequest,
) (*streampb.DescribeStreamResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream id", in.GetStreamId()); err != nil {
		return nil, err
	}
	return h.client.DescribeStream(ctx, &streampb.DescribeStreamRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) CloseStream(
	ctx context.Context, req *streampb.CloseStreamRequest,
) (*streampb.CloseStreamResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream id", in.GetStreamId()); err != nil {
		return nil, err
	}
	return h.client.CloseStream(ctx, &streampb.CloseStreamRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) TruncateStream(
	ctx context.Context, req *streampb.TruncateStreamRequest,
) (*streampb.TruncateStreamResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream id", in.GetStreamId()); err != nil {
		return nil, err
	}
	if err := checkOffset("new base offset", in.GetNewBaseOffset()); err != nil {
		return nil, err
	}
	return h.client.TruncateStream(ctx, &streampb.TruncateStreamRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

func (h *FrontendHandler) DeleteStream(
	ctx context.Context, req *streampb.DeleteStreamRequest,
) (*streampb.DeleteStreamResponse, error) {
	in := req.GetFrontendRequest()
	id, err := h.namespaceID(in.GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := h.checkID("stream id", in.GetStreamId()); err != nil {
		return nil, err
	}
	return h.client.DeleteStream(ctx, &streampb.DeleteStreamRequest{
		NamespaceId: id, FrontendRequest: in,
	})
}

// ListStreams answers from visibility rather than from any one stream, so it
// does not route to a shard and is served here rather than on the history side.
func (h *FrontendHandler) ListStreams(
	ctx context.Context, req *streampb.ListStreamsRequest,
) (*streampb.ListStreamsResponse, error) {
	in := req.GetFrontendRequest()
	if _, err := h.namespaceID(in.GetNamespace()); err != nil {
		return nil, err
	}
	// The archetype is a predicate the caller's query can replace rather than
	// one it is anded with, so a query naming the division would list any
	// archetype in the namespace, workflows included, through a stream-scoped
	// read-only RPC.
	if strings.Contains(
		strings.ToLower(in.GetQuery()), strings.ToLower(sadefs.TemporalNamespaceDivision)) {
		return nil, serviceerror.NewInvalidArgumentf(
			"a stream query cannot filter on %s", sadefs.TemporalNamespaceDivision)
	}

	pageSize := int(in.GetPageSize())
	if pageSize <= 0 || pageSize > stream.MaxListPageSize {
		pageSize = stream.MaxListPageSize
	}

	resp, err := chasm.ListExecutions[*stream.Stream, *emptypb.Empty](
		ctx,
		&chasm.ListExecutionsRequest{
			NamespaceName: in.GetNamespace(),
			PageSize:      pageSize,
			NextPageToken: in.GetNextPageToken(),
			Query:         in.GetQuery(),
		},
	)
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
