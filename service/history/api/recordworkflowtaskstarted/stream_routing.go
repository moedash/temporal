package recordworkflowtaskstarted

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
)

type streamClientContextKey struct{}

// WithStreamClient supplies shard routing for calls that reach a stream in
// another execution from inside a workflow task's transaction.
func WithStreamClient(ctx context.Context, client streamlib.StreamServiceClient) context.Context {
	if client == nil {
		return ctx
	}
	return context.WithValue(ctx, streamClientContextKey{}, client)
}

// StreamClientFromContext returns the routed client installed by the engine.
func StreamClientFromContext(ctx context.Context) (streamlib.StreamServiceClient, bool) {
	client, ok := ctx.Value(streamClientContextKey{}).(streamlib.StreamServiceClient)
	return client, ok
}

// WithRoutedDeadline bounds a routed call made while the workflow lock is held.
func WithRoutedDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, stream.RoutedCallTimeout)
}

func readExternalWindow(
	ctx context.Context,
	namespaceID, streamID string,
	from, to int64,
) (stream.Window, error) {
	if to <= from {
		return stream.Window{To: from}, nil
	}
	limit := int32(min(to-from, int64(stream.DefaultMaxMessagesPerPoll)))
	client, ok := StreamClientFromContext(ctx)
	if !ok {
		// Hand-built engines in component tests have no service client.
		return chasm.ReadComponent(ctx,
			chasm.NewComponentRef[*stream.Stream](chasm.ExecutionKey{
				NamespaceID: namespaceID,
				BusinessID:  streamID,
			}),
			(*stream.Stream).ReadWindow,
			stream.WindowRequest{From: from, MaxMessages: limit})
	}

	callCtx, cancel := WithRoutedDeadline(ctx)
	defer cancel()
	response, err := client.PollMessages(callCtx, &streamlib.PollMessagesRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &streamlib.PollMessagesInput{
			StreamId: streamID, FromOffset: from, MaxMessages: limit,
		},
	})
	if err != nil {
		return stream.Window{}, err
	}
	out := response.GetFrontendResponse()
	if out == nil || out.GetNextOffset() < from || out.GetNextOffset() > to ||
		out.GetNextOffset() > out.GetHeadOffset() ||
		int64(len(out.GetMessages())) != out.GetNextOffset()-from {
		return stream.Window{}, serviceerror.NewDataLoss(
			"stream read returned an invalid contiguous range")
	}
	for index, message := range out.GetMessages() {
		if message == nil || message.GetOffset() != from+int64(index) {
			return stream.Window{}, serviceerror.NewDataLoss(
				"stream read returned a missing or reordered offset")
		}
	}
	w := stream.Window{
		State: &streamlib.StreamState{
			HeadOffset:  out.GetHeadOffset(),
			Closed:      out.GetClosed(),
			CloseReason: out.GetCloseReason(),
		},
		To:    out.GetNextOffset(),
		Limit: int(limit),
		RunID: out.GetRunId(),
	}
	if len(out.GetMessages()) == 0 {
		return w, nil
	}
	data, err := proto.Marshal(&streamlib.StreamMessageBatch{Messages: out.GetMessages()})
	if err != nil {
		return stream.Window{}, err
	}
	w.Blobs = []*commonpb.DataBlob{{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: data}}
	w.Starts = []int64{from}
	return w, nil
}
