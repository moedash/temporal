package service

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

// recordingClient captures what the frontend forwards, so a test can check
// what history would have been asked without a history service.
type recordingClient struct {
	streampb.StreamServiceClient
	polls     []*streampb.PollMessagesRequest
	truncates []*streampb.TruncateStreamRequest
}

func (c *recordingClient) PollMessages(
	_ context.Context, req *streampb.PollMessagesRequest, _ ...grpc.CallOption,
) (*streampb.PollMessagesResponse, error) {
	c.polls = append(c.polls, req)
	return &streampb.PollMessagesResponse{}, nil
}

func (c *recordingClient) TruncateStream(
	_ context.Context, req *streampb.TruncateStreamRequest, _ ...grpc.CallOption,
) (*streampb.TruncateStreamResponse, error) {
	c.truncates = append(c.truncates, req)
	return &streampb.TruncateStreamResponse{}, nil
}

func newTestFrontend(t *testing.T, maxIDLength int) (*FrontendHandler, *recordingClient) {
	t.Helper()
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceID(namespace.Name("ns")).Return(namespace.ID("ns-id"), nil).AnyTimes()
	client := &recordingClient{}
	config := &stream.Config{MaxIDLength: dynamicconfig.GetIntPropertyFn(maxIDLength)}
	return NewFrontendHandler(client, registry, log.NewNoopLogger(), config), client
}

// The checks the workflow handler makes for its own ids, made here for stream
// ids and names before a request is routed.
func TestFrontendRefusesWhatHistoryWouldOnlyDiscoverLater(t *testing.T) {
	h, client := newTestFrontend(t, 16)
	ctx := context.Background()
	var invalid *serviceerror.InvalidArgument

	_, err := h.CreateStream(ctx, &streampb.CreateStreamRequest{
		FrontendRequest: &streampb.CreateStreamInput{Namespace: "ns", StreamId: strings.Repeat("x", 17)},
	})
	require.ErrorAs(t, err, &invalid, "a stream id becomes a business id and is bounded like one")

	_, err = h.PollWorkflowMessages(ctx, &streampb.PollWorkflowMessagesRequest{
		FrontendRequest: &streampb.PollWorkflowMessagesInput{
			Namespace: "ns", WorkflowId: "wf", StreamName: strings.Repeat("n", 17),
		},
	})
	require.ErrorAs(t, err, &invalid, "a stream name becomes a state key and is bounded the same way")

	_, err = h.PollMessages(ctx, &streampb.PollMessagesRequest{
		FrontendRequest: &streampb.PollMessagesInput{Namespace: "ns", StreamId: "s", FromOffset: -1},
	})
	require.ErrorAs(t, err, &invalid)

	_, err = h.TruncateStream(ctx, &streampb.TruncateStreamRequest{
		FrontendRequest: &streampb.TruncateStreamInput{Namespace: "ns", StreamId: "s", NewBaseOffset: -5},
	})
	require.ErrorAs(t, err, &invalid)
	require.Empty(t, client.polls)
	require.Empty(t, client.truncates, "a refused request is never routed")
}

// A page larger than the server serves is clamped before routing, so the
// caller sees the same page it would have been given anyway.
func TestFrontendClampsThePageSize(t *testing.T) {
	h, client := newTestFrontend(t, 1000)
	_, err := h.PollMessages(context.Background(), &streampb.PollMessagesRequest{
		FrontendRequest: &streampb.PollMessagesInput{
			Namespace: "ns", StreamId: "s", MaxMessages: stream.DefaultMaxMessagesPerPoll * 4,
		},
	})
	require.NoError(t, err)
	require.Len(t, client.polls, 1)
	require.Equal(t, "ns-id", client.polls[0].GetNamespaceId())
	require.EqualValues(t, stream.DefaultMaxMessagesPerPoll, client.polls[0].GetFrontendRequest().GetMaxMessages())
}
