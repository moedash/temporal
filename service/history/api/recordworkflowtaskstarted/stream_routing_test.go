package recordworkflowtaskstarted

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/definition"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type routedStreamClient struct {
	streamlib.StreamServiceClient
	poll func(context.Context, *streamlib.PollMessagesRequest) (*streamlib.PollMessagesResponse, error)
}

func (c *routedStreamClient) PollMessages(
	ctx context.Context, request *streamlib.PollMessagesRequest, _ ...grpc.CallOption,
) (*streamlib.PollMessagesResponse, error) {
	return c.poll(ctx, request)
}

func TestExternalStreamLiveAndReplayUseRoutedPayloadRead(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "replay"}[replay], func(t *testing.T) {
			calls := 0
			client := &routedStreamClient{poll: func(ctx context.Context, req *streamlib.PollMessagesRequest) (*streamlib.PollMessagesResponse, error) {
				calls++
				require.Equal(t, "namespace-id", req.GetNamespaceId())
				require.Equal(t, "remote-source", req.GetFrontendRequest().GetStreamId())
				require.EqualValues(t, 4, req.GetFrontendRequest().GetFromOffset())
				require.EqualValues(t, 2, req.GetFrontendRequest().GetMaxMessages())
				require.False(t, req.GetFrontendRequest().GetWaitNewMessages())
				require.Empty(t, req.GetFrontendRequest().GetTopics())
				return &streamlib.PollMessagesResponse{FrontendResponse: &streamlib.PollMessagesOutput{
					NextOffset: 6, HeadOffset: 9,
					Messages: []*streamlib.StreamMessage{
						{Offset: 4, Kind: streamlib.STREAM_MESSAGE_KIND_DATA, Body: &commonpb.Payload{Data: []byte("a")}},
						{Offset: 5, Kind: streamlib.STREAM_MESSAGE_KIND_DATA, Body: &commonpb.Payload{Data: []byte("b")}},
					},
				}}, nil
			}}
			// No local engine is installed. Reverting to the old local-controller
			// path cannot accidentally satisfy this test's remote source.
			ctx := WithStreamClient(context.Background(), client)
			var window stream.Window
			var err error
			if replay {
				window, err = readRecordedRange(ctx,
					definition.NewWorkflowKey("namespace-id", "consumer", "consumer-run"),
					streamOrigin{external: true}, "remote-source", 4, 6)
			} else {
				window, err = readWindowFor(ctx, nil, nil, "namespace-id", "input", true, "remote-source", 4, 6)
			}
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			messages, next, err := stream.CollectMessages(window.Blobs, window.Starts, 4, window.To, window.Limit, nil)
			require.NoError(t, err)
			require.EqualValues(t, 6, next)
			require.Len(t, messages, 2)
			require.Equal(t, []byte("a"), messages[0].GetBody().GetData())
			require.Equal(t, []byte("b"), messages[1].GetBody().GetData())
		})
	}
}

func TestExternalStreamRoutedReadRejectsCorruptOrExpandedRange(t *testing.T) {
	cases := map[string]*streamlib.PollMessagesOutput{
		"missing-response":        nil,
		"skipped-offset":          {NextOffset: 6, HeadOffset: 9, Messages: []*streamlib.StreamMessage{{Offset: 4}, {Offset: 6}}},
		"short-count":             {NextOffset: 6, HeadOffset: 9, Messages: []*streamlib.StreamMessage{{Offset: 4}}},
		"past-requested-frontier": {NextOffset: 7, HeadOffset: 9, Messages: []*streamlib.StreamMessage{{Offset: 4}, {Offset: 5}, {Offset: 6}}},
		"past-committed-head":     {NextOffset: 6, HeadOffset: 5, Messages: []*streamlib.StreamMessage{{Offset: 4}, {Offset: 5}}},
	}
	for name, response := range cases {
		t.Run(name, func(t *testing.T) {
			client := &routedStreamClient{poll: func(context.Context, *streamlib.PollMessagesRequest) (*streamlib.PollMessagesResponse, error) {
				return &streamlib.PollMessagesResponse{FrontendResponse: response}, nil
			}}
			_, err := readExternalWindow(WithStreamClient(context.Background(), client), "ns", "source", 4, 6)
			var integrity *serviceerror.DataLoss
			require.ErrorAs(t, err, &integrity)
		})
	}
}

func TestExternalStreamRoutedReadPropagatesSourceFailure(t *testing.T) {
	want := errors.New("source shard unavailable")
	client := &routedStreamClient{poll: func(context.Context, *streamlib.PollMessagesRequest) (*streamlib.PollMessagesResponse, error) {
		return nil, want
	}}
	_, err := readExternalWindow(WithStreamClient(context.Background(), client), "ns", "source", 4, 6)
	require.ErrorIs(t, err, want)
}

func TestOwnedLiveStreamNeverReentersThroughRoutedClient(t *testing.T) {
	client := &routedStreamClient{poll: func(context.Context, *streamlib.PollMessagesRequest) (*streamlib.PollMessagesResponse, error) {
		t.Fatal("owned stream read re-entered the consumer through an RPC")
		return nil, nil
	}}
	window, err := readWindowFor(WithStreamClient(context.Background(), client), nil,
		&chasmworkflow.Workflow{}, "ns", "owned", false, "owned", 4, 6)
	require.NoError(t, err)
	require.EqualValues(t, 4, window.To)
}

func TestOwnedReplayPinsConsumerRun(t *testing.T) {
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ref chasm.ComponentRef, _ func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
			require.Equal(t, "consumer-run", ref.RunID)
			require.Equal(t, "consumer", ref.BusinessID)
			return nil
		})
	_, err := readRecordedRange(chasm.NewEngineContext(context.Background(), engine),
		definition.NewWorkflowKey("ns", "consumer", "consumer-run"),
		streamOrigin{name: "owned"}, "owned", 4, 6)
	require.NoError(t, err)
}
