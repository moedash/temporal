package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
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
	return newTestFrontendWith(t, maxIDLength, true)
}

func newTestFrontendWith(
	t *testing.T, maxIDLength int, enabled bool,
) (*FrontendHandler, *recordingClient) {
	t.Helper()
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceID(namespace.Name("ns")).Return(namespace.ID("ns-id"), nil).AnyTimes()
	registry.EXPECT().GetNamespace(namespace.Name("ns")).Return(
		namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "ns"},
			&persistencespb.NamespaceConfig{Retention: durationpb.New(24 * time.Hour)},
			"cluster",
		), nil).AnyTimes()
	client := &recordingClient{}
	config := &stream.Config{
		Enabled:     dynamicconfig.GetBoolPropertyFnFilteredByNamespace(enabled),
		MaxIDLength: dynamicconfig.GetIntPropertyFn(maxIDLength),
	}
	return NewFrontendHandler(client, registry, log.NewNoopLogger(), config), client
}

// Off by default, so merging this does not turn the surface on everywhere. The
// gate sits on the namespace resolution every RPC goes through.
func TestFrontendRefusesEveryCallWhenStreamsAreOff(t *testing.T) {
	h, client := newTestFrontendWith(t, 1000, false)
	ctx := context.Background()
	var unimplemented *serviceerror.Unimplemented

	_, err := h.CreateStream(ctx, &streampb.CreateStreamRequest{
		FrontendRequest: &streampb.CreateStreamInput{Namespace: "ns", StreamId: "s"},
	})
	require.ErrorAs(t, err, &unimplemented)

	_, err = h.PollMessages(ctx, &streampb.PollMessagesRequest{
		FrontendRequest: &streampb.PollMessagesInput{Namespace: "ns", StreamId: "s"},
	})
	require.ErrorAs(t, err, &unimplemented)

	_, err = h.ListStreams(ctx, &streampb.ListStreamsRequest{
		FrontendRequest: &streampb.ListStreamsInput{Namespace: "ns"},
	})
	require.ErrorAs(t, err, &unimplemented)
	require.Empty(t, client.polls, "a refused request is never routed")
}

// A closed stream with no retention schedules no deletion, so it and its
// batches stay in the database for good. Left unset it takes the namespace's.
func TestFrontendSettlesTheStreamLifecycle(t *testing.T) {
	h, _ := newTestFrontend(t, 1000)

	settled, err := h.checkLifecycle("ns", nil)
	require.NoError(t, err)
	require.Equal(t, 24*time.Hour, settled.GetRetention().AsDuration())

	_, err = h.checkLifecycle("ns", &streampb.StreamLifecycle{Retention: durationpb.New(0)})
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid, "an explicit zero is a mistake, not a request for forever")

	_, err = h.checkLifecycle("ns", &streampb.StreamLifecycle{
		Retention: durationpb.New(48 * time.Hour),
	})
	require.ErrorAs(t, err, &invalid, "a stream cannot outlive its namespace's retention")

	_, err = h.checkLifecycle("ns", &streampb.StreamLifecycle{MaxItems: -1})
	require.ErrorAs(t, err, &invalid)
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

// The archetype is a predicate the caller's query can replace rather than one
// it is anded with, so a query naming the division would reach executions this
// RPC has no business listing.
func TestFrontendRefusesAListQueryThatNamesTheArchetype(t *testing.T) {
	h, _ := newTestFrontend(t, 1000)
	var invalid *serviceerror.InvalidArgument

	_, err := h.ListStreams(context.Background(), &streampb.ListStreamsRequest{
		FrontendRequest: &streampb.ListStreamsInput{
			Namespace: "ns",
			Query:     `TemporalNamespaceDivision = "TemporalScheduler"`,
		},
	})
	require.ErrorAs(t, err, &invalid)

	_, err = h.ListStreams(context.Background(), &streampb.ListStreamsRequest{
		FrontendRequest: &streampb.ListStreamsInput{
			Namespace: "ns",
			Query:     `temporalnamespacedivision = "x"`,
		},
	})
	require.ErrorAs(t, err, &invalid, "the check does not depend on how the caller cased it")
}
