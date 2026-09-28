package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
)

// A stream holding offsets 3 to 7: five records appended after three were
// truncated away.
func newTruncatedStream(t *testing.T) *Stream {
	t.Helper()
	s := newTestStream(t)
	s.Batches = make(chasm.Map[int64, *commonpb.DataBlob])
	_, err := s.AddMessages(nil, AddMessagesRequest{Records: msgs("a", "b", "c")})
	require.NoError(t, err)
	_, err = s.AddMessages(nil, AddMessagesRequest{Records: msgs("d", "e", "f", "g", "h")})
	require.NoError(t, err)
	require.NoError(t, s.Truncate(nil, 3))
	return s
}

func TestStartPositionsResolveAgainstTheFrontier(t *testing.T) {
	s := newTruncatedStream(t)
	for _, tc := range []struct {
		name string
		pos  *streampb.StreamStartPosition
		want int64
	}{
		{"offset", AtOffset(5), 5},
		{"earliest is the floor, not zero", Earliest(), 3},
		{"tail is the head", Tail(), 8},
		{"last n", LastN(2), 6},
		{"last n past the floor clamps to it", LastN(100), 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.resolveStart(tc.pos)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestStartPositionsThatNameNothingAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		pos  *streampb.StreamStartPosition
	}{
		{"no arm", &streampb.StreamStartPosition{}},
		{"negative offset", AtOffset(-1)},
		{"zero last n", LastN(0)},
		{"negative last n", LastN(-3)},
		{"earliest false", &streampb.StreamStartPosition{
			Position: &streampb.StreamStartPosition_Earliest{Earliest: false},
		}},
		{"tail false", &streampb.StreamStartPosition{
			Position: &streampb.StreamStartPosition_Tail{Tail: false},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var invalid *serviceerror.InvalidArgument
			require.ErrorAs(t, CheckStart(tc.pos), &invalid)
		})
	}
}

func TestRequestedStartReadsEitherField(t *testing.T) {
	got, err := RequestedStart(nil, "start_offset", 4)
	require.NoError(t, err)
	require.Equal(t, int64(4), got.GetOffset())

	got, err = RequestedStart(nil, "start_offset", 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), got.GetOffset(), "an unset position and offset is offset zero")

	got, err = RequestedStart(Earliest(), "start_offset", 0)
	require.NoError(t, err)
	require.True(t, got.GetEarliest())

	var invalid *serviceerror.InvalidArgument
	_, err = RequestedStart(Earliest(), "start_offset", 2)
	require.ErrorAs(t, err, &invalid, "both fields set could disagree")
	require.ErrorContains(t, err, "start_offset")
}

func TestRegisterConsumerRecordsTheResolvedStart(t *testing.T) {
	s := newTruncatedStream(t)

	start, err := s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:a", WorkflowID: "wf-1", RunID: "run-1", Start: Earliest(),
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), start)
	require.Equal(t, int64(3), s.State.Consumers["workflow:a"].GetReplayFloor())

	start, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:b", WorkflowID: "wf-1", RunID: "run-1", Start: LastN(1),
	})
	require.NoError(t, err)
	require.Equal(t, int64(7), start)

	// A consumer already registered keeps its position whatever it asks for.
	start, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:a", WorkflowID: "wf-1", RunID: "run-1", Start: Tail(),
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), start)

	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:c", WorkflowID: "wf-1", RunID: "run-1", Start: AtOffset(0),
	})
	var failed *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failed, "an absolute offset below the floor is still refused")
}

func TestReadWindowResolvesAStartPositionInTheSameRead(t *testing.T) {
	s := newTruncatedStream(t)
	ctx := &chasm.MockContext{
		HandleExecutionKey: func() chasm.ExecutionKey { return chasm.ExecutionKey{RunID: "run-1"} },
	}

	w, err := s.ReadWindow(ctx, WindowRequest{Start: Earliest()})
	require.NoError(t, err)
	require.Equal(t, int64(3), w.From, "earliest reads from the floor of a truncated stream")
	require.Equal(t, int64(8), w.To)

	w, err = s.ReadWindow(ctx, WindowRequest{Start: LastN(2)})
	require.NoError(t, err)
	require.Equal(t, int64(6), w.From)

	w, err = s.ReadWindow(ctx, WindowRequest{Start: Tail()})
	require.NoError(t, err)
	require.Equal(t, int64(8), w.From)
	require.Empty(t, w.Blobs)
}
