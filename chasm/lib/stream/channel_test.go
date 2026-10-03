package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/payload"
)

func newChannelTestContext() *chasm.MockMutableContext {
	now := time.Unix(1_700_000_000, 0).UTC()
	return &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return now },
	}}
}

// Every append and the close are one change each, a producer's retry none,
// and the notification for a change carries the sequence, the head paired
// with the run, and closed only on the close.
func TestStreamChangeNotification(t *testing.T) {
	ctx := newChannelTestContext()
	s := newTestStream(t)

	_, err := s.AddMessages(ctx, AddMessagesRequest{Records: msgs("a", "b")})
	require.NoError(t, err)
	n := s.ChangeNotification("stream/s", "run-1")
	require.Equal(t, "stream/s", n.GetChannel())
	require.Equal(t, int64(1), n.GetCounter())
	require.Equal(t, "run-1:2", string(n.GetPosition()))
	require.Empty(t, n.GetMetadata())

	retry := AddMessagesRequest{Records: msgs("c"), ProducerID: "p", Sequence: 1}
	_, err = s.AddMessages(ctx, retry)
	require.NoError(t, err)
	replay, err := s.AddMessages(ctx, retry)
	require.NoError(t, err)
	require.True(t, replay.Deduplicated)
	require.Equal(t, int64(2), s.ChangeNotification("stream/s", "run-1").GetCounter(),
		"a replay is not a change")

	require.NoError(t, s.CloseAndSchedule(ctx, nil))
	n = s.ChangeNotification("stream/s", "run-1")
	require.Equal(t, int64(3), n.GetCounter())
	require.Equal(t, "run-1:3", string(n.GetPosition()))
	var closed bool
	require.NoError(t, payload.Decode(n.GetMetadata()[ClosedMetadataKey], &closed))
	require.True(t, closed)
	s.Close(ctx.Now(s), nil)
	require.Equal(t, int64(3), s.State.GetChangeSequence(), "a second close is not a change")
}

// One channel task is outstanding at a time, it reads the latest change when
// it runs and lowers the flag, and the switch leaves none.
func TestStreamScheduleChannelNotify(t *testing.T) {
	ctx := newChannelTestContext()
	s := newTestStream(t)
	_, err := s.AddMessages(ctx, AddMessagesRequest{Records: msgs("a")})
	require.NoError(t, err)

	s.ScheduleChannelNotify(ctx, "stream/s", "s", Limits{ChannelNotifyOff: true})
	require.Empty(t, ctx.Tasks)

	s.ScheduleChannelNotify(ctx, "stream/s", "s", Limits{})
	require.Len(t, ctx.Tasks, 1)
	task, ok := ctx.Tasks[0].Payload.(*streamlib.StreamNotifyChannelTask)
	require.True(t, ok, "%T", ctx.Tasks[0].Payload)
	require.Equal(t, "stream/s", task.GetChannel())
	require.Equal(t, "s", task.GetRun())

	_, err = s.AddMessages(ctx, AddMessagesRequest{Records: msgs("b")})
	require.NoError(t, err)
	s.ScheduleChannelNotify(ctx, "stream/s", "s", Limits{})
	require.Len(t, ctx.Tasks, 1, "one task at a time")

	n, err := s.TakeChannelChange(ctx, task)
	require.NoError(t, err)
	require.Equal(t, int64(2), n.GetCounter(), "the task reads the latest change")
	require.Equal(t, "s:2", string(n.GetPosition()))
	s.ScheduleChannelNotify(ctx, "stream/s", "s", Limits{})
	require.Len(t, ctx.Tasks, 2, "the flag is down, so the next change arms a task")
}

// The channel a stream announces on follows from its owner alone: a workflow's
// own stream and a standalone activity's are linked to that execution under
// the stream's name, a workflow activity's is linked to the workflow under the
// activity's and the stream's names.
func TestOwnedChannelAddress(t *testing.T) {
	name, linkedTo := OwnedChannelAddress(&streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW, Id: "wf", RunId: "run-1",
	}, "output")
	require.Equal(t, "stream/output", name)
	require.Equal(t, enumspb.EXECUTION_TYPE_WORKFLOW, linkedTo.GetType())
	require.Equal(t, "wf", linkedTo.GetBusinessId())
	require.Equal(t, "run-1", linkedTo.GetRunId())

	name, linkedTo = OwnedChannelAddress(&streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY, Id: "wf", ActivityId: "model-call",
	}, "output")
	require.Equal(t, "stream/model-call/output", name)
	require.Equal(t, enumspb.EXECUTION_TYPE_WORKFLOW, linkedTo.GetType())
	require.Equal(t, "wf", linkedTo.GetBusinessId())
	require.Empty(t, linkedTo.GetRunId())

	name, linkedTo = OwnedChannelAddress(&streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_ACTIVITY, Id: "act",
	}, "output")
	require.Equal(t, "stream/output", name)
	require.Equal(t, enumspb.EXECUTION_TYPE_ACTIVITY, linkedTo.GetType())
	require.Equal(t, "act", linkedTo.GetBusinessId())
}
