package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// agingStream is a standalone stream with a retention, driven by a clock the
// test moves, so batches carry the times the test chooses.
func agingStream(t *testing.T, retention time.Duration) (*Stream, *chasm.MockMutableContext, *time.Time) {
	t.Helper()
	// UTC, since a stamp read back from a batch is UTC and the assertions
	// compare instants with their locations.
	now := time.Unix(1_700_000_000, 0).UTC()
	mctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return now },
	}}
	s := newTestStream(t)
	s.Batches = make(chasm.Map[int64, *commonpb.DataBlob])
	s.State.Lifecycle = &streamlib.StreamLifecycle{Retention: durationpb.New(retention)}
	return s, mctx, &now
}

// pinConsumer registers an active workflow consumer whose replay floor is at
// offset, written straight into the table so the test does not depend on how
// a registration names its start.
func pinConsumer(s *Stream, id string, offset int64) {
	s.State.Consumers[id] = &streamlib.ConsumerCursor{
		WorkflowId: "wf", RunId: "run", Offset: offset, ReplayFloor: offset,
		Active: true, External: true,
	}
}

func ageTasks(mctx *chasm.MockMutableContext) []chasm.MockTask {
	var out []chasm.MockTask
	for _, task := range mctx.Tasks {
		if _, ok := task.Payload.(*streamlib.StreamAgeTask); ok {
			out = append(out, task)
		}
	}
	return out
}

func TestFirstAppendArmsOneAgeCheckAFullRetentionOut(t *testing.T) {
	s, mctx, now := agingStream(t, time.Hour)
	start := *now

	_, err := s.AddMessages(mctx, AddMessagesRequest{Records: msgs("a")})
	require.NoError(t, err)
	*now = now.Add(time.Minute)
	_, err = s.AddMessages(mctx, AddMessagesRequest{Records: msgs("b")})
	require.NoError(t, err)

	tasks := ageTasks(mctx)
	require.Len(t, tasks, 1, "one check outstanding at a time")
	require.Equal(t, start.Add(time.Hour), tasks[0].Attributes.ScheduledTime,
		"nothing can have aged before a full retention")
	require.True(t, s.State.AgeTaskPending)
}

func TestTruncateAgedReclaimsOnlyWhatHasAged(t *testing.T) {
	s, mctx, now := agingStream(t, time.Hour)
	_, err := s.AddMessages(mctx, AddMessagesRequest{Records: msgs("a", "b")})
	require.NoError(t, err)
	*now = now.Add(30 * time.Minute)
	_, err = s.AddMessages(mctx, AddMessagesRequest{Records: msgs("c")})
	require.NoError(t, err)
	*now = now.Add(20 * time.Minute)
	_, err = s.AddMessages(mctx, AddMessagesRequest{Records: msgs("d")})
	require.NoError(t, err)
	secondAppendedAt := now.Add(-20 * time.Minute)

	// Ten minutes past the first batch's retention: it goes, the second is
	// still ten minutes short, and that is when the next check is due.
	*now = secondAppendedAt.Add(50 * time.Minute)
	next, err := s.TruncateAged(mctx, *now)
	require.NoError(t, err)
	require.Equal(t, int64(2), s.State.BaseOffset)
	require.Equal(t, secondAppendedAt.Add(time.Hour), next)
	require.Len(t, s.Batches, 2)
	require.Positive(t, s.State.HeldBytes)

	// Long after everything aged: the floor reaches the head and nothing is
	// waiting to age.
	*now = now.Add(24 * time.Hour)
	next, err = s.TruncateAged(mctx, *now)
	require.NoError(t, err)
	require.Equal(t, s.State.HeadOffset, s.State.BaseOffset)
	require.True(t, next.IsZero())
	require.Empty(t, s.Batches)
	require.Equal(t, int64(0), s.State.HeldBytes)
}

func TestTruncateAgedStopsAtAnActiveConsumersFloor(t *testing.T) {
	s, mctx, now := agingStream(t, time.Hour)
	_, err := s.AddMessages(mctx, AddMessagesRequest{Records: msgs("a", "b")})
	require.NoError(t, err)
	_, err = s.AddMessages(mctx, AddMessagesRequest{Records: msgs("c", "d")})
	require.NoError(t, err)
	pinConsumer(s, "workflow:wf/run", 2)

	*now = now.Add(2 * time.Hour)
	next, err := s.TruncateAged(mctx, *now)
	require.NoError(t, err)
	require.Equal(t, int64(2), s.State.BaseOffset, "aged, but only up to the consumer's floor")
	require.Equal(t, *now, next, "asked about again, in case the consumer lets go")
	require.Len(t, s.Batches, 1)

	// Once the consumer is gone the rest ages out.
	s.ForgetConsumer(mctx, "workflow:wf/run")
	next, err = s.TruncateAged(mctx, *now)
	require.NoError(t, err)
	require.Equal(t, int64(4), s.State.BaseOffset)
	require.True(t, next.IsZero())
}

func TestUnstampedAndClosedStreamsDoNotAge(t *testing.T) {
	// Written without a clock, as batches were before they carried a time.
	s := newTestStream(t)
	s.State.Lifecycle = &streamlib.StreamLifecycle{Retention: durationpb.New(time.Second)}
	_, err := s.AddMessages(nil, AddMessagesRequest{Records: msgs("old")})
	require.NoError(t, err)
	next, err := s.TruncateAged(nil, time.Unix(2_000_000_000, 0))
	require.NoError(t, err)
	require.Equal(t, int64(0), s.State.BaseOffset)
	require.True(t, next.IsZero())

	// Closed: the retention counts down to deletion instead.
	closed, mctx, now := agingStream(t, time.Second)
	_, err = closed.AddMessages(mctx, AddMessagesRequest{Records: msgs("tail")})
	require.NoError(t, err)
	closed.Close(*now, nil)
	next, err = closed.TruncateAged(mctx, now.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, int64(0), closed.State.BaseOffset, "a draining consumer is owed the tail")
	require.True(t, next.IsZero())
}

func TestRunAgeCheckReArmsWhileRecordsAreHeldAndStandsDownWhenNoneAre(t *testing.T) {
	s, mctx, now := agingStream(t, time.Hour)
	_, err := s.AddMessages(mctx, AddMessagesRequest{Records: msgs("a")})
	require.NoError(t, err)
	*now = now.Add(30 * time.Minute)
	_, err = s.AddMessages(mctx, AddMessagesRequest{Records: msgs("b")})
	require.NoError(t, err)
	require.Len(t, ageTasks(mctx), 1)

	// The first check fires at the first batch's expiry: it reclaims that
	// batch and re-arms for the second's, which is further off than a recheck.
	*now = now.Add(30 * time.Minute)
	require.NoError(t, s.RunAgeCheck(mctx, time.Minute))
	require.Equal(t, int64(1), s.State.BaseOffset)
	tasks := ageTasks(mctx)
	require.Len(t, tasks, 2)
	require.Equal(t, now.Add(30*time.Minute), tasks[1].Attributes.ScheduledTime)
	require.True(t, s.State.AgeTaskPending)

	// A check that finds a batch held by a consumer asks again after the
	// recheck interval rather than at once.
	pinConsumer(s, "workflow:wf/run", 1)
	*now = now.Add(time.Hour)
	require.NoError(t, s.RunAgeCheck(mctx, time.Minute))
	require.Equal(t, int64(1), s.State.BaseOffset)
	tasks = ageTasks(mctx)
	require.Len(t, tasks, 3)
	require.Equal(t, now.Add(time.Minute), tasks[2].Attributes.ScheduledTime)

	// With nothing held the check stands down, and the next append arms it.
	s.ForgetConsumer(mctx, "workflow:wf/run")
	require.NoError(t, s.RunAgeCheck(mctx, time.Minute))
	require.Equal(t, s.State.HeadOffset, s.State.BaseOffset)
	require.False(t, s.State.AgeTaskPending)
	require.Len(t, ageTasks(mctx), 3)
	_, err = s.AddMessages(mctx, AddMessagesRequest{Records: msgs("c")})
	require.NoError(t, err)
	require.Len(t, ageTasks(mctx), 4)
}
