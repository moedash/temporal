package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
)

func newWakeTestContext() chasm.MutableContext {
	return &chasm.MockMutableContext{}
}

func acceptWake(
	t *testing.T,
	w *Workflow,
	ctx chasm.MutableContext,
	source string,
	position string,
	counter int64,
) bool {
	t.Helper()
	folded, err := w.AcceptWake(ctx, source, []byte(position), counter, 8)
	require.NoError(t, err)
	return folded
}

// Wakes that arrive before any task starts fold into one, and the task gets
// the highest counter's position.
func TestWakesFoldWhileUndelivered(t *testing.T) {
	ctx := newWakeTestContext()
	w := &Workflow{}

	require.False(t, acceptWake(t, w, ctx, "orders", "p1", 1))
	require.True(t, acceptWake(t, w, ctx, "orders", "p3", 3))
	require.True(t, acceptWake(t, w, ctx, "orders", "p2", 2), "a lower counter folds too")
	require.True(t, w.WakeIsDuplicate(ctx, "orders", 3))
	require.True(t, w.WakeIsDuplicate(ctx, "orders", 2))
	require.False(t, w.WakeIsDuplicate(ctx, "orders", 4))
	require.False(t, w.WakeIsDuplicate(ctx, "unknown", 1))

	taken := w.TakeWakesForTask(ctx)
	require.Len(t, taken, 1)
	require.Equal(t, int64(3), taken[0].GetCounter())
	require.Equal(t, []byte("p3"), taken[0].GetPosition())
}

// Only the task knows what it read, so a wake that arrives after a task took
// the entry is accepted, even at the same or a lower counter, and stays for
// the next task when the open one completes.
func TestWakeAfterDeliveryStartsANewGeneration(t *testing.T) {
	ctx := newWakeTestContext()
	w := &Workflow{}

	require.False(t, acceptWake(t, w, ctx, "orders", "p5", 5))
	w.TakeWakesForTask(ctx)
	require.False(t, w.HasUndeliveredWakes(ctx))
	require.False(t, w.WakeIsDuplicate(ctx, "orders", 5), "a carried wake is never a duplicate")

	require.False(t, acceptWake(t, w, ctx, "orders", "p3", 3))
	require.True(t, w.HasUndeliveredWakes(ctx))
	require.True(t, acceptWake(t, w, ctx, "orders", "p3", 3), "the new generation folds repeats")

	require.Equal(t, 0, w.AckDeliveredWakes(ctx), "the newer generation stays")
	taken := w.TakeWakesForTask(ctx)
	require.Len(t, taken, 1)
	require.Equal(t, int64(3), taken[0].GetCounter())
	require.Equal(t, []byte("p3"), taken[0].GetPosition())
	require.Equal(t, 1, w.AckDeliveredWakes(ctx))
	require.False(t, w.HasPendingWakes())
}

// A completed task deletes what it carried, so a later wake with the same
// counter is a new wake and not a fold. This is the stall a watcher would
// otherwise hit after a task drained nothing.
func TestCompletedTaskDeletesItsWakes(t *testing.T) {
	ctx := newWakeTestContext()
	w := &Workflow{}

	require.False(t, acceptWake(t, w, ctx, "b", "b1", 1))
	require.False(t, acceptWake(t, w, ctx, "a", "a4", 4))
	taken := w.TakeWakesForTask(ctx)
	require.Len(t, taken, 2)
	require.Equal(t, "a", taken[0].GetSource(), "wakes go out sorted by source")
	require.Equal(t, "b", taken[1].GetSource())

	require.Equal(t, 2, w.AckDeliveredWakes(ctx))
	require.False(t, w.HasPendingWakes())
	require.Empty(t, w.TakeWakesForTask(ctx))

	require.False(t, acceptWake(t, w, ctx, "a", "a4", 4))
	require.True(t, w.HasUndeliveredWakes(ctx))
}

// A task that fails acks nothing, so the next attempt carries the same wakes.
func TestWakesAreRedeliveredAfterAFailedTask(t *testing.T) {
	ctx := newWakeTestContext()
	w := &Workflow{}

	require.False(t, acceptWake(t, w, ctx, "orders", "p7", 7))
	first := w.TakeWakesForTask(ctx)
	require.Len(t, first, 1)
	require.False(t, w.HasUndeliveredWakes(ctx), "the failure schedules the retry itself")

	require.Equal(t, first, w.PendingWakes(ctx))
	require.Equal(t, first, w.TakeWakesForTask(ctx))
}

// The limit counts sources with a wake pending, and nothing is evicted to
// make room.
func TestAcceptWakeRefusesANewSourcePastTheLimit(t *testing.T) {
	ctx := newWakeTestContext()
	w := &Workflow{}

	_, err := w.AcceptWake(ctx, "a", nil, 1, 2)
	require.NoError(t, err)
	_, err = w.AcceptWake(ctx, "b", nil, 1, 2)
	require.NoError(t, err)

	_, err = w.AcceptWake(ctx, "c", nil, 1, 2)
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, exhausted.Cause)
	require.Len(t, w.Wakes, 2)

	folded, err := w.AcceptWake(ctx, "a", nil, 2, 2)
	require.NoError(t, err, "a source already pending still moves at the limit")
	require.True(t, folded)

	w.TakeWakesForTask(ctx)
	w.AckDeliveredWakes(ctx)
	_, err = w.AcceptWake(ctx, "c", nil, 1, 2)
	require.NoError(t, err, "completed sources free their slots")
}
