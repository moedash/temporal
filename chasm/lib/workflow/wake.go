package workflow

import (
	"slices"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
)

// readOnly hides the mutable half of a context. Reaching a data field through
// a mutable context marks it for persistence, and the wake table is read on
// paths that must leave an execution untouched when nothing changes.
func readOnly(ctx chasm.Context) chasm.Context {
	return struct{ chasm.Context }{ctx}
}

func undelivered(entry *chasmworkflowpb.WakeEntry) bool {
	return entry.GetDeliveredGeneration() < entry.GetGeneration()
}

// WakeIsDuplicate reports whether a wake changes nothing: the source already
// has a wake no started task has carried, at this counter or a higher one.
func (w *Workflow) WakeIsDuplicate(ctx chasm.Context, source string, counter int64) bool {
	field, ok := w.Wakes[source]
	if !ok {
		return false
	}
	entry := field.Get(readOnly(ctx))
	return undelivered(entry) && entry.GetCounter() >= counter
}

// AcceptWake records that a source this run consumes has moved, which is a
// reason to run a Workflow Task and nothing more: no event is written. It
// reports whether the wake folded into one already owed to the workflow.
//
// A wake folds only into a wake that no started task has carried. One that
// arrives after a task took the entry starts a new generation, whatever its
// counter: only the task knows what it read, so a wake the server has handed
// out is never proof that the source's latest position was seen.
//
// Within a generation the higher counter wins, so the task that carries it
// gets the latest position. A caller that must not dirty the execution on a
// pure duplicate asks WakeIsDuplicate with a read-only context first, since
// reaching this component mutably is already a write.
//
// The table holds one entry per source with a wake pending, and a new source
// past the limit is refused rather than dropping one that is still owed.
func (w *Workflow) AcceptWake(
	mctx chasm.MutableContext,
	source string,
	position []byte,
	counter int64,
	limit int,
) (bool, error) {
	if field, ok := w.Wakes[source]; ok {
		current := field.Get(readOnly(mctx))
		if undelivered(current) {
			if counter > current.GetCounter() {
				entry := field.Get(mctx)
				entry.Position = position
				entry.Counter = counter
			}
			return true, nil
		}
		entry := field.Get(mctx)
		entry.Position = position
		entry.Counter = counter
		entry.Generation++
		return false, nil
	}

	if limit > 0 && len(w.Wakes) >= limit {
		return false, serviceerror.NewResourceExhaustedf(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			"workflow already has wakes pending from %d sources", len(w.Wakes))
	}
	if w.Wakes == nil {
		w.Wakes = make(chasm.Map[string, *chasmworkflowpb.WakeEntry])
	}
	w.Wakes[source] = chasm.NewDataField(mctx, &chasmworkflowpb.WakeEntry{
		Position:   position,
		Counter:    counter,
		Generation: 1,
	})
	return false, nil
}

// HasPendingWakes reports whether any source has a wake that a completed
// Workflow Task has not seen.
func (w *Workflow) HasPendingWakes() bool {
	return len(w.Wakes) > 0
}

// HasUndeliveredWakes reports whether a wake has arrived that no started
// Workflow Task has carried, which is what schedules a task for it.
//
// A wake handed to a task which then failed is not counted: the failure
// schedules the next attempt itself, and that attempt carries it again.
func (w *Workflow) HasUndeliveredWakes(ctx chasm.Context) bool {
	view := readOnly(ctx)
	for _, field := range w.Wakes {
		if undelivered(field.Get(view)) {
			return true
		}
	}
	return false
}

// PendingWakes returns the wakes a task starting now would carry, sorted by
// source, without recording that any were handed out.
func (w *Workflow) PendingWakes(ctx chasm.Context) []*workflowpb.Wake {
	view := readOnly(ctx)
	out := make([]*workflowpb.Wake, 0, len(w.Wakes))
	for source, field := range w.Wakes {
		entry := field.Get(view)
		out = append(out, &workflowpb.Wake{
			Source:   source,
			Position: entry.GetPosition(),
			Counter:  entry.GetCounter(),
		})
	}
	slices.SortFunc(out, func(a, b *workflowpb.Wake) int {
		return strings.Compare(a.GetSource(), b.GetSource())
	})
	return out
}

// TakeWakesForTask returns every pending wake for a Workflow Task that is
// starting, and records the generation each was handed out at.
//
// An entry an earlier task carried and did not complete goes out again, so a
// failed or timed out task loses nothing.
func (w *Workflow) TakeWakesForTask(mctx chasm.MutableContext) []*workflowpb.Wake {
	out := w.PendingWakes(mctx)
	for _, wake := range out {
		entry := w.Wakes[wake.GetSource()].Get(mctx)
		entry.DeliveredGeneration = entry.GetGeneration()
	}
	return out
}

// AckDeliveredWakes deletes the wakes a completed Workflow Task carried and
// returns how many. Called while the completion's transaction is open. An
// entry that gained a generation while the task ran stays, so the transaction
// close schedules the task that carries it.
func (w *Workflow) AckDeliveredWakes(mctx chasm.MutableContext) int {
	view := readOnly(mctx)
	var done []string
	for source, field := range w.Wakes {
		if !undelivered(field.Get(view)) {
			done = append(done, source)
		}
	}
	for _, source := range done {
		delete(w.Wakes, source)
	}
	return len(done)
}
