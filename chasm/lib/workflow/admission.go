package workflow

import (
	"context"
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// AdmissionFailure turns a refusal of a command into a workflow task failure
// with the given cause.
//
// A refusal returned as a plain error fails the RespondWorkflowTaskCompleted
// call instead, and the worker then retries the same commands against the same
// limits until the task times out, with nothing in History saying why. Only
// refusals are converted: an internal or storage error is still the request's
// to report, because a retry can succeed.
func AdmissionFailure(cause enumspb.WorkflowTaskFailedCause, err error) error {
	var invalid *serviceerror.InvalidArgument
	var precondition *serviceerror.FailedPrecondition
	var exhausted *serviceerror.ResourceExhausted
	var notFound *serviceerror.NotFound
	if errors.As(err, &invalid) || errors.As(err, &precondition) ||
		errors.As(err, &exhausted) || errors.As(err, &notFound) {
		return FailWorkflowTaskError{Cause: cause, Message: err.Error()}
	}
	return err
}

// RoutedSetBudget bounds every routed call one workflow task makes while the
// execution's lock is held, taken together. A routed call has a deadline of
// its own, and a task can carry as many as the subscription limit allows, so
// without this the lock hold grows with that count.
const RoutedSetBudget = 15 * time.Second

// WithRoutedBudget bounds a whole set of routed calls made under one workflow
// lock.
//
// Each call has a deadline of its own, but a task can carry as many of them as
// the subscription limit allows, and per-call deadlines multiplied by that
// count is how long the lock could be held. One budget over the set is what
// actually bounds it: a call that runs out of it fails, and the workflow task
// fails with it rather than the lock being held for minutes.
func WithRoutedBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, RoutedSetBudget)
}
