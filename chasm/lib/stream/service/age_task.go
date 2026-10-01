package service

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// ageTaskHandler reclaims what has aged past an open stream's retention.
//
// A pure task: it reads the oldest batches for their age and moves the floor
// in one transition, with nothing to say to anyone else. The component decides
// whether to arm the next check and when, so the handler only supplies the
// cadence it re-arms on.
type ageTaskHandler struct {
	chasm.PureTaskHandlerBase

	config *stream.Config
}

func newAgeTaskHandler(config *stream.Config) *ageTaskHandler {
	return &ageTaskHandler{config: config}
}

// Validate lets the check run while it is the one outstanding and the stream
// is open. A closed stream's retention counts down to deletion instead, and a
// check whose flag is already down was superseded.
func (h *ageTaskHandler) Validate(
	_ chasm.Context,
	s *stream.Stream,
	_ chasm.TaskInvocation,
	_ *streampb.StreamAgeTask,
) (bool, error) {
	return s.State.GetAgeTaskPending() && !s.State.GetClosed(), nil
}

func (h *ageTaskHandler) Execute(
	mctx chasm.MutableContext,
	s *stream.Stream,
	_ chasm.TaskAttributes,
	_ *streampb.StreamAgeTask,
) error {
	return s.RunAgeCheck(mctx, h.config.RetentionRecheckInterval())
}
