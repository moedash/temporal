package service

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// The append that scheduled this task also raised the coalescing flag, and
// nothing but the task's own run lowers it. Turning the task down for a stream
// whose consumers have gone quiet would leave the flag up for good, and no
// later append would schedule another task, so a consumer that subscribes
// afterwards would never be woken.
func TestNotifyTaskIsNotTurnedDownWhileTheCoalescingFlagIsUp(t *testing.T) {
	h := &notifyConsumersTaskHandler{}
	s := &stream.Stream{State: &streampb.StreamState{
		NotifyPending: true,
		Consumers:     map[string]*streampb.ConsumerCursor{},
	}}

	ok, err := h.Validate(nil, s, chasm.TaskInvocation{}, &streampb.StreamNotifyConsumersTask{})
	require.NoError(t, err)
	require.True(t, ok, "the task has to run so that its own read lowers the flag")

	// An inactive entry is the same case: whoever the append meant to tell has
	// gone, and the flag still has to come down.
	s.State.Consumers["workflow:wf/run-1"] = &streampb.ConsumerCursor{External: true, Active: false}
	ok, err = h.Validate(nil, s, chasm.TaskInvocation{}, &streampb.StreamNotifyConsumersTask{})
	require.NoError(t, err)
	require.True(t, ok)
}
