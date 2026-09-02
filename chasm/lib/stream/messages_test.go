package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

func sized(n int, bytes int) []*streampb.StreamMessage {
	out := make([]*streampb.StreamMessage, n)
	for i := range out {
		out[i] = &streampb.StreamMessage{
			Body: &commonpb.Payload{Data: make([]byte, bytes)},
			Kind: streampb.STREAM_MESSAGE_KIND_DATA,
		}
	}
	return out
}

func TestCapByBytesTrimsToAPrefix(t *testing.T) {
	messages, next := CapByBytes(sized(10, 100), 4, 250)
	require.Len(t, messages, 2)
	require.Equal(t, int64(6), next, "the recorded range must cover exactly what was kept")
}

func TestCapByBytesKeepsEverythingUnderBudget(t *testing.T) {
	messages, next := CapByBytes(sized(3, 10), 0, 1<<20)
	require.Len(t, messages, 3)
	require.Equal(t, int64(3), next)
}

// A single oversized message must still go out. Held back it would stall the
// cursor, and an unconsumed range schedules a workflow task, so the workflow
// would wake forever and never receive anything.
func TestCapByBytesAlwaysDeliversTheFirstMessage(t *testing.T) {
	messages, next := CapByBytes(sized(3, 5000), 7, 10)
	require.Len(t, messages, 1)
	require.Equal(t, int64(8), next)
}

func TestCapByBytesOnAnEmptyRun(t *testing.T) {
	messages, next := CapByBytes(nil, 9, 100)
	require.Empty(t, messages)
	require.Equal(t, int64(9), next)
}
