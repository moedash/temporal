package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	streampb "go.temporal.io/api/stream/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

func sized(n int, bytes int) []*streamlib.StreamRecord {
	out := make([]*streamlib.StreamRecord, n)
	for i := range out {
		out[i] = &streamlib.StreamRecord{
			Body: &commonpb.Payload{Data: make([]byte, bytes)},
			Kind: streampb.STREAM_RECORD_KIND_DATA,
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

// A reader in any language decodes the record the producer wrote, so every
// field the store holds has to come back on the Workflow Task, the kind and the
// producer identity included. A FINISH record is a record like any other to the
// consumer that reads it.
func TestToAPIRecordsCarriesTheRecordAsWritten(t *testing.T) {
	stored := []*streamlib.StreamRecord{
		{
			Body:       &commonpb.Payload{Data: []byte("token")},
			Metadata:   map[string]*commonpb.Payload{"model": {Data: []byte("m1")}},
			Topic:      "tokens",
			Kind:       streampb.STREAM_RECORD_KIND_DATA,
			ProducerId: "model-call",
			Attempt:    2,
			Sequence:   7,
			Offset:     41,
		},
		{
			Topic:      "tokens",
			Kind:       streampb.STREAM_RECORD_KIND_FINISH,
			ProducerId: "model-call",
			Attempt:    2,
			Sequence:   8,
			Offset:     42,
		},
	}

	got := ToAPIRecords(stored)
	require.Len(t, got, 2)

	require.Equal(t, "token", string(got[0].GetBody().GetData()))
	require.Equal(t, "m1", string(got[0].GetMetadata()["model"].GetData()))
	require.Equal(t, "tokens", got[0].GetTopic())
	require.Equal(t, streampb.STREAM_RECORD_KIND_DATA, got[0].GetKind())
	require.Equal(t, "model-call", got[0].GetProducerId())
	require.Equal(t, int64(2), got[0].GetAttempt())
	require.Equal(t, int64(7), got[0].GetSequence())

	require.Equal(t, streampb.STREAM_RECORD_KIND_FINISH, got[1].GetKind())
	require.Nil(t, got[1].GetBody())
	require.Equal(t, "model-call", got[1].GetProducerId())
	require.Equal(t, int64(8), got[1].GetSequence())
}
