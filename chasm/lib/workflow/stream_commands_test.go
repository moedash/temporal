package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	streampb "go.temporal.io/api/stream/v1"
)

// An empty producer id is how a reader tells the owning workflow's records from
// an outside producer's. The command path is the only writer on the workflow's
// behalf, so it decides the field rather than trusting what the worker sent,
// while the attempt and sequence stay the worker's to number.
func TestPublishedRecordsBelongToTheWorkflow(t *testing.T) {
	got := toLibraryRecords([]*streampb.StreamRecord{
		{
			Body:       &commonpb.Payload{Data: []byte("token")},
			Metadata:   map[string]*commonpb.Payload{"k": {Data: []byte("v")}},
			Topic:      "tokens",
			Kind:       streampb.STREAM_RECORD_KIND_FINISH,
			ProducerId: "impostor",
			Attempt:    3,
			Sequence:   9,
		},
		{Body: &commonpb.Payload{Data: []byte("kindless")}},
	})
	require.Len(t, got, 2)

	require.Equal(t, "token", string(got[0].GetBody().GetData()))
	require.Equal(t, "v", string(got[0].GetMetadata()["k"].GetData()))
	require.Equal(t, "tokens", got[0].GetTopic())
	require.Equal(t, streampb.STREAM_RECORD_KIND_FINISH, got[0].GetKind())
	require.Empty(t, got[0].GetProducerId(), "the workflow is the producer here")
	require.Equal(t, int64(3), got[0].GetAttempt())
	require.Equal(t, int64(9), got[0].GetSequence())

	require.Equal(t, streampb.STREAM_RECORD_KIND_DATA, got[1].GetKind(),
		"a record with no kind is data")
	require.Empty(t, got[1].GetProducerId())
}
