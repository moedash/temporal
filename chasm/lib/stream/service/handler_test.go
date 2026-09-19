package service

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
)

func batchBlob(t *testing.T, topic string, bodies ...string) *commonpb.DataBlob {
	t.Helper()
	messages := make([]*streampb.StreamMessage, len(bodies))
	for i, b := range bodies {
		messages[i] = &streampb.StreamMessage{
			Body:  &commonpb.Payload{Data: []byte(b)},
			Topic: topic,
			Kind:  streampb.STREAM_MESSAGE_KIND_DATA,
		}
	}
	data, err := proto.Marshal(&streampb.StreamMessageBatch{Messages: messages})
	require.NoError(t, err)
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: data}
}

// A filtered page that matched nothing still advances the reader, but only
// over the offsets that were actually examined. A window whose batches stop
// short of its end must not report the end as the next offset, or the reader
// steps over messages it was never shown.
func TestFormatWindowAdvancesOnlyOverExaminedOffsets(t *testing.T) {
	w := stream.Window{
		State:  &streampb.StreamState{HeadOffset: 10},
		Blobs:  []*commonpb.DataBlob{batchBlob(t, "a", "m0", "m1", "m2")},
		Starts: []int64{0},
		To:     5,
		Limit:  5,
	}
	out, err := formatWindow(w, stream.WindowRequest{From: 0, Topics: []string{"nothing"}})
	require.NoError(t, err)
	require.Empty(t, out.GetMessages())
	require.Equal(t, int64(3), out.GetNextOffset(),
		"offsets 3 and 4 were never read, so the reader must not be moved past them")

	// A page that filtered everything out but covered its whole window does
	// advance to the end, so the reader does not loop on the same offsets.
	w.Blobs = []*commonpb.DataBlob{batchBlob(t, "a", "m0", "m1", "m2", "m3", "m4")}
	out, err = formatWindow(w, stream.WindowRequest{From: 0, Topics: []string{"nothing"}})
	require.NoError(t, err)
	require.Empty(t, out.GetMessages())
	require.Equal(t, int64(5), out.GetNextOffset())
}
