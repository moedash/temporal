package service

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"google.golang.org/protobuf/proto"
)

func batchBlob(t *testing.T, topic string, bodies ...string) *commonpb.DataBlob {
	t.Helper()
	messages := make([]*streamlib.StreamRecord, len(bodies))
	for i, b := range bodies {
		messages[i] = &streamlib.StreamRecord{
			Body:  &commonpb.Payload{Data: []byte(b)},
			Topic: topic,
			Kind:  streampb.STREAM_RECORD_KIND_DATA,
		}
	}
	data, err := proto.Marshal(&streamlib.StreamRecordBatch{Records: messages})
	require.NoError(t, err)
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: data}
}

// A filtered page that matched nothing still advances the reader, but only
// over the offsets that were actually examined. A window whose batches stop
// short of its end must not report the end as the next offset, or the reader
// steps over messages it was never shown.
func TestFormatWindowAdvancesOnlyOverExaminedOffsets(t *testing.T) {
	w := stream.Window{
		State:  &streamlib.StreamState{HeadOffset: 10},
		Blobs:  []*commonpb.DataBlob{batchBlob(t, "a", "m0", "m1", "m2")},
		Starts: []int64{0},
		To:     5,
		Limit:  5,
	}
	out, err := formatWindow(w, stream.WindowRequest{From: 0, Topics: []string{"nothing"}})
	require.NoError(t, err)
	require.Empty(t, out.GetRecords())
	require.Equal(t, int64(3), out.GetNextOffset(),
		"offsets 3 and 4 were never read, so the reader must not be moved past them")

	// A page that filtered everything out but covered its whole window does
	// advance to the end, so the reader does not loop on the same offsets.
	w.Blobs = []*commonpb.DataBlob{batchBlob(t, "a", "m0", "m1", "m2", "m3", "m4")}
	out, err = formatWindow(w, stream.WindowRequest{From: 0, Topics: []string{"nothing"}})
	require.NoError(t, err)
	require.Empty(t, out.GetRecords())
	require.Equal(t, int64(5), out.GetNextOffset())
}

// Every owner kind resolves to the execution that holds the stream and the
// key it is held under there. A workflow's activity is routed on the workflow
// and held under a reserved key, which the workflow kind may not name.
func TestResolveOwnedStream(t *testing.T) {
	wf := &streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW, Id: "wf", RunId: "run",
	}
	target, err := resolveOwned("ns", wf, "")
	require.NoError(t, err)
	require.Equal(t, stream.DefaultStreamName, target.key)
	require.Equal(t, "wf", target.ref.BusinessID)
	require.Equal(t, "run", target.ref.RunID)

	saa := &streamlib.StreamOwner{Kind: streamlib.STREAM_OWNER_KIND_ACTIVITY, Id: "act"}
	target, err = resolveOwned("ns", saa, "progress")
	require.NoError(t, err)
	require.Equal(t, "progress", target.key)
	require.Equal(t, "act", target.ref.BusinessID)

	wfa := &streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY, Id: "wf", ActivityId: "act",
	}
	target, err = resolveOwned("ns", wfa, "")
	require.NoError(t, err)
	require.Equal(t, "wf", target.ref.BusinessID, "a workflow's activity routes on the workflow")
	require.Equal(t, chasmworkflow.ActivityStreamKey("act", stream.DefaultStreamName), target.key)

	_, err = resolveOwned("ns", wf, target.key)
	require.ErrorContains(t, err, "reserved", "the workflow kind may not reach an activity's stream")
}

func TestResolveOwnedRefusesAnIncompleteOwner(t *testing.T) {
	for name, owner := range map[string]*streamlib.StreamOwner{
		"no owner": nil,
		"no kind":  {Id: "wf"},
		"no id":    {Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW},
		"activity of a standalone": {
			Kind: streamlib.STREAM_OWNER_KIND_ACTIVITY, Id: "act", ActivityId: "other",
		},
		"workflow activity without the activity": {
			Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY, Id: "wf",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := resolveOwned("ns", owner, "")
			var invalid *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalid)
		})
	}
}
