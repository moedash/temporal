package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

func bodiesOf(records []*streamlib.StreamRecord) []string {
	out := make([]string, len(records))
	for i, r := range records {
		out[i] = string(r.GetBody().GetData())
	}
	return out
}

// A copy that begins inside a batch trims it and keys what it kept at the
// copy's start, so the seeded stream begins exactly at the inherited offset.
func TestSeedTrimsTheBatchStraddlingTheStart(t *testing.T) {
	source := newTestStream(t)
	for _, batch := range [][]string{{"a", "b", "c"}, {"d", "e"}} {
		_, err := source.AddMessages(nil, AddMessagesRequest{Records: msgs(batch...)})
		require.NoError(t, err)
	}
	blobs, starts, err := source.ReadBatches(nil, 1, 4, 0)
	require.NoError(t, err)

	target := &Stream{State: &streamlib.StreamState{BaseOffset: 1, HeadOffset: 1}}
	require.NoError(t, target.Seed(nil, blobs, starts, 1, 4))
	require.Equal(t, int64(4), target.State.HeadOffset)
	require.Equal(t, []int64{1, 3}, target.batchStarts())
	require.Positive(t, target.State.AppendedBytes, "the copy counts against the budget")
	require.Empty(t, target.State.Producers)

	copied, copiedStarts, err := target.ReadBatches(nil, 1, 4, 0)
	require.NoError(t, err)
	records, next, err := CollectRecords(copied, copiedStarts, 1, 4, 10, nil)
	require.NoError(t, err)
	require.Equal(t, int64(4), next)
	require.Equal(t, []string{"b", "c", "d"}, bodiesOf(records))
	require.Equal(t, []int64{1, 2, 3}, []int64{
		records[0].GetOffset(), records[1].GetOffset(), records[2].GetOffset()})
}

func TestSeedRefusesAStreamThatIsNotEmptyAtTheStart(t *testing.T) {
	source := newTestStream(t)
	_, err := source.AddMessages(nil, AddMessagesRequest{Records: msgs("a", "b")})
	require.NoError(t, err)
	blobs, starts, err := source.ReadBatches(nil, 0, 2, 0)
	require.NoError(t, err)

	var internal *serviceerror.Internal
	occupied := newTestStream(t)
	_, err = occupied.AddMessages(nil, AddMessagesRequest{Records: msgs("x")})
	require.NoError(t, err)
	require.ErrorAs(t, occupied.Seed(nil, blobs, starts, 0, 2), &internal)

	elsewhere := &Stream{State: &streamlib.StreamState{BaseOffset: 5, HeadOffset: 5}}
	require.ErrorAs(t, elsewhere.Seed(nil, blobs, starts, 0, 2), &internal)

	// A window that does not reach the end it was asked for is refused rather
	// than left as a stream shorter than its history claims.
	short := newTestStream(t)
	require.ErrorAs(t, short.Seed(nil, blobs, starts, 0, 3), &internal)
}
