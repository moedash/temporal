package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

func newTestStream(t *testing.T, bucketSize int64) *Stream {
	t.Helper()
	s, err := NewStream(nil, NewStreamRequest{
		CollectionID: "col-1",
		BucketSize:   bucketSize,
	})
	require.NoError(t, err)
	return s
}

func msgs(bodies ...string) []*streampb.StreamMessage {
	out := make([]*streampb.StreamMessage, len(bodies))
	for i, b := range bodies {
		out[i] = &streampb.StreamMessage{
			Body: &commonpb.Payload{Data: []byte(b)},
			Kind: streampb.STREAM_MESSAGE_KIND_DATA,
		}
	}
	return out
}

func TestAddMessagesAssignsContiguousOffsets(t *testing.T) {
	s := newTestStream(t, 100)

	first, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c"), TxnID: 1})
	require.NoError(t, err)
	require.Equal(t, int64(0), first.FirstOffset)
	require.Equal(t, int64(3), first.Count)
	require.Equal(t, int64(3), first.NextOffset)

	second, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("d", "e"), TxnID: 2})
	require.NoError(t, err)
	require.Equal(t, int64(3), second.FirstOffset)
	require.Equal(t, int64(5), second.NextOffset)
	require.Equal(t, int64(5), s.State.HeadOffset)
}

func TestAddMessagesStagesRatherThanPersists(t *testing.T) {
	s := newTestStream(t, 100)

	res, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b"), TxnID: 7})
	require.NoError(t, err)
	require.Len(t, res.Appends, 1)

	// The node is bucket-relative and starts at 1, and it chains to the
	// previous transaction so a stale node is rejected on read.
	require.Equal(t, int64(0), res.Appends[0].Bucket)
	require.Equal(t, int64(1), res.Appends[0].NodeID)
	require.Equal(t, int64(7), res.Appends[0].TxnID)
	require.Equal(t, int64(0), res.Appends[0].PrevTxnID)
	require.True(t, res.Appends[0].IsNewBucket)
	require.NotEmpty(t, res.Appends[0].Blob.Data)
}

func TestTransactionIDMustAdvance(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a"), TxnID: 5})
	require.NoError(t, err)

	// Reusing an ID would leave two rows at one node with no way to tell which
	// one won, so it is rejected rather than silently accepted.
	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("b"), TxnID: 5})
	require.Error(t, err)
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid)
}

func TestDedupReturnsOriginalOffsets(t *testing.T) {
	s := newTestStream(t, 100)
	req := AddMessagesRequest{Messages: msgs("a", "b"), ProducerID: "p1", Sequence: 1, TxnID: 1}

	first, err := s.AddMessages(nil, req)
	require.NoError(t, err)
	require.False(t, first.Deduplicated)

	retry := req
	retry.TxnID = 2
	again, err := s.AddMessages(nil, retry)
	require.NoError(t, err)
	require.True(t, again.Deduplicated)
	require.Equal(t, first.FirstOffset, again.FirstOffset)
	require.Empty(t, again.Appends)
	require.Equal(t, int64(2), s.State.HeadOffset, "a retry must not advance the head")
}

func TestDedupRejectsDifferentContent(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("a"), ProducerID: "p1", Sequence: 1, TxnID: 1,
	})
	require.NoError(t, err)

	// Returning the recorded offsets here would report success while dropping
	// the caller's data, which is worse than failing.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("different"), ProducerID: "p1", Sequence: 1, TxnID: 2,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "different content")
}

func TestExpectedOffsetMismatchReportsHead(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a"), TxnID: 1})
	require.NoError(t, err)

	stale := int64(0)
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("b"), ExpectedOffset: &stale, TxnID: 2,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream head is 1")
}

func TestOwnerEpochFencesStaleProducer(t *testing.T) {
	s := newTestStream(t, 100)
	s.State.OwnerEpoch = 5

	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a"), OwnerEpoch: 4, TxnID: 1})
	require.Error(t, err)

	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a"), OwnerEpoch: 5, TxnID: 1})
	require.NoError(t, err)
}

func TestFinishWritingFencesOneProducerOnly(t *testing.T) {
	s := newTestStream(t, 100)
	require.NoError(t, s.FinishWriting(nil, "p1"))

	_, err := s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("a"), ProducerID: "p1", Sequence: 1, TxnID: 1,
	})
	require.Error(t, err)

	// Another producer is unaffected: finishing is per-producer, not a close.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("a"), ProducerID: "p2", Sequence: 1, TxnID: 2,
	})
	require.NoError(t, err)
	require.False(t, s.State.Closed)
}

func TestCloseRejectsFurtherAppends(t *testing.T) {
	s := newTestStream(t, 100)
	require.NoError(t, s.Close(nil, nil))

	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a"), TxnID: 1})
	require.Error(t, err)
	var precondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &precondition)
}

func TestBatchMayNotCrossABucket(t *testing.T) {
	s := newTestStream(t, 4)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c"), TxnID: 1})
	require.NoError(t, err)

	// Offsets 3 and 4 fall in different buckets, and a bucket is a storage
	// partition, so a node spanning both is not representable.
	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("d", "e"), TxnID: 2})
	require.Error(t, err)
	require.Contains(t, err.Error(), "crosses a bucket boundary")
}

func TestAppendsRollToNewBucket(t *testing.T) {
	s := newTestStream(t, 4)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d"), TxnID: 1})
	require.NoError(t, err)

	res, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("e"), TxnID: 2})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Appends[0].Bucket)
	require.Equal(t, int64(1), res.Appends[0].NodeID, "node ids restart per bucket")
	require.True(t, res.Appends[0].IsNewBucket)
}

func TestTruncateRespectsConsumerPin(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d"), TxnID: 1})
	require.NoError(t, err)

	s.State.Consumers["wf-1"] = &streampb.ConsumerCursor{
		WorkflowId: "wf-1", Offset: 2, Active: true,
	}

	// A workflow consumer's history records an offset range it must be able to
	// re-read on replay, so truncation cannot pass it.
	require.Error(t, s.Truncate(nil, 3))
	require.NoError(t, s.Truncate(nil, 2))
	require.Equal(t, int64(2), s.State.BaseOffset)

	s.State.Consumers["wf-1"].Active = false
	require.NoError(t, s.Truncate(nil, 4))
}

func TestTruncateBounds(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b"), TxnID: 1})
	require.NoError(t, err)

	require.NoError(t, s.Truncate(nil, 1))
	require.Error(t, s.Truncate(nil, 0), "truncation must not go backwards")
	require.Error(t, s.Truncate(nil, 3), "truncation must not pass the head")
}

func TestBucketArithmetic(t *testing.T) {
	require.Equal(t, int64(0), BucketOf(0, 10))
	require.Equal(t, int64(0), BucketOf(9, 10))
	require.Equal(t, int64(1), BucketOf(10, 10))

	// Node ids are bucket-relative and start at 1, because the store rejects 0.
	require.Equal(t, int64(1), NodeIDOf(0, 10))
	require.Equal(t, int64(10), NodeIDOf(9, 10))
	require.Equal(t, int64(1), NodeIDOf(10, 10))
	require.Equal(t, int64(20), BucketStart(2, 10))
}
