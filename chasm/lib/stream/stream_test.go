package stream

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Built directly rather than through NewStream: these exercise state
// transitions, and NewStream also wires a visibility field that needs a live
// context. Construction through the real path is covered end to end in
// tests/stream_test.go.
func newTestStream(t *testing.T, bucketSize int64) *Stream {
	t.Helper()
	return &Stream{
		State: &streampb.StreamState{
			CollectionId: "col-1",
			BucketSize:   bucketSize,
			Producers:    make(map[string]*streampb.ProducerCursor),
			Consumers:    make(map[string]*streampb.ConsumerCursor),
		},
	}
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
	s.Close(time.Now(), nil)

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
	_, err = s.Truncate(nil, 3)
	require.Error(t, err)
	_, err = s.Truncate(nil, 2)
	require.NoError(t, err)
	require.Equal(t, int64(2), s.State.BaseOffset)

	s.State.Consumers["wf-1"].Active = false
	_, err = s.Truncate(nil, 4)
	require.NoError(t, err)
}

func TestTruncateBounds(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b"), TxnID: 1})
	require.NoError(t, err)

	_, err = s.Truncate(nil, 1)
	require.NoError(t, err)
	_, err = s.Truncate(nil, 0)
	require.Error(t, err, "truncation must not go backwards")
	_, err = s.Truncate(nil, 3)
	require.Error(t, err, "truncation must not pass the head")
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

func TestReclaimableBuckets(t *testing.T) {
	// Only buckets lying entirely below the floor are reclaimable, so nothing a
	// reader can still ask for is ever dropped.
	require.Empty(t, ReclaimableBuckets(0, 3, 4))
	require.Equal(t, []int64{0}, ReclaimableBuckets(0, 4, 4))
	require.Equal(t, []int64{0, 1}, ReclaimableBuckets(0, 8, 4))
	require.Equal(t, []int64{1}, ReclaimableBuckets(4, 8, 4))
	require.Empty(t, ReclaimableBuckets(4, 5, 4))
}

func TestCapTruncatesInline(t *testing.T) {
	s := newTestStream(t, 4)
	s.State.Lifecycle = &streampb.StreamLifecycle{MaxItems: 4}

	for i := range 4 {
		_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b"), TxnID: int64(i + 1)})
		require.NoError(t, err)
	}

	// Eight appended, four retained, so the floor sits at 4 and bucket 0 is
	// entirely below it.
	require.Equal(t, int64(8), s.State.HeadOffset)
	require.Equal(t, int64(4), s.State.BaseOffset)
}

func TestCapYieldsToAConsumerPin(t *testing.T) {
	s := newTestStream(t, 100)
	s.State.Lifecycle = &streampb.StreamLifecycle{MaxItems: 2}
	s.State.Consumers["wf-1"] = &streampb.ConsumerCursor{
		WorkflowId: "wf-1", Offset: 1, Active: true,
	}

	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d"), TxnID: 1})
	require.NoError(t, err)

	// The cap wants a floor of 2, but a workflow consumer recorded a cursor at 1
	// and must be able to re-read from there on replay. Storage grows rather
	// than that consumer losing data.
	require.Equal(t, int64(1), s.State.BaseOffset)
}

func TestCloseSchedulesRetentionOnlyWhenConfigured(t *testing.T) {
	now := time.Now()

	plain := newTestStream(t, 100)
	require.True(t, plain.Close(now, nil).IsZero(), "no retention configured, nothing to schedule")

	withRetention := newTestStream(t, 100)
	withRetention.State.Lifecycle = &streampb.StreamLifecycle{
		Retention: durationpb.New(time.Hour),
	}
	at := withRetention.Close(now, nil)
	require.Equal(t, now.Add(time.Hour), at)
	require.NotNil(t, withRetention.State.CloseTime)

	// Closing twice must not re-arm deletion.
	require.True(t, withRetention.Close(now, nil).IsZero())
}

// The pin test above sets State.Consumers by hand, which is why nothing caught
// that no caller ever populated it. These go through the registration API.
func TestRegisterConsumerPinsTruncation(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d"), TxnID: 1})
	require.NoError(t, err)

	require.NoError(t, s.RegisterConsumer(nil, "workflow:output", "wf-1", "run-1", 2, false))

	_, err = s.Truncate(nil, 3)
	require.ErrorContains(t, err, "an active consumer still needs")

	_, err = s.Truncate(nil, 2)
	require.NoError(t, err)
	require.Equal(t, int64(2), s.State.BaseOffset)
}

func TestAdvanceConsumerReleasesTruncation(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d"), TxnID: 1})
	require.NoError(t, err)
	require.NoError(t, s.RegisterConsumer(nil, "workflow:output", "wf-1", "run-1", 0, false))

	_, err = s.Truncate(nil, 1)
	require.Error(t, err, "the pin still sits at 0")

	s.AdvanceConsumer(nil, "workflow:output", 3)
	_, err = s.Truncate(nil, 3)
	require.NoError(t, err)
	require.Equal(t, int64(3), s.State.BaseOffset)
}

// Lowering the pin would hand back a guarantee already written to History: a
// recorded range has to stay re-readable.
func TestAdvanceConsumerNeverRewinds(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d"), TxnID: 1})
	require.NoError(t, err)
	require.NoError(t, s.RegisterConsumer(nil, "workflow:output", "wf-1", "run-1", 0, false))

	s.AdvanceConsumer(nil, "workflow:output", 3)
	s.AdvanceConsumer(nil, "workflow:output", 1)

	pin, ok := s.consumerPin()
	require.True(t, ok)
	require.Equal(t, int64(3), pin)
}

func TestRegisterConsumerRejectsAnOffsetBelowTheFloor(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d"), TxnID: 1})
	require.NoError(t, err)
	_, err = s.Truncate(nil, 2)
	require.NoError(t, err)

	err = s.RegisterConsumer(nil, "workflow:output", "wf-1", "run-1", 1, false)
	require.ErrorContains(t, err, "below the stream's floor")
}

// Resubscribing reactivates the existing pin rather than resetting it, so a
// consumer cannot rewind its own floor by subscribing again.
func TestRegisterConsumerTwiceKeepsThePin(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d"), TxnID: 1})
	require.NoError(t, err)
	require.NoError(t, s.RegisterConsumer(nil, "workflow:output", "wf-1", "run-1", 0, false))
	s.AdvanceConsumer(nil, "workflow:output", 3)

	require.NoError(t, s.RegisterConsumer(nil, "workflow:output", "wf-1", "run-1", 0, false))

	pin, ok := s.consumerPin()
	require.True(t, ok)
	require.Equal(t, int64(3), pin)
}

func TestDeregisterConsumerReleasesThePin(t *testing.T) {
	s := newTestStream(t, 100)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d"), TxnID: 1})
	require.NoError(t, err)
	require.NoError(t, s.RegisterConsumer(nil, "workflow:output", "wf-1", "run-1", 1, false))

	s.DeregisterConsumer(nil, "workflow:output")

	_, err = s.Truncate(nil, 4)
	require.NoError(t, err)
}

// The cap is a storage bound, not a licence to drop a range a consumer has
// recorded a cursor for, so it stops at the pin and storage grows instead.
func TestMessageCapYieldsToARegisteredConsumer(t *testing.T) {
	s := newTestStream(t, 100)
	s.State.Lifecycle = &streampb.StreamLifecycle{MaxItems: 2}

	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b"), TxnID: 1})
	require.NoError(t, err)
	require.NoError(t, s.RegisterConsumer(nil, "workflow:output", "wf-1", "run-1", 0, false))

	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("c", "d"), TxnID: 2})
	require.NoError(t, err)
	require.Equal(t, int64(0), s.State.BaseOffset, "the cap must not pass the consumer's pin")

	s.AdvanceConsumer(nil, "workflow:output", 4)
	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("e"), TxnID: 3})
	require.NoError(t, err)
	require.Equal(t, int64(3), s.State.BaseOffset, "once the pin moves the cap applies again")
}

// A caller sending a fresh producer id per request would otherwise grow the
// component state until no append fits, which leaves the stream unwritable for
// good rather than failing the call that caused it.
func TestStreamProducerTableIsBounded(t *testing.T) {
	s := newTestStream(t, 100000)

	for i := range MaxProducersPerStream {
		_, err := s.AddMessages(nil, AddMessagesRequest{
			Messages:   msgs("m"),
			ProducerID: fmt.Sprintf("p%d", i),
			Sequence:   1,
			TxnID:      int64(i + 1),
		})
		require.NoError(t, err)
	}

	_, err := s.AddMessages(nil, AddMessagesRequest{
		Messages:   msgs("one too many"),
		ProducerID: "p-over",
		Sequence:   1,
		TxnID:      int64(MaxProducersPerStream + 1),
	})
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid)

	// A producer already tracked keeps working, so the cap cannot wedge the
	// producers that filled it.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Messages:   msgs("still fine"),
		ProducerID: "p0",
		Sequence:   2,
		TxnID:      int64(MaxProducersPerStream + 2),
	})
	require.NoError(t, err)

	// An anonymous append is never blocked by the table.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("anon"),
		TxnID:    int64(MaxProducersPerStream + 3),
	})
	require.NoError(t, err)
}

// Each consumer holds a truncation floor, so an unbounded table pins storage as
// well as growing state.
func TestStreamConsumerTableIsBounded(t *testing.T) {
	s := newTestStream(t, 100000)

	for i := range MaxConsumersPerStream {
		require.NoError(t, s.RegisterConsumer(
			nil, fmt.Sprintf("c%d", i), "wf", "run", 0, true))
	}

	err := s.RegisterConsumer(nil, "c-over", "wf", "run", 0, true)
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid)

	// Re-registering an existing consumer is an update, not a new entry.
	require.NoError(t, s.RegisterConsumer(nil, "c0", "wf", "run", 0, true))
}
