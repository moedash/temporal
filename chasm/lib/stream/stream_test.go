package stream

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Built directly rather than through NewStream: these exercise state
// transitions, and NewStream also wires a visibility field that needs a live
// context. Construction through the real path is covered end to end in
// tests/stream_test.go.
func newTestStream(t *testing.T) *Stream {
	t.Helper()
	return &Stream{
		State: &streampb.StreamState{
			Producers: make(map[string]*streampb.ProducerCursor),
			Consumers: make(map[string]*streampb.ConsumerCursor),
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
	s := newTestStream(t)

	first, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c")})
	require.NoError(t, err)
	require.Equal(t, int64(0), first.FirstOffset)
	require.Equal(t, int64(3), first.Count)
	require.Equal(t, int64(3), first.NextOffset)

	second, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("d", "e")})
	require.NoError(t, err)
	require.Equal(t, int64(3), second.FirstOffset)
	require.Equal(t, int64(5), second.NextOffset)
	require.Equal(t, int64(5), s.State.HeadOffset)
}

func TestAddMessagesWritesTheBatchIntoTheComponent(t *testing.T) {
	s := newTestStream(t)

	res, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b")})
	require.NoError(t, err)
	require.NotEmpty(t, res.Blob.Data)

	// Keyed by the offset it starts at, which is the key a retry of this
	// append writes under, so the retry replaces rather than races.
	require.Len(t, s.Batches, 1)
	_, ok := s.Batches[0]
	require.True(t, ok, "the batch must be keyed by its first offset")
	require.Equal(t, int64(2), s.State.HeadOffset)
}

func TestDedupReturnsOriginalOffsets(t *testing.T) {
	s := newTestStream(t)
	req := AddMessagesRequest{Messages: msgs("a", "b"), ProducerID: "p1", Sequence: 1}

	first, err := s.AddMessages(nil, req)
	require.NoError(t, err)
	require.False(t, first.Deduplicated)

	retry := req
	again, err := s.AddMessages(nil, retry)
	require.NoError(t, err)
	require.True(t, again.Deduplicated)
	require.Equal(t, first.FirstOffset, again.FirstOffset)
	require.Nil(t, again.Blob, "a deduplicated retry writes nothing")
	require.Equal(t, int64(2), s.State.HeadOffset, "a retry must not advance the head")
}

func TestDedupIgnoresPayloadMetadataMapOrder(t *testing.T) {
	s := newTestStream(t)
	for attempt := range 32 {
		body := &commonpb.Payload{
			Data: []byte("encoded"),
			Metadata: map[string][]byte{
				"encoding": []byte("test/envelope"),
				"key-id":   []byte("key-1"),
				"nonce":    []byte("fixed-for-this-record"),
			},
		}
		result, err := s.AddMessages(nil, AddMessagesRequest{
			Messages: []*streampb.StreamMessage{{
				Kind:     streampb.STREAM_MESSAGE_KIND_DATA,
				Body:     body,
				Metadata: map[string]*commonpb.Payload{"attempt": body, "checkpoint": body},
			}},
			ProducerID: "encoded-producer", Sequence: 1,
		})
		require.NoError(t, err, "identical retry %d must ignore protobuf map iteration order", attempt)
		require.Equal(t, attempt > 0, result.Deduplicated)
		require.Equal(t, int64(0), result.FirstOffset)
		require.Equal(t, int64(1), s.State.HeadOffset)
	}
}

func TestDedupRejectsDifferentContent(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("a"), ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)

	// Returning the recorded offsets here would report success while dropping
	// the caller's data, which is worse than failing.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("different"), ProducerID: "p1", Sequence: 1,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "different content")
}

func TestExpectedOffsetMismatchReportsHead(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a")})
	require.NoError(t, err)

	stale := int64(0)
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("b"), ExpectedOffset: &stale,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream head is 1")
}

func TestFinishWritingFencesOneProducerOnly(t *testing.T) {
	s := newTestStream(t)
	require.NoError(t, s.FinishWriting(nil, "p1"))

	_, err := s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("a"), ProducerID: "p1", Sequence: 1,
	})
	require.Error(t, err)

	// Another producer is unaffected: finishing is per-producer, not a close.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("a"), ProducerID: "p2", Sequence: 1,
	})
	require.NoError(t, err)
	require.False(t, s.State.Closed)
}

func TestCloseRejectsFurtherAppends(t *testing.T) {
	s := newTestStream(t)
	s.Close(time.Now(), nil)

	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a")})
	require.Error(t, err)
	var precondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &precondition)
}

func TestReadSpansBatchesAndStartsAtTheBatchHoldingTheOffset(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c")})
	require.NoError(t, err)
	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("d", "e")})
	require.NoError(t, err)
	require.Len(t, s.Batches, 2)

	// A read from offset 1 lands inside the first batch. It gets that batch
	// whole, because a consumer asks for an offset and not for a batch, and
	// the batch is the smallest thing stored.
	blobs, starts, err := s.ReadBatches(nil, 1, 5, 0)
	require.NoError(t, err)
	require.Len(t, blobs, 2)
	require.Equal(t, []int64{0, 3}, starts)

	// A read wholly inside the second batch does not drag the first along.
	blobs, starts, err = s.ReadBatches(nil, 3, 5, 0)
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	require.Equal(t, []int64{3}, starts)
}

func TestReclaimDropsOnlyBatchesFullyBelowTheFloor(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c")})
	require.NoError(t, err)
	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("d", "e")})
	require.NoError(t, err)

	// The floor lands mid-batch, so that batch stays: offsets above the floor
	// are still readable and they live in it.
	require.NoError(t, s.Truncate(nil, 1))
	require.Len(t, s.Batches, 2)

	// Now the whole first batch is below the floor and can go.
	require.NoError(t, s.Truncate(nil, 3))
	require.Len(t, s.Batches, 1)
	_, ok := s.Batches[3]
	require.True(t, ok, "the batch holding readable offsets must survive")
}

func TestTruncateStopsAtAnActiveConsumersReplayFloor(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.NoError(t, err)

	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "wf-1", WorkflowID: "wf-1", RunID: "run-1", Offset: 0,
	})
	require.NoError(t, err)
	s.AdvanceConsumer(nil, "wf-1", 2)

	// Reading to 2 is exactly what makes offsets 0 and 1 matter: they are in
	// this consumer's History and a replay is asked to reproduce them.
	err = s.Truncate(nil, 3)
	require.ErrorContains(t, err, "still depends on offset 0")
	require.Equal(t, int64(0), s.State.BaseOffset)

	// Whatever is above the floor is still spare capacity.
	require.NoError(t, s.Truncate(nil, 0))
}

func TestTruncateBounds(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b")})
	require.NoError(t, err)

	err = s.Truncate(nil, 1)
	require.NoError(t, err)
	err = s.Truncate(nil, 0)
	require.Error(t, err, "truncation must not go backwards")
	err = s.Truncate(nil, 3)
	require.Error(t, err, "truncation must not pass the head")
}

func TestCapTruncatesInline(t *testing.T) {
	s := newTestStream(t)
	s.State.Lifecycle = &streampb.StreamLifecycle{MaxItems: 4}

	for range 4 {
		_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b")})
		require.NoError(t, err)
	}

	// Eight appended, four retained, so the floor sits at 4 and the first two
	// batches are entirely below it.
	require.Equal(t, int64(8), s.State.HeadOffset)
	require.Equal(t, int64(4), s.State.BaseOffset)
}

func TestCapRefusesAnAppendItCouldOnlyAbsorbByDroppingReadRecords(t *testing.T) {
	s := newTestStream(t)
	s.State.Lifecycle = &streampb.StreamLifecycle{MaxItems: 2}
	_, err := s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "wf-1", WorkflowID: "wf-1", RunID: "run-1", Offset: 0,
	})
	require.NoError(t, err)

	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.ErrorContains(t, err, "still depends on offset 0")
	require.Equal(t, int64(0), s.State.HeadOffset, "a refused append writes nothing")

	// Nothing is stuck. The consumer going away is what makes room, and it is
	// something someone does rather than something that happens quietly.
	s.DeregisterConsumer(nil, "wf-1")
	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.NoError(t, err)
	require.Equal(t, int64(2), s.State.BaseOffset, "the cap applies once nobody needs the bytes")
}

func TestCloseSchedulesRetentionOnlyWhenConfigured(t *testing.T) {
	now := time.Now()

	plain := newTestStream(t)
	require.True(t, plain.Close(now, nil).IsZero(), "no retention configured, nothing to schedule")

	withRetention := newTestStream(t)
	withRetention.State.Lifecycle = &streampb.StreamLifecycle{
		Retention: durationpb.New(time.Hour),
	}
	at := withRetention.Close(now, nil)
	require.Equal(t, now.Add(time.Hour), at)
	require.NotNil(t, withRetention.State.CloseTime)

	// Closing twice must not re-arm deletion.
	require.True(t, withRetention.Close(now, nil).IsZero())
}

func TestRegisterConsumerPinsFromWhereItSubscribed(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.NoError(t, err)

	// Subscribing at 2 says nothing about offsets 0 and 1, so those stay
	// droppable and everything from 2 up does not.
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 2,
	})
	require.NoError(t, err)

	require.NoError(t, s.Truncate(nil, 2))
	require.Equal(t, int64(2), s.State.BaseOffset)
	require.ErrorContains(t, s.Truncate(nil, 3), "still depends on offset 2")
}

func TestAdvanceConsumerTracksWhereAConsumerHasReached(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.NoError(t, err)
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 0,
	})
	require.NoError(t, err)

	// The read position decides whether this consumer is worth waking. The
	// replay floor, not this, is what retention respects.
	s.AdvanceConsumer(nil, "workflow:output", 3)
	require.Equal(t, int64(3), s.State.Consumers["workflow:output"].Offset)
}

// Lowering the pin would hand back a guarantee already written to History: a
// recorded range has to stay re-readable.
func TestAdvanceConsumerNeverRewinds(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.NoError(t, err)
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 0,
	})
	require.NoError(t, err)

	s.AdvanceConsumer(nil, "workflow:output", 3)
	s.AdvanceConsumer(nil, "workflow:output", 1)

	require.Equal(t, int64(3), s.State.Consumers["workflow:output"].GetOffset())
}

func TestRegisterConsumerRejectsAnOffsetBelowTheFloor(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.NoError(t, err)
	err = s.Truncate(nil, 2)
	require.NoError(t, err)

	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 1,
	})
	require.ErrorContains(t, err, "below the stream's floor")
}

// Resubscribing reactivates the existing pin rather than resetting it, so a
// consumer cannot rewind its own floor by subscribing again.
func TestRegisterConsumerTwiceKeepsThePin(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.NoError(t, err)
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 0,
	})
	require.NoError(t, err)
	s.AdvanceConsumer(nil, "workflow:output", 3)

	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 0,
	})
	require.NoError(t, err)

	require.Equal(t, int64(3), s.State.Consumers["workflow:output"].GetOffset())
}

func TestDeregisterConsumerReleasesThePin(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.NoError(t, err)
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 1,
	})
	require.NoError(t, err)

	s.DeregisterConsumer(nil, "workflow:output")

	err = s.Truncate(nil, 4)
	require.NoError(t, err)
}

func TestMessageCapStillAppliesWithNoConsumerToProtect(t *testing.T) {
	s := newTestStream(t)
	s.State.Lifecycle = &streampb.StreamLifecycle{MaxItems: 2}

	for range 3 {
		_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b")})
		require.NoError(t, err)
	}

	// The refusal is about a consumer's recovery, so a stream with none behaves
	// exactly as a capped log should.
	require.Equal(t, int64(6), s.State.HeadOffset)
	require.Equal(t, int64(4), s.State.BaseOffset)
}

// A consumer that arrives after the messages were written cannot make the cap
// retroactively wrong, so the clamp keeps its bytes and the stream sits over
// its cap until it goes away.
func TestCapClampsToAConsumerThatRegisteredLate(t *testing.T) {
	s := newTestStream(t)
	s.State.Lifecycle = &streampb.StreamLifecycle{MaxItems: 2}

	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b")})
	require.NoError(t, err)
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 0,
	})
	require.NoError(t, err)
	s.State.Lifecycle = &streampb.StreamLifecycle{MaxItems: 1}
	s.applyCap()

	require.Equal(t, int64(0), s.State.BaseOffset, "the clamp keeps what the consumer needs")
}

// The reason the pin was taken out in the first place. It must not come back:
// a consumer that finished has to stop holding storage.
func TestAConsumerThatDeregisteredHoldsNothing(t *testing.T) {
	s := newTestStream(t)
	s.State.Lifecycle = &streampb.StreamLifecycle{MaxItems: 2}
	_, err := s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 0,
	})
	require.NoError(t, err)
	s.DeregisterConsumer(nil, "workflow:output")

	for range 3 {
		_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b")})
		require.NoError(t, err)
	}
	require.Equal(t, int64(4), s.State.BaseOffset)
}

// Coming back to a stream that moved past what its History refers to has to be
// refused at registration, which is the last moment before the workflow
// depends on those offsets again.
func TestReregisteringBelowTheFloorIsRefused(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.NoError(t, err)
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 0,
	})
	require.NoError(t, err)
	s.DeregisterConsumer(nil, "workflow:output")
	require.NoError(t, s.Truncate(nil, 2))

	// Resubscribing further along does not repair the gap. What this consumer
	// already recorded starts at 0, and offsets 0 and 1 are gone.
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:output", WorkflowID: "wf-1", RunID: "run-1", Offset: 3,
	})
	require.ErrorContains(t, err, "the stream now starts at 2")
}

// A caller sending a fresh producer id per request would otherwise grow the
// component state until no append fits, which leaves the stream unwritable for
// good rather than failing the call that caused it.
func TestStreamProducerTableIsBounded(t *testing.T) {
	s := newTestStream(t)

	for i := range MaxProducersPerStream {
		_, err := s.AddMessages(nil, AddMessagesRequest{
			Messages:   msgs("m"),
			ProducerID: fmt.Sprintf("p%d", i),
			Sequence:   1,
		})
		require.NoError(t, err)
	}

	_, err := s.AddMessages(nil, AddMessagesRequest{
		Messages:   msgs("one too many"),
		ProducerID: "p-over",
		Sequence:   1,
	})
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid)

	// A producer already tracked keeps working, so the cap cannot wedge the
	// producers that filled it.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Messages:   msgs("still fine"),
		ProducerID: "p0",
		Sequence:   2,
	})
	require.NoError(t, err)

	// An anonymous append is never blocked by the table.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Messages: msgs("anon"),
	})
	require.NoError(t, err)
}

// Each consumer holds a truncation floor, so an unbounded table pins storage as
// well as growing state.
func TestStreamConsumerTableIsBounded(t *testing.T) {
	s := newTestStream(t)

	for i := range MaxConsumersPerStream {
		_, err := s.RegisterConsumer(nil, ConsumerRegistration{
			ConsumerID: fmt.Sprintf("c%d", i), WorkflowID: "wf", RunID: "run", External: true,
		})
		require.NoError(t, err)
	}

	_, err := s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "c-over", WorkflowID: "wf", RunID: "run", Offset: 0, External: true,
	})
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid)

	// Re-registering an existing consumer is an update, not a new entry.
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "c0", WorkflowID: "wf", RunID: "run", Offset: 0, External: true,
	})
	require.NoError(t, err)
}

// A producer that leaves the kind unset means data. Delivery to a workflow
// drops anything that is not data, so without this the message would take an
// offset and never be seen by a subscriber.
func TestAddMessagesTreatsAnUnsetKindAsData(t *testing.T) {
	s := newTestStream(t)
	unset := []*streampb.StreamMessage{
		{Body: &commonpb.Payload{Data: []byte("a")}},
		{Body: &commonpb.Payload{Data: []byte("b")}},
	}
	_, err := s.AddMessages(nil, AddMessagesRequest{
		Messages: unset, ProducerID: "p", Sequence: 1,
	})
	require.NoError(t, err)

	blobs, starts, err := s.ReadBatches(nil, 0, 2, 0)
	require.NoError(t, err)
	collected, _, err := CollectMessages(blobs, starts, 0, 2, 10, nil)
	require.NoError(t, err)
	require.Len(t, ToAPIMessages(collected), 2, "both messages must reach a subscriber")

	// The retry carries the same unset kind and has to hash the same.
	retry, err := s.AddMessages(nil, AddMessagesRequest{
		Messages: []*streampb.StreamMessage{
			{Body: &commonpb.Payload{Data: []byte("a")}},
			{Body: &commonpb.Payload{Data: []byte("b")}},
		},
		ProducerID: "p", Sequence: 1,
	})
	require.NoError(t, err)
	require.True(t, retry.Deduplicated)
}

// A budgeted stream refuses the append that would not fit rather than dropping
// older messages: its batches are someone's mutable state, and the alternative
// to refusing is the execution size limit terminating that workflow later.
func TestBudgetRefusesAnAppendThatDoesNotFit(t *testing.T) {
	s := newTestStream(t)
	s.State.Budget = &streampb.StreamBudget{MaxItems: 3}

	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b")})
	require.NoError(t, err)

	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("c", "d")})
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, int64(2), s.State.HeadOffset, "a refused append writes nothing")

	// The last slot is still there for an append that fits.
	_, err = s.AddMessages(nil, AddMessagesRequest{Messages: msgs("c")})
	require.NoError(t, err)

	bytesOnly := newTestStream(t)
	bytesOnly.State.Budget = &streampb.StreamBudget{MaxBytes: 16}
	_, err = bytesOnly.AddMessages(nil, AddMessagesRequest{Messages: msgs("small")})
	require.NoError(t, err)
	_, err = bytesOnly.AddMessages(nil, AddMessagesRequest{Messages: msgs("another one")})
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, int64(1), bytesOnly.State.HeadOffset)
}

// One workflow id has one open run, so a pin from another run of the same
// workflow belongs to a run that finished. Registering the new run drops it,
// which is what lets the new run start at its own offset instead of inheriting
// a floor that may already be below the stream's base.
func TestRegisterConsumerReplacesAnEntryFromAnotherRun(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{Messages: msgs("a", "b", "c", "d")})
	require.NoError(t, err)
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:wf/run-1", WorkflowID: "wf", RunID: "run-1", Offset: 0, External: true,
	})
	require.NoError(t, err)

	start, err := s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:wf/run-2", WorkflowID: "wf", RunID: "run-2", Offset: 3, External: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), start)
	require.Len(t, s.State.Consumers, 1)
	require.Equal(t, "run-2", s.State.Consumers["workflow:wf/run-2"].GetRunId())
	require.NoError(t, s.Truncate(nil, 3), "only the new run's floor holds")

	// A different workflow id is not the same consumer and keeps its pin.
	_, err = s.RegisterConsumer(nil, ConsumerRegistration{
		ConsumerID: "workflow:other/run-9", WorkflowID: "other", RunID: "run-9",
		Offset: 3, External: true,
	})
	require.NoError(t, err)
	require.Len(t, s.State.Consumers, 2)
}

// Every append that leaves an external consumer behind would otherwise queue a
// task of its own. One outstanding task carries every append that lands before
// it runs, and the task's own read lowers the flag before the next one is owed.
func TestNotifyCoalescesIntoOneOutstandingTask(t *testing.T) {
	mctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
	}}
	s := newTestStream(t)
	s.Batches = make(chasm.Map[int64, *commonpb.DataBlob])
	_, err := s.RegisterConsumer(mctx, ConsumerRegistration{
		ConsumerID: "workflow:wf/run-1", WorkflowID: "wf", RunID: "run-1", Offset: 0, External: true,
	})
	require.NoError(t, err)

	_, err = s.AddMessages(mctx, AddMessagesRequest{Messages: msgs("a")})
	require.NoError(t, err)
	_, err = s.AddMessages(mctx, AddMessagesRequest{Messages: msgs("b")})
	require.NoError(t, err)
	require.Len(t, mctx.Tasks, 1, "the second append rides the task the first scheduled")

	// The task's read lowers the flag, so the next append owes a new task.
	state, err := s.TakeNotifySnapshot(mctx, struct{}{})
	require.NoError(t, err)
	require.Equal(t, int64(2), state.GetHeadOffset(),
		"the task sees every append that landed before it")
	_, err = s.AddMessages(mctx, AddMessagesRequest{Messages: msgs("c")})
	require.NoError(t, err)
	require.Len(t, mctx.Tasks, 2)
}
