package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	chasmstream "go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// End-to-end coverage of the native stream path: append through the frontend,
// read back by offset, and the lifecycle transitions around it. This is the
// path the benchmark will eventually compare against the Signal-and-Update
// baseline in streaming_baseline_test.go.

const streamMaxBatch = chasmstream.MaxMessagesPerBatch

type streamTestEnv struct {
	env     *testcore.TestEnv
	client  streampb.StreamServiceClient
	ns      string
	cleanup []func()
}

func newStreamTestEnv(t *testing.T) *streamTestEnv {
	return newStreamTestEnvFrom(t, testcore.NewEnv(t))
}

// newStreamTestEnvFrom lets a test that needs its own env, for example one
// driving the raw task poller, still reach the stream API.
func newStreamTestEnvFrom(t *testing.T, env *testcore.TestEnv) *streamTestEnv {

	conn, err := grpc.NewClient(env.FrontendGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	env2 := &streamTestEnv{env: env, client: streampb.NewStreamServiceClient(conn), ns: env.Namespace().String()}
	t.Cleanup(func() {
		for _, c := range env2.cleanup {
			c()
		}
	})

	return env2
}

func (s *streamTestEnv) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	s.cleanup = append(s.cleanup, cancel)
	return ctx
}

func (s *streamTestEnv) create(ctx context.Context, t *testing.T, streamID string) {
	t.Helper()
	_, err := s.client.CreateStream(ctx, &streampb.CreateStreamRequest{
		FrontendRequest: &streampb.CreateStreamInput{Namespace: s.ns, StreamId: streamID},
	})
	require.NoError(t, err)
}

func (s *streamTestEnv) add(
	ctx context.Context, t *testing.T, streamID string, in *streampb.AddMessagesInput,
) (*streampb.AddMessagesOutput, error) {
	t.Helper()
	in.Namespace = s.ns
	in.StreamId = streamID
	resp, err := s.client.AddMessages(ctx, &streampb.AddMessagesRequest{FrontendRequest: in})
	if err != nil {
		return nil, err
	}
	return resp.GetFrontendResponse(), nil
}

func (s *streamTestEnv) poll(
	ctx context.Context, t *testing.T, streamID string, from int64, topics ...string,
) *streampb.PollMessagesOutput {
	t.Helper()
	resp, err := s.client.PollMessages(ctx, &streampb.PollMessagesRequest{
		FrontendRequest: &streampb.PollMessagesInput{
			Namespace: s.ns, StreamId: streamID, FromOffset: from, Topics: topics,
		},
	})
	require.NoError(t, err)
	return resp.GetFrontendResponse()
}

func streamMsgs(topic string, bodies ...string) []*streampb.StreamMessage {
	out := make([]*streampb.StreamMessage, len(bodies))
	for i, b := range bodies {
		out[i] = &streampb.StreamMessage{
			Body:  &commonpb.Payload{Data: []byte(b)},
			Topic: topic,
			Kind:  streampb.STREAM_MESSAGE_KIND_DATA,
		}
	}
	return out
}

func bodies(msgs []*streampb.StreamMessage) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.GetBody().GetData())
	}
	return out
}

func streamCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestStreamAppendAndRead(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-append-read"
	s.create(ctx, t, id)

	first, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "a", "b", "c")})
	require.NoError(t, err)
	require.Equal(t, int64(0), first.GetFirstOffset())
	require.Equal(t, int64(3), first.GetNextOffset())

	second, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "d")})
	require.NoError(t, err)
	require.Equal(t, int64(3), second.GetFirstOffset())

	all := s.poll(ctx, t, id, 0)
	require.Equal(t, []string{"a", "b", "c", "d"}, bodies(all.GetMessages()))
	require.Equal(t, int64(4), all.GetNextOffset())
	require.Equal(t, int64(4), all.GetHeadOffset())
	require.False(t, all.GetClosed())

	// A reader owns its cursor, so resuming mid-stream is just another read.
	tail := s.poll(ctx, t, id, 2)
	require.Equal(t, []string{"c", "d"}, bodies(tail.GetMessages()))

	// Caught up returns empty rather than erroring.
	caughtUp := s.poll(ctx, t, id, 4)
	require.Empty(t, caughtUp.GetMessages())
	require.Equal(t, int64(4), caughtUp.GetNextOffset())
}

func TestStreamManyReadersAreIndependent(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-many-readers"
	s.create(ctx, t, id)

	_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "a", "b")})
	require.NoError(t, err)

	// No durable per-subscriber state, so reader count is not a state-machine
	// concern and there is no equivalent of the 10-subscriber ceiling the
	// Signal-and-Update pattern hits.
	for range 25 {
		got := s.poll(ctx, t, id, 0)
		require.Equal(t, []string{"a", "b"}, bodies(got.GetMessages()))
	}
}

func TestStreamProducerDedup(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-dedup"
	s.create(ctx, t, id)

	in := &streampb.AddMessagesInput{
		Messages: streamMsgs("", "a", "b"), ProducerId: "p1", Sequence: 1,
	}
	first, err := s.add(ctx, t, id, in)
	require.NoError(t, err)
	require.False(t, first.GetDeduplicated())

	retry, err := s.add(ctx, t, id, &streampb.AddMessagesInput{
		Messages: streamMsgs("", "a", "b"), ProducerId: "p1", Sequence: 1,
	})
	require.NoError(t, err)
	require.True(t, retry.GetDeduplicated())
	require.Equal(t, first.GetFirstOffset(), retry.GetFirstOffset())

	// The retry must not have appended a second copy.
	got := s.poll(ctx, t, id, 0)
	require.Equal(t, []string{"a", "b"}, bodies(got.GetMessages()))

	// Same sequence with different content is a client bug. Returning the
	// recorded offsets would report success while dropping the data.
	_, err = s.add(ctx, t, id, &streampb.AddMessagesInput{
		Messages: streamMsgs("", "different"), ProducerId: "p1", Sequence: 1,
	})
	require.ErrorContains(t, err, "different content")
}

func TestStreamTopicFilter(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-topics"
	s.create(ctx, t, id)

	_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("tokens", "t1")})
	require.NoError(t, err)
	_, err = s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("tools", "x1")})
	require.NoError(t, err)
	_, err = s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("tokens", "t2")})
	require.NoError(t, err)

	got := s.poll(ctx, t, id, 0, "tokens")
	require.Equal(t, []string{"t1", "t2"}, bodies(got.GetMessages()))
	// Offsets are global, so a filtered read still advances past what it skipped.
	require.Equal(t, int64(3), got.GetNextOffset())
}

func TestStreamFinishWritingIsPerProducer(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-finish"
	s.create(ctx, t, id)

	_, err := s.client.FinishWriting(ctx, &streampb.FinishWritingRequest{
		FrontendRequest: &streampb.FinishWritingInput{
			Namespace: s.ns, StreamId: id, ProducerId: "p1",
		},
	})
	require.NoError(t, err)

	_, err = s.add(ctx, t, id, &streampb.AddMessagesInput{
		Messages: streamMsgs("", "a"), ProducerId: "p1", Sequence: 1,
	})
	require.Error(t, err)

	// Finishing is per-producer, not a close, so others carry on.
	_, err = s.add(ctx, t, id, &streampb.AddMessagesInput{
		Messages: streamMsgs("", "b"), ProducerId: "p2", Sequence: 1,
	})
	require.NoError(t, err)
}

func TestStreamCloseAndTruncate(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-lifecycle"
	s.create(ctx, t, id)

	_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "a", "b", "c")})
	require.NoError(t, err)

	_, err = s.client.TruncateStream(ctx, &streampb.TruncateStreamRequest{
		FrontendRequest: &streampb.TruncateStreamInput{
			Namespace: s.ns, StreamId: id, NewBaseOffset: 1,
		},
	})
	require.NoError(t, err)

	// A reader below the floor gets a distinguishable error carrying the floor,
	// so it can jump forward rather than fail.
	_, err = s.client.PollMessages(ctx, &streampb.PollMessagesRequest{
		FrontendRequest: &streampb.PollMessagesInput{Namespace: s.ns, StreamId: id, FromOffset: 0},
	})
	require.ErrorContains(t, err, "truncated")

	_, err = s.client.CloseStream(ctx, &streampb.CloseStreamRequest{
		FrontendRequest: &streampb.CloseStreamInput{Namespace: s.ns, StreamId: id},
	})
	require.NoError(t, err)

	_, err = s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "d")})
	require.Error(t, err)

	// Closed is a state a reader observes, not an error, and the data stays
	// readable rather than requiring a shutdown handshake with the producer.
	got := s.poll(ctx, t, id, 1)
	require.True(t, got.GetClosed())
	require.Equal(t, []string{"b", "c"}, bodies(got.GetMessages()))
}

func TestStreamReadStartingInsideABatch(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-mid-batch"
	s.create(ctx, t, id)

	// One batch covering 0..2, a second covering 3. A node is addressed by the
	// first offset of its batch, so reading from 2 has to find the node that
	// contains it rather than the node whose ID equals it. Getting that wrong
	// silently drops the messages before the boundary.
	_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "a", "b", "c")})
	require.NoError(t, err)
	_, err = s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "d")})
	require.NoError(t, err)

	for from, want := range map[int64][]string{
		0: {"a", "b", "c", "d"},
		1: {"b", "c", "d"},
		2: {"c", "d"},
		3: {"d"},
	} {
		got := s.poll(ctx, t, id, from)
		require.Equal(t, want, bodies(got.GetMessages()), "reading from offset %d", from)
		require.Equal(t, int64(4), got.GetNextOffset())
	}
}

func TestStreamBatchSizeIsBounded(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-batch-bound"
	s.create(ctx, t, id)

	// The bound is not only admission control: it bounds how far a read has to
	// step back to find the node containing an arbitrary offset.
	tooMany := make([]string, streamMaxBatch+1)
	for i := range tooMany {
		tooMany[i] = "x"
	}
	_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", tooMany...)})
	require.ErrorContains(t, err, "exceeds the limit")
}

func (s *streamTestEnv) pollWait(
	ctx context.Context, streamID string, from int64,
) (*streampb.PollMessagesOutput, error) {
	resp, err := s.client.PollMessages(ctx, &streampb.PollMessagesRequest{
		FrontendRequest: &streampb.PollMessagesInput{
			Namespace: s.ns, StreamId: streamID, FromOffset: from, WaitNewMessages: true,
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetFrontendResponse(), nil
}

func TestStreamLongPollWakesOnAppend(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-longpoll-append"
	s.create(ctx, t, id)

	type result struct {
		out *streampb.PollMessagesOutput
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := s.pollWait(ctx, id, 0)
		done <- result{out, err}
	}()

	// The poll is parked on an empty stream; the append is what releases it.
	_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "a")})
	require.NoError(t, err)

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, []string{"a"}, bodies(r.out.GetMessages()))
		require.Equal(t, int64(1), r.out.GetNextOffset())
	case <-time.After(25 * time.Second):
		t.Fatal("long poll did not wake on append")
	}
}

func TestStreamLongPollWakesOnClose(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-longpoll-close"
	s.create(ctx, t, id)

	done := make(chan *streampb.PollMessagesOutput, 1)
	go func() {
		out, err := s.pollWait(ctx, id, 0)
		if err == nil {
			done <- out
		}
	}()

	_, err := s.client.CloseStream(ctx, &streampb.CloseStreamRequest{
		FrontendRequest: &streampb.CloseStreamInput{Namespace: s.ns, StreamId: id},
	})
	require.NoError(t, err)

	// Closing has to release a parked reader, otherwise a consumer of a
	// finished stream waits out the full timeout for no reason.
	select {
	case out := <-done:
		require.True(t, out.GetClosed())
		require.Empty(t, out.GetMessages())
	case <-time.After(25 * time.Second):
		t.Fatal("long poll did not wake on close")
	}
}

func TestStreamLongPollReturnsEmptyOnTimeout(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-longpoll-timeout"
	s.create(ctx, t, id)

	// A timeout is an empty response, not an error: the caller polls again
	// rather than distinguishing a quiet stream from a failure.
	start := time.Now()
	out, err := s.pollWait(ctx, id, 0)
	require.NoError(t, err)
	require.Empty(t, out.GetMessages())
	require.Equal(t, int64(0), out.GetNextOffset())
	require.False(t, out.GetClosed())
	require.Greater(t, time.Since(start), 5*time.Second, "the poll should have parked, not returned immediately")
}

func TestStreamLongPollReturnsImmediatelyWhenBehind(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-longpoll-behind"
	s.create(ctx, t, id)

	_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "a", "b")})
	require.NoError(t, err)

	// Waiting is only for a reader that is caught up. One that is behind must
	// not be parked behind data that already exists.
	start := time.Now()
	out, err := s.pollWait(ctx, id, 0)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, bodies(out.GetMessages()))
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestStreamCapTruncatesAndReclaims(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-cap"

	_, err := s.client.CreateStream(ctx, &streampb.CreateStreamRequest{
		FrontendRequest: &streampb.CreateStreamInput{
			Namespace: s.ns, StreamId: id,
			Lifecycle: &streampb.StreamLifecycle{MaxItems: 4},
		},
	})
	require.NoError(t, err)

	for _, batch := range [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}} {
		_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", batch...)})
		require.NoError(t, err)
	}

	// Six appended against a cap of four, so the floor advanced without anyone
	// asking. Reading from the floor still works and returns exactly what the
	// cap retained.
	got := s.poll(ctx, t, id, 2)
	require.Equal(t, []string{"c", "d", "e", "f"}, bodies(got.GetMessages()))

	// Below the floor is a distinguishable error, not silence.
	_, err = s.client.PollMessages(ctx, &streampb.PollMessagesRequest{
		FrontendRequest: &streampb.PollMessagesInput{Namespace: s.ns, StreamId: id, FromOffset: 0},
	})
	require.ErrorContains(t, err, "truncated")
}

func TestStreamClosedStaysReadable(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-closed-readable"
	s.create(ctx, t, id)

	_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "a", "b")})
	require.NoError(t, err)
	_, err = s.client.CloseStream(ctx, &streampb.CloseStreamRequest{
		FrontendRequest: &streampb.CloseStreamInput{Namespace: s.ns, StreamId: id},
	})
	require.NoError(t, err)

	// Close seals, it does not delete. A consumer can finish draining without
	// coordinating a shutdown with the producer, which is the handshake the
	// signal-based implementation forces today.
	got := s.poll(ctx, t, id, 0)
	require.Equal(t, []string{"a", "b"}, bodies(got.GetMessages()))
	require.True(t, got.GetClosed())

	desc, err := s.client.DescribeStream(ctx, &streampb.DescribeStreamRequest{
		FrontendRequest: &streampb.DescribeStreamInput{Namespace: s.ns, StreamId: id},
	})
	require.NoError(t, err)
	require.NotNil(t, desc.GetFrontendResponse().GetState().GetCloseTime())
}

func TestStreamListStreams(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)

	created := []string{"list-a", "list-b", "list-c"}
	for _, id := range created {
		s.create(ctx, t, id)
	}

	// Visibility is written by a task after the create commits, so this is
	// eventually consistent by design rather than by accident.
	await.RequireTrue(t, func() bool {
		resp, err := s.client.ListStreams(ctx, &streampb.ListStreamsRequest{
			FrontendRequest: &streampb.ListStreamsInput{Namespace: s.ns},
		})
		if err != nil {
			return false
		}
		found := make(map[string]bool)
		for _, e := range resp.GetFrontendResponse().GetStreams() {
			found[e.GetStreamId()] = true
		}
		for _, id := range created {
			if !found[id] {
				return false
			}
		}
		return true
	}, 20*time.Second, 250*time.Millisecond)
}

// A capped poll must not read the whole stream to answer. The read is clipped
// to the offsets the caller can be given, so a reader asking for one message
// off a long stream does not pull the rest into the history host on its way.
func TestStreamPollReadsOnlyWhatItReturns(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-poll-cap"
	s.create(ctx, t, id)

	for i := range 40 {
		_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{
			Messages: streamMsgs("", fmt.Sprintf("m%d", i)),
		})
		require.NoError(t, err)
	}

	got := s.pollMax(ctx, t, id, 0, 1)
	require.Equal(t, []string{"m0"}, bodies(got.GetMessages()))
	require.Equal(t, int64(1), got.GetNextOffset(), "a capped read advances only over what it gave")
	require.Equal(t, int64(40), got.GetHeadOffset(), "the frontier is still reported in full")

	// Paging from there covers the rest, so the cap trims the read and not the stream.
	got = s.pollMax(ctx, t, id, got.GetNextOffset(), 10)
	require.Equal(t, []string{"m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10"},
		bodies(got.GetMessages()))
	require.Equal(t, int64(11), got.GetNextOffset())

	// A filter finding nothing in the page still has to leave the reader able to
	// go on, and it must stop at the page rather than scanning to the head.
	got = s.pollMaxTopics(ctx, t, id, 0, 5, "nothing-matches-this")
	require.Empty(t, got.GetMessages())
	require.Equal(t, int64(5), got.GetNextOffset(),
		"a filtered page advanced past its own bound, so the read ran to the head")
}

// A stream id can be reused. The cached bytes belong to the log, not to the
// name, so a reader of the new stream must never be served the old one's.
func TestStreamPollAfterIdIsReusedServesTheNewStream(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-reused-id"

	s.create(ctx, t, id)
	_, err := s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "old")})
	require.NoError(t, err)
	// Read it back so the bytes are certain to be cached before the id is reused.
	require.Equal(t, []string{"old"}, bodies(s.poll(ctx, t, id, 0).GetMessages()))

	_, err = s.client.DeleteStream(ctx, &streampb.DeleteStreamRequest{
		FrontendRequest: &streampb.DeleteStreamInput{Namespace: s.ns, StreamId: id},
	})
	require.NoError(t, err)

	s.create(ctx, t, id)
	_, err = s.add(ctx, t, id, &streampb.AddMessagesInput{Messages: streamMsgs("", "new")})
	require.NoError(t, err)

	got := s.poll(ctx, t, id, 0)
	require.Equal(t, []string{"new"}, bodies(got.GetMessages()),
		"a reused id served bytes from the deleted stream")
}

func (s *streamTestEnv) pollMaxTopics(
	ctx context.Context, t *testing.T, streamID string, from int64, maxMessages int32, topics ...string,
) *streampb.PollMessagesOutput {
	t.Helper()
	resp, err := s.client.PollMessages(ctx, &streampb.PollMessagesRequest{
		FrontendRequest: &streampb.PollMessagesInput{
			Namespace: s.ns, StreamId: streamID, FromOffset: from,
			MaxMessages: maxMessages, Topics: topics,
		},
	})
	require.NoError(t, err)
	return resp.GetFrontendResponse()
}

func (s *streamTestEnv) pollMax(
	ctx context.Context, t *testing.T, streamID string, from int64, maxMessages int32,
) *streampb.PollMessagesOutput {
	t.Helper()
	resp, err := s.client.PollMessages(ctx, &streampb.PollMessagesRequest{
		FrontendRequest: &streampb.PollMessagesInput{
			Namespace: s.ns, StreamId: streamID, FromOffset: from, MaxMessages: maxMessages,
		},
	})
	require.NoError(t, err)
	return resp.GetFrontendResponse()
}
