package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	chasmstream "go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
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
	env    *testcore.TestEnv
	client streampb.StreamServiceClient
	ns     string
}

func newStreamTestEnv(t *testing.T) *streamTestEnv {
	env := testcore.NewEnv(t)

	conn, err := grpc.NewClient(env.FrontendGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return &streamTestEnv{
		env:    env,
		client: streampb.NewStreamServiceClient(conn),
		ns:     env.Namespace().String(),
	}
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
