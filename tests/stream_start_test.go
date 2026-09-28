package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	chasmstream "go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A reader with no offset yet names where it starts, and the server resolves
// that in the read it serves, so the first poll cannot race with truncation.

func (s *streamTestEnv) pollFrom(
	t *testing.T, streamID string, start *streampb.StreamStartPosition,
) (*streamlib.PollMessagesOutput, error) {
	t.Helper()
	resp, err := s.client.PollMessages(s.ctx(), &streamlib.PollMessagesRequest{
		FrontendRequest: &streamlib.PollMessagesInput{
			Namespace: s.ns, StreamId: streamID, StartPosition: start,
		},
	})
	return resp.GetFrontendResponse(), err
}

func (s *streamTestEnv) pollOwnedFrom(
	t *testing.T, owner *streamlib.StreamOwner, name string, start *streampb.StreamStartPosition,
) *streamlib.PollMessagesOutput {
	t.Helper()
	resp, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, Owner: owner, StreamName: name, StartPosition: start,
		},
	})
	require.NoError(t, err)
	return resp.GetFrontendResponse()
}

func TestStreamFirstPollStartPositions(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-start-positions"
	s.create(ctx, t, id)
	_, err := s.add(ctx, t, id, &streamlib.AddMessagesInput{
		Records: streamMsgs("", "a", "b", "c", "d", "e"),
	})
	require.NoError(t, err)
	_, err = s.client.TruncateStream(ctx, &streamlib.TruncateStreamRequest{
		FrontendRequest: &streamlib.TruncateStreamInput{
			Namespace: s.ns, StreamId: id, NewBaseOffset: 2,
		},
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		start *streampb.StreamStartPosition
		want  []string
	}{
		{"earliest reads from the floor", chasmstream.Earliest(), []string{"c", "d", "e"}},
		{"last n", chasmstream.LastN(2), []string{"d", "e"}},
		{"last n past the floor", chasmstream.LastN(100), []string{"c", "d", "e"}},
		{"offset", chasmstream.AtOffset(3), []string{"d", "e"}},
		{"tail reads nothing yet", chasmstream.Tail(), []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.pollFrom(t, id, tc.start)
			require.NoError(t, err)
			require.Equal(t, tc.want, bodies(got.GetRecords()))
			require.Equal(t, int64(5), got.GetNextOffset(),
				"next_offset carries the resolved start for the polls after it")
		})
	}

	_, err = s.pollFrom(t, id, chasmstream.AtOffset(0))
	require.ErrorContains(t, err, "truncated", "an absolute offset below the floor is refused")
}

func TestStreamFirstPollRefusesAStartItCannotResolve(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-start-refused"
	s.create(ctx, t, id)

	for _, start := range []*streampb.StreamStartPosition{
		{},
		chasmstream.LastN(0),
		{Position: &streampb.StreamStartPosition_Earliest{Earliest: false}},
	} {
		_, err := s.pollFrom(t, id, start)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	}

	// A reader that already has an offset resumes from it.
	_, err := s.client.PollMessages(ctx, &streamlib.PollMessagesRequest{
		FrontendRequest: &streamlib.PollMessagesInput{
			Namespace: s.ns, StreamId: id, FromOffset: 1, StartPosition: chasmstream.Earliest(),
		},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
}

// A tail read that blocks resolves the head once, before it waits. Resolving
// it again after the wake would land on the new head and return nothing.
func TestStreamTailPollWaitsForTheNextAppend(t *testing.T) {
	s := newStreamTestEnv(t)
	ctx := streamCtx(t)
	const id = "stream-start-tail-wait"
	s.create(ctx, t, id)
	_, err := s.add(ctx, t, id, &streamlib.AddMessagesInput{Records: streamMsgs("", "old")})
	require.NoError(t, err)

	type result struct {
		out *streamlib.PollMessagesOutput
		err error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := s.client.PollMessages(ctx, &streamlib.PollMessagesRequest{
			FrontendRequest: &streamlib.PollMessagesInput{
				Namespace: s.ns, StreamId: id, StartPosition: chasmstream.Tail(),
				WaitNewMessages: true,
			},
		})
		done <- result{resp.GetFrontendResponse(), err}
	}()

	// Appended until the parked poll takes one, since there is no signal for
	// when it has parked.
	deadline := time.After(25 * time.Second)
	for {
		_, err := s.add(ctx, t, id, &streamlib.AddMessagesInput{Records: streamMsgs("", "new")})
		require.NoError(t, err)
		select {
		case r := <-done:
			require.NoError(t, r.err)
			require.NotEmpty(t, r.out.GetRecords())
			for _, b := range bodies(r.out.GetRecords()) {
				require.Equal(t, "new", b, "a tail read never returns what was there before it")
			}
			return
		case <-time.After(500 * time.Millisecond):
		case <-deadline:
			t.Fatal("tail poll did not wake on append")
		}
	}
}

func TestStreamActivityStreamStartPositions(t *testing.T) {
	env, s := newStreamActivityEnv(t)

	activityID := "stream-start-saa-" + uuid.NewString()
	taskQueue := activityID + "-tq"
	_, err := env.FrontendClient().StartActivityExecution(s.ctx(),
		&workflowservice.StartActivityExecutionRequest{
			Namespace:           s.ns,
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "streaming-activity"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           uuid.NewString(),
		})
	require.NoError(t, err)
	owner := &streamlib.StreamOwner{Kind: streamlib.STREAM_OWNER_KIND_ACTIVITY, Id: activityID}

	// Nothing published yet reads as an empty stream at offset zero, whatever
	// the position.
	for _, start := range []*streampb.StreamStartPosition{
		chasmstream.Earliest(), chasmstream.Tail(), chasmstream.LastN(3),
	} {
		empty := s.pollOwnedFrom(t, owner, "", start)
		require.Empty(t, empty.GetRecords())
		require.Equal(t, int64(0), empty.GetNextOffset())
	}

	pollActivityTask(t, env, s, taskQueue)
	_, err = s.addOwned(t, owner, "", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(1, "one", "two", "three"),
	})
	require.NoError(t, err)

	require.Equal(t, []string{"one", "two", "three"},
		bodies(s.pollOwnedFrom(t, owner, "", chasmstream.Earliest()).GetRecords()))
	require.Equal(t, []string{"three"},
		bodies(s.pollOwnedFrom(t, owner, "", chasmstream.LastN(1)).GetRecords()))
	tail := s.pollOwnedFrom(t, owner, "", chasmstream.Tail())
	require.Empty(t, tail.GetRecords())
	require.Equal(t, int64(3), tail.GetNextOffset())
}

func TestStreamWorkflowActivityStreamStartPositions(t *testing.T) {
	env, s := newStreamActivityEnv(t)

	workflowID := "stream-start-wfa-" + uuid.NewString()
	runID := scheduleStreamingActivity(t, env, s, workflowID, "model-call", nil)
	pollActivityTask(t, env, s, workflowID+"-tq")
	owner := &streamlib.StreamOwner{
		Kind:       streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY,
		Id:         workflowID,
		RunId:      runID,
		ActivityId: "model-call",
	}
	_, err := s.addOwned(t, owner, "", &streamlib.AddWorkflowMessagesInput{
		Records: attemptRecords(1, "a", "b", "c"),
	})
	require.NoError(t, err)

	require.Equal(t, []string{"b", "c"},
		bodies(s.pollOwnedFrom(t, owner, "", chasmstream.LastN(2)).GetRecords()))
	require.Equal(t, []string{"a", "b", "c"},
		bodies(s.pollOwnedFrom(t, owner, "", chasmstream.Earliest()).GetRecords()))
}
