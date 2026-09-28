package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	chasmstream "go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/tests/testcore"
)

// A subscribe command names where it starts. The server resolves the position
// when it registers the subscription and records the absolute offset on the
// event, so a replay never resolves it again.

func subscribeFromCommand(
	nameOrID string, offset int64, start *streampb.StreamStartPosition,
) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SUBSCRIBE_STREAM,
		Attributes: &commandpb.Command_SubscribeStreamCommandAttributes{
			SubscribeStreamCommandAttributes: &commandpb.SubscribeStreamCommandAttributes{
				StreamNameOrId: nameOrID, StartOffset: offset, StartPosition: start,
			},
		},
	}
}

func subscribedStarts(events []*historypb.HistoryEvent) map[string]int64 {
	out := map[string]int64{}
	for _, e := range events {
		if a := e.GetWorkflowStreamSubscribedEventAttributes(); a != nil {
			out[a.GetStreamId()] = a.GetStartOffset()
		}
	}
	return out
}

func TestStreamSubscribeCommandRecordsTheResolvedStart(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)
	ctx := streamCtx(t)
	execution, tq := startConsumer(t, s, "stream-sub-start-")

	// A standalone stream whose first two records were truncated away. Offset
	// zero names nothing it still holds, which earliest has to get right.
	standalone := execution.GetWorkflowId() + "-standalone"
	s.create(ctx, t, standalone)
	_, err := s.add(ctx, t, standalone, &streamlib.AddMessagesInput{
		Records: streamMsgs("", "a", "b", "c", "d", "e"),
	})
	require.NoError(t, err)
	_, err = s.client.TruncateStream(ctx, &streamlib.TruncateStreamRequest{
		FrontendRequest: &streamlib.TruncateStreamInput{
			Namespace: s.ns, StreamId: standalone, NewBaseOffset: 2,
		},
	})
	require.NoError(t, err)

	owner := &streamlib.StreamOwner{
		Kind: streamlib.STREAM_OWNER_KIND_WORKFLOW, Id: execution.GetWorkflowId(),
	}
	_, err = s.addOwned(t, owner, "inputs", &streamlib.AddWorkflowMessagesInput{
		Records: streamMsgs("", "x", "y", "z"),
	})
	require.NoError(t, err)

	//nolint:staticcheck // SA1019: only the deprecated poller can emit this command type.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			return []*commandpb.Command{
				subscribeFromCommand(standalone, 0, chasmstream.Earliest()),
				subscribeFromCommand("inputs", 0, chasmstream.LastN(1)),
				subscribeFromCommand("not-yet-written", 0, chasmstream.Tail()),
			}, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	require.Equal(t, map[string]int64{
		standalone:        2,
		"inputs":          2,
		"not-yet-written": 0,
	}, subscribedStarts(env.GetHistory(s.ns, execution)))
}

func TestStreamSubscribeCommandRefusesAStartItCannotResolve(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	for _, command := range []*commandpb.Command{
		subscribeFromCommand("inputs", 1, chasmstream.Earliest()),
		subscribeFromCommand("inputs", 0, chasmstream.LastN(0)),
		subscribeFromCommand("inputs", 0, &streampb.StreamStartPosition{}),
	} {
		execution, tq := startConsumer(t, s, "stream-sub-start-refused-")
		//nolint:staticcheck // SA1019: only the deprecated poller can emit this command type.
		poller := &testcore.TaskPoller{
			Client:    env.FrontendClient(),
			Namespace: s.ns,
			TaskQueue: tq,
			Identity:  "tester",
			WorkflowTaskHandler: func(
				*workflowservice.PollWorkflowTaskQueueResponse,
			) ([]*commandpb.Command, error) {
				return []*commandpb.Command{command}, nil
			},
			Logger: env.Logger,
			T:      t,
		}
		_, err := poller.PollAndProcessWorkflowTask()
		require.Error(t, err)

		events := env.GetHistory(s.ns, execution)
		require.NotNil(t, workflowTaskFailedWith(events,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SUBSCRIBE_STREAM_ATTRIBUTES))
		require.Empty(t, subscribedStarts(events), "a refused subscribe registers nothing")
	}
}
