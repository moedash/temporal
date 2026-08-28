package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Path A: a workflow publishing to a stream it owns. The stream is co-located
// with the workflow, so the frontier advances in the workflow task's own
// commit, and History gets one fixed-size event naming the offset range rather
// than anything that was published.
//
// Driven through the raw task poller rather than an SDK, because emitting a new
// command type does not need one.
func TestStreamWorkflowPublishesWithARangeEvent(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	id := "stream-wf-publish-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	we, err := env.FrontendClient().StartWorkflowExecution(s.ctx(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.ns,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "stream-publisher"},
		TaskQueue:           tq,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            "tester",
	})
	require.NoError(t, err)

	published := false
	// The newer taskpoller cannot emit an arbitrary command type, which is
	// the whole point here.
	//nolint:staticcheck // SA1019: deprecated poller is the only one that can.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(*workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			if published {
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
					},
				}}, nil
			}
			published = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
				Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
					AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
						Messages: []*streampb.StreamMessage{
							{Body: &commonpb.Payload{Data: []byte("planning")}, Topic: "progress"},
							{Body: &commonpb.Payload{Data: []byte("calling tool")}, Topic: "progress"},
						},
					},
				},
			}}, nil
		},
		Logger: env.Logger,
		T:      t,
	}

	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	// One event for the batch, holding the range and none of the payload. Two
	// messages were published, so it has to name both of them and stop there.
	events := env.GetHistory(s.ns, &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.GetRunId()})
	var added []*historypb.WorkflowStreamMessagesAddedEventAttributes
	for _, e := range events {
		if a := e.GetWorkflowStreamMessagesAddedEventAttributes(); a != nil {
			added = append(added, a)
		}
	}
	require.Len(t, added, 1, "one publish command writes one event")
	require.Equal(t, int64(0), added[0].GetFirstOffset())
	require.Equal(t, int64(2), added[0].GetMessageCount())
	require.Equal(t, chasmworkflow.DefaultStreamName, added[0].GetStreamId(),
		"an unnamed stream resolves to the default before it is recorded")

	// The bodies stay out of History. Asserted on the serialized event rather
	// than on its fields, because a field this test forgot to check would still
	// be carrying them.
	raw, err := added[0].Marshal()
	require.NoError(t, err)
	require.NotContains(t, string(raw), "calling tool",
		"the event must name the range, never carry the payload")

	// Known gap, asserted rather than tolerated: an attached stream lives
	// inside the workflow's execution, so it has no standalone id to route on
	// and the read API cannot reach it. Addressing it needs a path-addressed
	// component reference, which CHASM does not expose publicly.
	//
	// When that lands, this expectation flips to reading the two messages back,
	// and the test will fail here to say so rather than quietly passing.
	streamID := we.GetRunId() + "/" + chasmworkflow.DefaultStreamName
	_, err = s.client.PollMessages(s.ctx(), &streamlib.PollMessagesRequest{
		FrontendRequest: &streamlib.PollMessagesInput{
			Namespace: s.ns, StreamId: streamID, FromOffset: 0,
		},
	})
	require.ErrorContains(t, err, "stream not found",
		"an attached stream is still only reachable through its workflow")
}
