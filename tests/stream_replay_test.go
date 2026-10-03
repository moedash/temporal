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
	"google.golang.org/protobuf/types/known/durationpb"
)

// startConsumer starts a workflow on its own task queue and returns the
// execution and the queue.
func startConsumer(
	t *testing.T, s *streamTestEnv, prefix string,
) (*commonpb.WorkflowExecution, *taskqueuepb.TaskQueue) {
	t.Helper()
	id := prefix + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	we, err := s.env.FrontendClient().StartWorkflowExecution(
		s.ctx(),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.ns,
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: "stream-consumer"},
			TaskQueue:           tq,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            "tester",
		},
	)
	require.NoError(t, err)
	return &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.GetRunId()}, tq
}

func publishCommand(bodies ...string) []*commandpb.Command {
	messages := make([]*streampb.StreamRecord, len(bodies))
	for i, b := range bodies {
		messages[i] = &streampb.StreamRecord{Body: &commonpb.Payload{Data: []byte(b)}, Topic: "tokens"}
	}
	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_APPEND_STREAM_RECORDS,
		Attributes: &commandpb.Command_AppendStreamRecordsCommandAttributes{
			AppendStreamRecordsCommandAttributes: &commandpb.AppendStreamRecordsCommandAttributes{
				Records: messages,
			},
		},
	}}
}

func workflowTaskFailedWith(
	events []*historypb.HistoryEvent,
	cause enumspb.WorkflowTaskFailedCause,
) *historypb.WorkflowTaskFailedEventAttributes {
	for _, e := range events {
		if attrs := e.GetWorkflowTaskFailedEventAttributes(); attrs != nil && attrs.GetCause() == cause {
			return attrs
		}
	}
	return nil
}
