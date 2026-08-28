package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// What a history event per publish would cost.
//
// Publishing from workflow code is the one stream command with no history
// event, which is why no SDK can reach it: sdk-core matches commands to events
// positionally, so a command that produces none desynchronises replay. Giving
// it an event fixes that, and the objection is that unlike subscribing, which
// happens once, publishing happens per batch. This measures the per-batch
// price so the trade is decided on a number.
//
// The method is a marginal one. Each arm runs a workflow whose single workflow
// task carries N publish commands and then completes, so history holds the
// fixed opening and closing events plus exactly N publishes. Differencing
// against the N=0 arm cancels the fixed part and leaves the cost of one
// publish. Message size is varied against a fixed batch count to show whether
// payload bytes reach History at all.

type publishCostArm struct {
	name             string
	batches          int
	messagesPerBatch int
	messageSize      int
	// Sends the same bytes as Signals instead, which is what a workflow
	// streaming today has to do. The comparison is the point of the exercise.
	viaSignal bool
}

type publishCostResult struct {
	arm           publishCostArm
	historyEvents int64
	historyBytes  int64
}

func (r publishCostResult) messages() int {
	return r.arm.batches * r.arm.messagesPerBatch
}

// The published limits a per-batch event has to be judged against.
const (
	historyCountLimitError = 50 * 1024
	historySizeLimitError  = 50 * 1024 * 1024
)

func TestStreamPublishHistoryCost(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement, not a correctness check")
	}

	arms := []publishCostArm{
		// The control. Everything is differenced against this.
		{name: "control", batches: 0},

		// Batch count at a fixed message shape: is the cost linear, and what
		// is the slope.
		{name: "stream-b1", batches: 1, messagesPerBatch: 1, messageSize: 20},
		{name: "stream-b10", batches: 10, messagesPerBatch: 1, messageSize: 20},
		{name: "stream-b100", batches: 100, messagesPerBatch: 1, messageSize: 20},
		{name: "stream-b500", batches: 500, messagesPerBatch: 1, messageSize: 20},

		// Same batch count, more messages inside each: the per-batch event
		// should not notice.
		{name: "stream-b100-m10", batches: 100, messagesPerBatch: 10, messageSize: 20},

		// Same batch count, bigger messages: this is the claim that payloads
		// never enter History, stated as a measurement.
		{name: "stream-b100-s200", batches: 100, messagesPerBatch: 1, messageSize: 200},
		{name: "stream-b100-s2000", batches: 100, messagesPerBatch: 1, messageSize: 2000},

		// What the same traffic costs through Signals today.
		{name: "signal-b100-s20", batches: 100, messagesPerBatch: 1, messageSize: 20, viaSignal: true},
		{name: "signal-b100-s2000", batches: 100, messagesPerBatch: 1, messageSize: 2000, viaSignal: true},
	}

	results := make([]publishCostResult, 0, len(arms))
	byName := map[string]publishCostResult{}
	for _, arm := range arms {
		r := runPublishCostArm(t, arm)
		results = append(results, r)
		byName[arm.name] = r
	}
	reportPublishCost(t, results)
	assertPublishCost(t, byName)
}

// The report is the deliverable, but the properties it shows are the ones the
// design rests on, so they are asserted rather than left to be eyeballed.
func assertPublishCost(t *testing.T, byName map[string]publishCostResult) {
	control := byName["control"]
	marginalBytes := func(name string) int64 {
		return byName[name].historyBytes - control.historyBytes
	}
	marginalEvents := func(name string) int64 {
		return byName[name].historyEvents - control.historyEvents
	}

	// One event per batch, not per message. b100-m10 publishes ten times the
	// messages of b100 through the same hundred commands.
	require.Equal(t, int64(100), marginalEvents("stream-b100"))
	require.Equal(t, int64(100), marginalEvents("stream-b100-m10"))
	require.Equal(t, int64(500), marginalEvents("stream-b500"))

	// A hundredfold increase in payload size must not move History at all,
	// which is the claim that bodies never enter it.
	require.Equal(t, marginalBytes("stream-b100-s200"), marginalBytes("stream-b100-s2000"),
		"20x the payload changed the history cost, so payload is reaching History")

	// Ten times the messages through the same batches, within a byte or two of
	// noise from varint widths.
	require.InDelta(t, marginalBytes("stream-b100"), marginalBytes("stream-b100-m10"), 200,
		"cost tracked message count rather than batch count")

	// The comparison that justifies the feature. Same traffic, and Signals are
	// the only way to do this today.
	require.Less(t, marginalBytes("stream-b100-s2000"), marginalBytes("signal-b100-s2000")/20,
		"a stream publish should be far cheaper than the Signal it replaces")
}

func runPublishCostArm(t *testing.T, arm publishCostArm) publishCostResult {
	t.Helper()
	env := testcore.NewEnv(t, testcore.WithDisableTestloggerFailure())

	// Fixed-width regardless of the arm, because the workflow id and the task
	// queue name derived from it are both carried in the opening events. Naming
	// the arms in there would put ten bytes of arm-name difference into the very
	// figure being differenced.
	id := "publish-cost-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	ctx := testcore.NewContext()

	_, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "publish-cost"},
		TaskQueue:           tq,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(60 * time.Second),
		Identity:            "tester",
	})
	require.NoError(t, err)

	body := []byte(strings.Repeat("x", arm.messageSize))

	// Sent before the first workflow task is polled, so they land in history
	// ahead of it and the single task still closes the workflow.
	if arm.viaSignal {
		for range arm.batches {
			payloads := make([]*commonpb.Payload, 0, arm.messagesPerBatch)
			for range arm.messagesPerBatch {
				payloads = append(payloads, &commonpb.Payload{Data: body})
			}
			_, err := env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
				Namespace:         env.Namespace().String(),
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id},
				SignalName:        "stream-item",
				Input:             &commonpb.Payloads{Payloads: payloads},
				Identity:          "tester",
				RequestId:         uuid.NewString(),
			})
			require.NoError(t, err)
		}
	}

	//nolint:staticcheck // SA1019: only the deprecated poller emits raw commands.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(*workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			commands := make([]*commandpb.Command, 0, arm.batches+1)
			if !arm.viaSignal {
				for range arm.batches {
					messages := make([]*streampb.StreamMessage, 0, arm.messagesPerBatch)
					for range arm.messagesPerBatch {
						messages = append(messages, &streampb.StreamMessage{
							Body:  &commonpb.Payload{Data: body},
							Topic: "progress",
						})
					}
					commands = append(commands, &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
						Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
							AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
								Messages: messages,
							},
						},
					})
				}
			}
			return append(commands, &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			}), nil
		},
		Logger: env.Logger,
		T:      t,
	}

	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: id},
	})
	require.NoError(t, err)

	return publishCostResult{
		arm:           arm,
		historyEvents: desc.GetWorkflowExecutionInfo().GetHistoryLength(),
		historyBytes:  desc.GetWorkflowExecutionInfo().GetHistorySizeBytes(),
	}
}

func reportPublishCost(t *testing.T, results []publishCostResult) {
	var control publishCostResult
	for _, r := range results {
		if r.arm.name == "control" {
			control = r
		}
	}

	t.Log("Cost of one history event per publish, measured by differencing against an empty run.")
	t.Logf("Control: %d events, %d bytes.", control.historyEvents, control.historyBytes)
	t.Log("")
	t.Log("| arm | batches | msgs | msg size | events | bytes | events/batch | bytes/batch | bytes/msg |")
	t.Log("|---|---|---|---|---|---|---|---|---|")
	for _, r := range results {
		if r.arm.batches == 0 {
			continue
		}
		marginalEvents := r.historyEvents - control.historyEvents
		marginalBytes := r.historyBytes - control.historyBytes
		perMsg := "n/a"
		if r.messages() > 0 {
			perMsg = fmt.Sprintf("%.1f", float64(marginalBytes)/float64(r.messages()))
		}
		t.Logf("| %s | %d | %d | %d | %d | %d | %.2f | %.1f | %s |",
			r.arm.name, r.arm.batches, r.messages(), r.arm.messageSize,
			r.historyEvents, r.historyBytes,
			float64(marginalEvents)/float64(r.arm.batches),
			float64(marginalBytes)/float64(r.arm.batches),
			perMsg)
	}
	t.Log("")

	// The ceilings the number has to be read against. An event per batch makes
	// the event-count limit the binding one long before the size limit.
	for _, r := range results {
		if r.arm.batches == 0 || r.arm.viaSignal {
			continue
		}
		marginalBytes := r.historyBytes - control.historyBytes
		marginalEvents := r.historyEvents - control.historyEvents
		if marginalEvents == 0 {
			continue
		}
		bytesPerEvent := float64(marginalBytes) / float64(marginalEvents)
		t.Logf("%s: %.0f bytes/event implies %d batches before the %d-event limit, "+
			"%d before the %dMB size limit",
			r.arm.name, bytesPerEvent,
			historyCountLimitError,
			historyCountLimitError,
			int(float64(historySizeLimitError)/bytesPerEvent),
			historySizeLimitError/(1024*1024))
	}
}
