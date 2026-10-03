package tests

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/workflowservice/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
)

// A query with no workflow task in flight is dispatched straight through
// matching, on a task built without RecordWorkflowTaskStarted. That task
// carries the whole history to whichever worker polls it, which may never have
// seen the execution, so it has to carry the recorded ranges too or the worker
// cannot replay a consuming workflow far enough to answer.
//
// The query is answered by a second poller that took part in none of the
// earlier tasks, from the slices on the query task alone.
func TestQueryTaskCarriesTheConsumedRanges(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)
	execution, tq := startConsumer(t, s, "stream-wf-query-")

	task := 0
	//nolint:staticcheck // SA1019: only the deprecated poller can emit this command type.
	worker := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "worker",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			task++
			if task == 1 {
				return publishCommand("answer-1", "answer-2"), nil
			}
			return nil, nil
		},
		Logger: env.Logger,
		T:      t,
	}

	_, err := worker.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	_, err = s.client.SubscribeWorkflow(s.ctx(), &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(),
			StreamName: chasmworkflow.DefaultStreamName, StartOffset: 0,
		},
	})
	require.NoError(t, err)

	// The subscription schedules the task that consumes the backlog.
	_, err = worker.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	consumedAt := completedEventWithCursors(t, env.GetHistory(s.ns, execution))

	var queryTask *workflowservice.PollWorkflowTaskQueueResponse
	//nolint:staticcheck // SA1019: consistent with the worker above.
	coldWorker := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "cold-worker",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			t.Fatal("the query must arrive as a query task, not a workflow task")
			return nil, nil
		},
		QueryHandler: func(
			resp *workflowservice.PollWorkflowTaskQueueResponse,
		) (*commonpb.Payloads, error) {
			queryTask = resp
			// Answer from the task alone, which is all a replaying worker has.
			var seen []string
			for _, slice := range resp.GetStreamSlices() {
				for _, record := range slice.GetRecords() {
					seen = append(seen, string(record.GetBody().GetData()))
				}
			}
			return payloads.EncodeString(strings.Join(seen, ",")), nil
		},
		Logger: env.Logger,
		T:      t,
	}

	type answer struct {
		resp *workflowservice.QueryWorkflowResponse
		err  error
	}
	answered := make(chan answer, 1)
	go func() {
		resp, err := env.FrontendClient().QueryWorkflow(s.ctx(), &workflowservice.QueryWorkflowRequest{
			Namespace: s.ns,
			Execution: execution,
			Query:     &querypb.WorkflowQuery{QueryType: "consumed"},
		})
		answered <- answer{resp: resp, err: err}
	}()

	res, err := coldWorker.PollAndProcessWorkflowTask()
	require.NoError(t, err)
	require.True(t, res.IsQueryTask, "the query is dispatched on its own task")
	require.NotNil(t, queryTask.GetQuery())
	require.NotEmpty(t, queryTask.GetHistory().GetEvents(), "a non-sticky query carries full history")

	replayed := sliceForEvent(queryTask.GetStreamSlices(), consumedAt)
	require.NotNil(t, replayed, "the query task carries the range recorded at event %d", consumedAt)
	require.Equal(t, int64(0), replayed.GetFromOffset())
	require.Equal(t, int64(2), replayed.GetToOffset())
	require.Equal(t, execution.GetRunId(), replayed.GetRunId(), "the slice names the run holding the stream")
	require.Equal(t, []string{"answer-1", "answer-2"}, apiBodies(replayed.GetRecords()))

	// Nothing on a query task is about to be consumed, so every slice is a
	// re-supply for a completion the history already holds.
	for _, slice := range queryTask.GetStreamSlices() {
		require.NotZero(t, slice.GetWorkflowTaskCompletedEventId(),
			"a query task must not carry an untagged slice, it starts no task")
	}

	got := <-answered
	require.NoError(t, got.err)
	var result string
	require.NoError(t, payloads.Decode(got.resp.GetQueryResult(), &result))
	require.Equal(t, "answer-1,answer-2", result,
		"the query answer is computed from the re-supplied records")
}
