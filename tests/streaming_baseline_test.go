package tests

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/tests/testcore"
)

// Baseline measurements for today's Workflow Streams pattern: an Activity
// batches items into Signals and a consumer long-polls with an Update. AI-198
// proposes replacing it, and a replacement cannot be justified without knowing
// what the current pattern actually costs.
//
// The two dimensions that matter are the flush interval and the subscriber
// count. The shipped guidance is a ~2s flush; the Native Streams 1-pager asks
// for 100ms to be practical. Running both shows the cost of closing that gap
// under the current design, which is the number the September check-in needs.
//
// The full matrix takes minutes, so it is opt-in via TEMPORAL_STREAM_BENCH=1.
// Without it a short configuration still runs, which keeps the harness honest
// without slowing the suite.

const (
	streamBatchSignal = "stream_batch"
	streamDoneSignal  = "stream_done"
	streamPollUpdate  = "poll_events"
	streamMessageSize = 20
)

type streamBaselineParams struct {
	name          string
	flushInterval time.Duration
	subscribers   int
	messageRate   int // messages per second
	duration      time.Duration
}

type streamBaselineResult struct {
	params streamBaselineParams

	messagesSent     int
	messagesReceived int
	pollRejections   int64

	historyBytes  int64
	historyEvents int64
	// Set when the workload has no workflow at all, so the history columns are
	// an absence rather than a measurement. Rendering them as 0.00 next to a
	// measured figure reads as a comparison that was never made.
	historyNotApplicable bool

	persistenceRequests int64
	persistenceByOp     map[string]int64

	latencyP50 time.Duration
	latencyP99 time.Duration

	// Set when the run could not complete, for example because the workflow
	// exceeded a history limit. That is itself a result worth reporting.
	failure string
}

// streamBaselineWorkflow mirrors the shipped pattern: Signals carry batches in,
// an Update long-polls them back out, and the workflow holds the buffer.
func streamBaselineWorkflow(ctx workflow.Context) error {
	var buffer []string
	done := false

	err := workflow.SetUpdateHandler(ctx, streamPollUpdate,
		func(ctx workflow.Context, lastSeen int) ([]string, error) {
			// The shape that makes this expensive: every poll is a durable
			// state transition, even when it returns nothing new.
			if err := workflow.Await(ctx, func() bool {
				return len(buffer) > lastSeen || done
			}); err != nil {
				return nil, err
			}
			if lastSeen >= len(buffer) {
				return nil, nil
			}
			out := make([]string, len(buffer)-lastSeen)
			copy(out, buffer[lastSeen:])
			return out, nil
		})
	if err != nil {
		return err
	}

	batches := workflow.GetSignalChannel(ctx, streamBatchSignal)
	finish := workflow.GetSignalChannel(ctx, streamDoneSignal)

	for !done {
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(batches, func(c workflow.ReceiveChannel, _ bool) {
			var batch []string
			c.Receive(ctx, &batch)
			buffer = append(buffer, batch...)
		})
		sel.AddReceive(finish, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			done = true
		})
		sel.Select(ctx)
	}

	// Let any parked pollers observe the terminal state before exiting.
	return workflow.Await(ctx, func() bool { return true })
}

func TestStreamingBaseline(t *testing.T) {
	matrix := shortStreamBaselineMatrix()
	if os.Getenv("TEMPORAL_STREAM_BENCH") == "1" {
		matrix = fullStreamBaselineMatrix()
	}

	results := make([]streamBaselineResult, 0, len(matrix))
	for _, p := range matrix {
		t.Run(p.name, func(t *testing.T) {
			results = append(results, runStreamBaseline(t, p))
		})
	}
	reportStreamBaseline(t, results)
}

func shortStreamBaselineMatrix() []streamBaselineParams {
	return []streamBaselineParams{
		{name: "flush2s_sub1", flushInterval: 2 * time.Second, subscribers: 1, messageRate: 40, duration: 6 * time.Second},
		{name: "flush100ms_sub1", flushInterval: 100 * time.Millisecond, subscribers: 1, messageRate: 40, duration: 6 * time.Second},
	}
}

func fullStreamBaselineMatrix() []streamBaselineParams {
	var out []streamBaselineParams
	for _, flush := range []time.Duration{2 * time.Second, 100 * time.Millisecond} {
		for _, subs := range []int{1, 5, 25} {
			out = append(out, streamBaselineParams{
				name:          fmt.Sprintf("flush%s_sub%d", flush, subs),
				flushInterval: flush,
				subscribers:   subs,
				messageRate:   40,
				duration:      50 * time.Second,
			})
		}
	}
	return out
}

func runStreamBaseline(t *testing.T, p streamBaselineParams) streamBaselineResult {
	// Dedicated cluster so competing test load does not distort the timings.
	// Metric capture is namespace-scoped because persistence_requests carries a
	// namespace tag, which also attributes the counts to this workload rather
	// than to cluster background traffic.
	//
	// Testlogger failure is off because a sustained-load run torn down while
	// queues are still draining always logs shard-status errors on shutdown.
	// The cost is losing the safety net that would catch a genuine server error
	// during the run, so treat an anomalous result as a reason to re-run with
	// the option removed rather than trusting it.
	env := testcore.NewEnv(t, testcore.WithDisableTestloggerFailure())
	res := streamBaselineResult{params: p, persistenceByOp: map[string]int64{}}

	env.SdkWorker().RegisterWorkflow(streamBaselineWorkflow)

	ctx, cancel := context.WithTimeout(context.Background(), p.duration+2*time.Minute)
	defer cancel()

	wfID := env.Tv().WorkflowID()
	run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: env.WorkerTaskQueue(),
	}, streamBaselineWorkflow)
	require.NoError(t, err)

	// Started after workflow creation so cluster, namespace, and start costs do
	// not inflate the per-message figures. Both designs pay those equally.
	capture := env.StartNamespaceMetricCapture()

	sentAt := &sync.Map{} // message sequence -> generation time
	var receivedTotal atomic.Int64
	var pollRejections atomic.Int64
	var consumers sync.WaitGroup
	consumerLatencies := make([][]time.Duration, p.subscribers)
	consumerCounts := make([]int, p.subscribers)
	consumerCtx, stopConsumers := context.WithCancel(ctx)
	defer stopConsumers()

	for i := range p.subscribers {
		consumers.Add(1)
		go func(idx int) {
			defer consumers.Done()
			lat, n := runStreamConsumer(consumerCtx, env, wfID, run.GetRunID(), sentAt, &receivedTotal, &pollRejections)
			consumerLatencies[idx] = lat
			consumerCounts[idx] = n
		}(i)
	}

	res.messagesSent = runStreamProducer(ctx, t, env, wfID, run.GetRunID(), p, sentAt, &res)

	// Drain, then stop the consumers, and only then let the workflow finish, so
	// that no consumer is ever polling an execution that is completing. One that
	// is counts its own doomed polls as rejections and its client backoff as
	// delivery latency, which measures the harness rather than the pattern. The
	// workflow finishes last so the history numbers below are final rather than a
	// mid-flight snapshot. A cell that fails to drain is reported rather than
	// failed: hitting a limit is a real property of this pattern and is part of
	// what the benchmark is measuring.
	want := int64(res.messagesSent) * int64(p.subscribers)
	if !waitForDrain(ctx, &receivedTotal, want, 15*time.Second) {
		t.Logf("drained %d of %d expected deliveries before timeout", receivedTotal.Load(), want)
	}
	stopConsumers()
	consumers.Wait()
	require.NoError(t, env.SdkClient().SignalWorkflow(ctx, wfID, run.GetRunID(), streamDoneSignal, nil))
	if err := run.Get(ctx, nil); err != nil {
		t.Logf("workflow did not complete cleanly: %v", err)
	}

	var all []time.Duration
	for i := range p.subscribers {
		all = append(all, consumerLatencies[i]...)
		res.messagesReceived += consumerCounts[i]
	}
	res.pollRejections = pollRejections.Load()
	res.latencyP50 = percentile(all, 0.50)
	res.latencyP99 = percentile(all, 0.99)

	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: env.Tv().WithWorkflowID(wfID).WorkflowExecution(),
	})
	if err == nil {
		res.historyBytes = desc.GetWorkflowExecutionInfo().GetHistorySizeBytes()
		res.historyEvents = desc.GetWorkflowExecutionInfo().GetHistoryLength()
	}

	for _, rec := range capture.Metric(metrics.PersistenceRequests.Name()) {
		res.persistenceRequests += recordingCount(rec)
		if op, ok := rec.Tags["operation"]; ok {
			res.persistenceByOp[op] += recordingCount(rec)
		}
	}

	return res
}

func runStreamProducer(
	ctx context.Context,
	t *testing.T,
	env *testcore.TestEnv,
	wfID, runID string,
	p streamBaselineParams,
	sentAt *sync.Map,
	res *streamBaselineResult,
) int {
	payload := make([]byte, streamMessageSize)
	for i := range payload {
		payload[i] = 'x'
	}

	// Messages are generated continuously and flushed on the interval, which is
	// what an EventBatcher does. Latency is stamped at generation, not at
	// flush: the time an item waits in the batcher is the dominant cost of a
	// long flush interval, and stamping at flush would hide it entirely.
	genTicker := time.NewTicker(time.Second / time.Duration(p.messageRate))
	defer genTicker.Stop()
	flushTicker := time.NewTicker(p.flushInterval)
	defer flushTicker.Stop()
	deadline := time.Now().Add(p.duration)

	seq := 0
	var pending []string

	flush := func() bool {
		if len(pending) == 0 {
			return true
		}
		batch := pending
		pending = nil
		if err := env.SdkClient().SignalWorkflow(ctx, wfID, runID, streamBatchSignal, batch); err != nil {
			// A history or signal limit is a legitimate outcome for this
			// pattern, not a broken harness. Record it and stop.
			res.failure = err.Error()
			t.Logf("producer stopped after %d messages: %v", seq, err)
			return false
		}
		return true
	}

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return seq
		case <-genTicker.C:
			sentAt.Store(seq, time.Now())
			pending = append(pending, fmt.Sprintf("%d:%s", seq, payload))
			seq++
		case <-flushTicker.C:
			if !flush() {
				return seq
			}
		}
	}
	flush()
	return seq
}

func runStreamConsumer(
	ctx context.Context,
	env *testcore.TestEnv,
	wfID, runID string,
	sentAt *sync.Map,
	receivedTotal *atomic.Int64,
	rejections *atomic.Int64,
) ([]time.Duration, int) {
	var latencies []time.Duration
	var batch []string
	lastSeen := 0

	for ctx.Err() == nil {
		handle, err := env.SdkClient().UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
			WorkflowID:   wfID,
			RunID:        runID,
			UpdateName:   streamPollUpdate,
			Args:         []any{lastSeen},
			WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
		})
		if err == nil {
			err = handle.Get(ctx, &batch)
		}
		if err != nil {
			// A rejected poll is a measurement, not a reason to stop. Killing
			// the consumer here would report a server limit as consumer
			// slowness, which is a different and much less useful claim.
			rejections.Add(1)
			select {
			case <-ctx.Done():
				return latencies, lastSeen
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		received := time.Now()
		for range batch {
			if v, ok := sentAt.Load(lastSeen); ok {
				latencies = append(latencies, received.Sub(v.(time.Time)))
			}
			lastSeen++
			receivedTotal.Add(1)
		}
	}
	return latencies, lastSeen
}

// historyPerMsg renders a per-message history figure, keeping a workload with
// no workflow distinct from one that measured zero.
func (r streamBaselineResult) historyPerMsg(v int64) string {
	if r.historyNotApplicable {
		return "no workflow"
	}
	if r.messagesSent == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2f", float64(v)/float64(r.messagesSent))
}

// waitForDrain polls until every consumer has caught up or the deadline passes.
// It reports rather than asserts, because a cell that cannot drain is a result.
func waitForDrain(ctx context.Context, got *atomic.Int64, want int64, timeout time.Duration) bool {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(timeout)
	for {
		if got.Load() >= want {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			return false
		case <-ticker.C:
		}
	}
}

func recordingCount(rec *metricstest.CapturedRecording) int64 {
	switch v := rec.Value.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func percentile(d []time.Duration, q float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(d))
	copy(sorted, d)
	slices.Sort(sorted)
	idx := int(float64(len(sorted)-1) * q)
	return sorted[idx]
}

func reportStreamBaseline(t *testing.T, results []streamBaselineResult) {
	// Emitted as markdown so the numbers can go straight into the design docs
	// without being retyped, which is how transcription errors get in.
	t.Log("Workflow Streams baseline: Signals in, polling Update out")
	t.Log("")
	t.Log("| scenario | msgs | delivered | rejected polls | hist events/msg | hist bytes/msg | persist ops/msg | p50 | p99 |")
	t.Log("|---|---|---|---|---|---|---|---|---|")
	for _, r := range results {
		perMsg := func(v int64) string {
			if r.messagesSent == 0 {
				return "n/a"
			}
			return fmt.Sprintf("%.2f", float64(v)/float64(r.messagesSent))
		}
		t.Logf("| %s | %d | %d | %d | %s | %s | %s | %s | %s |",
			r.params.name, r.messagesSent, r.messagesReceived, r.pollRejections,
			r.historyPerMsg(r.historyEvents), r.historyPerMsg(r.historyBytes),
			perMsg(r.persistenceRequests),
			r.latencyP50.Round(time.Millisecond), r.latencyP99.Round(time.Millisecond))
	}
	t.Log("")
	for _, r := range results {
		if r.failure != "" {
			t.Logf("%s did not complete: %s", r.params.name, r.failure)
		}
		if r.messagesSent > 0 && r.messagesReceived < r.messagesSent*r.params.subscribers {
			t.Logf("%s under-delivered: %d of %d expected",
				r.params.name, r.messagesReceived, r.messagesSent*r.params.subscribers)
		}
	}
	t.Log("Totals are absolute, not rates: persist ops exclude cluster and workflow start.")
	t.Log("Rejected polls are bounded by history.maxInFlightUpdates (default 10) and")
	t.Log("history.maxTotalUpdates (default 2000), both per workflow execution.")
	for _, r := range results {
		t.Logf("%s raw: events=%d bytes=%d persistOps=%d byOp=%v",
			r.params.name, r.historyEvents, r.historyBytes, r.persistenceRequests, r.persistenceByOp)
	}
}
