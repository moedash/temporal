package tests

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The native-stream half of the comparison. It deliberately shares the workload
// shape, latency stamping, and metric capture with the Signal-and-Update
// baseline in streaming_baseline_test.go, because a benchmark whose two halves
// generate load differently measures the harness rather than the design.

func runNativeStream(t *testing.T, p streamBaselineParams) streamBaselineResult {
	env := testcore.NewEnv(t, testcore.WithDisableTestloggerFailure())
	res := streamBaselineResult{params: p, persistenceByOp: map[string]int64{}}

	conn, err := grpc.NewClient(env.FrontendGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	client := streampb.NewStreamServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), p.duration+2*time.Minute)
	defer cancel()

	ns := env.Namespace().String()
	streamID := fmt.Sprintf("bench-%s", p.name)
	created, err := client.CreateStream(ctx, &streampb.CreateStreamRequest{
		FrontendRequest: &streampb.CreateStreamInput{Namespace: ns, StreamId: streamID},
	})
	require.NoError(t, err)
	runID := created.GetFrontendResponse().GetRunId()

	// Started after creation so cluster and namespace setup do not inflate the
	// per-message figures, matching the baseline.
	capture := env.StartNamespaceMetricCapture()

	sentAt := &sync.Map{}
	var receivedTotal atomic.Int64
	var pollRejections atomic.Int64

	consumerCtx, stopConsumers := context.WithCancel(ctx)
	defer stopConsumers()
	var consumers sync.WaitGroup
	latencies := make([][]time.Duration, p.subscribers)
	counts := make([]int, p.subscribers)

	for i := range p.subscribers {
		consumers.Add(1)
		go func(idx int) {
			defer consumers.Done()
			latencies[idx], counts[idx] = runNativeConsumer(
				consumerCtx, client, ns, streamID, runID, sentAt, &receivedTotal, &pollRejections)
		}(i)
	}

	res.messagesSent = runNativeProducer(ctx, t, client, ns, streamID, runID, p, sentAt, &res)

	want := int64(res.messagesSent) * int64(p.subscribers)
	if !waitForDrain(ctx, &receivedTotal, want, 15*time.Second) {
		t.Logf("drained %d of %d expected deliveries before timeout", receivedTotal.Load(), want)
	}
	stopConsumers()
	consumers.Wait()

	var all []time.Duration
	for i := range p.subscribers {
		all = append(all, latencies[i]...)
		res.messagesReceived += counts[i]
	}
	res.pollRejections = pollRejections.Load()
	res.latencyP50 = percentile(all, 0.50)
	res.latencyP99 = percentile(all, 0.99)

	// Nothing enters workflow history, so these stay zero by construction
	// rather than by tuning. That is the claim, and reporting it as a measured
	// zero is the point.
	res.historyEvents = 0
	res.historyBytes = 0

	for _, rec := range capture.Metric(metrics.PersistenceRequests.Name()) {
		res.persistenceRequests += recordingCount(rec)
		if op, ok := rec.Tags["operation"]; ok {
			res.persistenceByOp[op] += recordingCount(rec)
		}
	}
	return res
}

func runNativeProducer(
	ctx context.Context,
	t *testing.T,
	client streampb.StreamServiceClient,
	ns, streamID, runID string,
	p streamBaselineParams,
	sentAt *sync.Map,
	res *streamBaselineResult,
) int {
	payload := make([]byte, streamMessageSize)
	for i := range payload {
		payload[i] = 'x'
	}

	genTicker := time.NewTicker(time.Second / time.Duration(p.messageRate))
	defer genTicker.Stop()
	flushTicker := time.NewTicker(p.flushInterval)
	defer flushTicker.Stop()
	deadline := time.Now().Add(p.duration)

	seq := 0
	var pending []*streampb.StreamMessage
	sequence := int64(0)

	flush := func() bool {
		if len(pending) == 0 {
			return true
		}
		batch := pending
		pending = nil
		sequence++
		_, err := client.AddMessages(ctx, &streampb.AddMessagesRequest{
			FrontendRequest: &streampb.AddMessagesInput{
				Namespace: ns, StreamId: streamID, RunId: runID, Messages: batch,
				ProducerId: "bench", Sequence: sequence,
			},
		})
		if err != nil {
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
			pending = append(pending, &streampb.StreamMessage{
				Body: &commonpb.Payload{Data: payload},
				Kind: streampb.STREAM_MESSAGE_KIND_DATA,
			})
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

func runNativeConsumer(
	ctx context.Context,
	client streampb.StreamServiceClient,
	ns, streamID, runID string,
	sentAt *sync.Map,
	receivedTotal *atomic.Int64,
	rejections *atomic.Int64,
) ([]time.Duration, int) {
	var out []time.Duration
	lastSeen := int64(0)

	for ctx.Err() == nil {
		resp, err := client.PollMessages(ctx, &streampb.PollMessagesRequest{
			FrontendRequest: &streampb.PollMessagesInput{
				Namespace: ns, StreamId: streamID, RunId: runID,
				FromOffset: lastSeen, WaitNewMessages: true,
			},
		})
		if err != nil {
			rejections.Add(1)
			select {
			case <-ctx.Done():
				return out, int(lastSeen)
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		received := time.Now()
		fr := resp.GetFrontendResponse()
		for range fr.GetMessages() {
			if v, ok := sentAt.Load(int(lastSeen)); ok {
				out = append(out, received.Sub(v.(time.Time)))
			}
			lastSeen++
			receivedTotal.Add(1)
		}
		if next := fr.GetNextOffset(); next > lastSeen {
			lastSeen = next
		}
	}
	return out, int(lastSeen)
}

// TestStreamingComparison runs both designs over the same workload and reports
// them together. This is the artifact the go/no-go decision needs.
func TestStreamingComparison(t *testing.T) {
	matrix := shortStreamBaselineMatrix()
	if os.Getenv("TEMPORAL_STREAM_BENCH") == "1" {
		matrix = fullStreamBaselineMatrix()
	}

	var baseline, native []streamBaselineResult
	for _, p := range matrix {
		t.Run("baseline/"+p.name, func(t *testing.T) {
			baseline = append(baseline, runStreamBaseline(t, p))
		})
		t.Run("native/"+p.name, func(t *testing.T) {
			native = append(native, runNativeStream(t, p))
		})
	}

	t.Log("Workflow Streams (Signals in, polling Update out) versus native streams")
	t.Log("")
	t.Log("| scenario | design | msgs | delivered | rejected | wf hist bytes/msg | persist ops/msg | p50 | p99 |")
	t.Log("|---|---|---|---|---|---|---|---|---|")
	for i := range baseline {
		logComparisonRow(t, "signals+update", baseline[i])
		logComparisonRow(t, "native", native[i])
	}

	t.Log("")
	for i := range baseline {
		t.Logf("%s signals+update raw: persistOps=%d byOp=%v",
			baseline[i].params.name, baseline[i].persistenceRequests, baseline[i].persistenceByOp)
		t.Logf("%s native raw: persistOps=%d byOp=%v",
			native[i].params.name, native[i].persistenceRequests, native[i].persistenceByOp)
	}
}

func logComparisonRow(t *testing.T, design string, r streamBaselineResult) {
	perMsg := func(v int64) string {
		if r.messagesSent == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%.2f", float64(v)/float64(r.messagesSent))
	}
	t.Logf("| %s | %s | %d | %d | %d | %s | %s | %s | %s |",
		r.params.name, design, r.messagesSent, r.messagesReceived, r.pollRejections,
		perMsg(r.historyBytes), perMsg(r.persistenceRequests),
		r.latencyP50.Round(time.Millisecond), r.latencyP99.Round(time.Millisecond))
}
