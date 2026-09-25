package xdc

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

// streamFailoverBase drives one workflow that owns a stream with records and
// an active subscription on the active cluster, so each suite below can fail
// the namespace over and look at what the standby has.
type streamFailoverBase struct {
	xdcBaseSuite
}

// StreamFailoverSuite runs with state-based replication, the mode that carries
// a CHASM tree between clusters.
type StreamFailoverSuite struct {
	streamFailoverBase
}

// StreamEventReplicationSuite runs with event-based replication, where only
// what History events carry reaches the standby.
type StreamEventReplicationSuite struct {
	streamFailoverBase
}

func TestStreamFailoverSuite(t *testing.T) {
	t.Parallel()
	s := &StreamFailoverSuite{}
	s.enableTransitionHistory = true
	suite.Run(t, s)
}

func TestStreamEventReplicationSuite(t *testing.T) {
	t.Parallel()
	s := &StreamEventReplicationSuite{}
	s.enableTransitionHistory = false
	suite.Run(t, s)
}

func (s *streamFailoverBase) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableChasm.Key():                      true,
		dynamicconfig.TransferProcessorMaxPollInterval.Key(): 1 * time.Second,
	}
	s.setupSuite()
}

func (s *streamFailoverBase) SetupTest() {
	s.setupTest()
}

func (s *streamFailoverBase) TearDownSuite() {
	s.tearDownSuite()
}

func (s *streamFailoverBase) streamClient(clusterIndex int) streamlib.StreamServiceClient {
	conn, err := grpc.NewClient(s.clusters[clusterIndex].Host().FrontendGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.NoError(err)
	s.T().Cleanup(func() { _ = conn.Close() })
	return streamlib.NewStreamServiceClient(conn)
}

// streamNodesOn lists the CHASM node paths of the execution on one cluster that
// belong to its streams and cursors, sorted, or nil when the execution is not
// there. A stream a workflow owns is `Streams#<name>`, its records are data
// nodes under `Streams$<name>$Batches#<offset>`, and the subscription is
// `StreamCursors#<name>`.
func (s *streamFailoverBase) streamNodesOn(
	ctx context.Context, clusterIndex int, ns string, execution *commonpb.WorkflowExecution,
) []string {
	resp, err := s.clusters[clusterIndex].AdminClient().DescribeMutableState(ctx,
		&adminservice.DescribeMutableStateRequest{Namespace: ns, Execution: execution})
	if err != nil {
		return nil
	}
	var paths []string
	for path := range resp.GetDatabaseMutableState().GetChasmNodes() {
		if strings.Contains(path, "Stream") {
			paths = append(paths, path)
		}
	}
	slices.Sort(paths)
	return paths
}

func apiBodies(records []*streampb.StreamRecord) []string {
	out := make([]string, 0, len(records))
	for _, r := range records {
		out = append(out, string(r.GetBody().GetData()))
	}
	return out
}

func storedBodies(records []*streamlib.StreamRecord) []string {
	out := make([]string, 0, len(records))
	for _, r := range records {
		out = append(out, string(r.GetBody().GetData()))
	}
	return out
}

func publishCommand(bodies ...string) []*commandpb.Command {
	records := make([]*streampb.StreamRecord, len(bodies))
	for i, b := range bodies {
		records[i] = &streampb.StreamRecord{Body: &commonpb.Payload{Data: []byte(b)}}
	}
	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_APPEND_STREAM_RECORDS,
		Attributes: &commandpb.Command_AppendStreamRecordsCommandAttributes{
			AppendStreamRecordsCommandAttributes: &commandpb.AppendStreamRecordsCommandAttributes{
				Records: records,
			},
		},
	}}
}

// streamingWorkflow is one execution driven by a raw poller on whichever
// cluster is asked, handing each task the commands queued for it.
type streamingWorkflow struct {
	s         *streamFailoverBase
	ns        string
	execution *commonpb.WorkflowExecution
	tq        *taskqueuepb.TaskQueue

	delivered [][]*streampb.StreamSlice
	commands  [][]*commandpb.Command
}

func (w *streamingWorkflow) runTask(
	clusterIndex int, cmds []*commandpb.Command,
) []*streampb.StreamSlice {
	w.commands = append(w.commands, cmds)
	//nolint:staticcheck // SA1019: only the deprecated poller can emit this command type.
	poller := &testcore.TaskPoller{
		Client:    w.s.clusters[clusterIndex].FrontendClient(),
		Namespace: w.ns,
		TaskQueue: w.tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			resp *workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			w.delivered = append(w.delivered, resp.GetStreamSlices())
			next := w.commands[0]
			w.commands = w.commands[1:]
			return next, nil
		},
		Logger: w.s.logger,
		T:      w.s.T(),
	}
	_, err := poller.PollAndProcessWorkflowTask()
	w.s.NoError(err)
	return w.delivered[len(w.delivered)-1]
}

func (w *streamingWorkflow) currentSlice(delivered []*streampb.StreamSlice) *streampb.StreamSlice {
	for _, sl := range delivered {
		if sl.GetWorkflowTaskCompletedEventId() == 0 {
			return sl
		}
	}
	w.s.FailNow("no slice for the current task")
	return nil
}

func (w *streamingWorkflow) signal(ctx context.Context, clusterIndex int) {
	_, err := w.s.clusters[clusterIndex].FrontendClient().SignalWorkflowExecution(ctx,
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace: w.ns, WorkflowExecution: w.execution,
			SignalName: "wake", Identity: "tester", RequestId: uuid.NewString(),
		})
	w.s.NoError(err)
}

// startStreamingWorkflow runs a workflow on the active cluster through one
// publish of three records, a subscription to that stream and the task that
// consumes them, then returns it with the stream nodes the active cluster holds.
func (s *streamFailoverBase) startStreamingWorkflow(
	ctx context.Context, ns string,
) (*streamingWorkflow, []string) {
	id := "stream-failover-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	we, err := s.clusters[0].FrontendClient().StartWorkflowExecution(ctx,
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           ns,
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: "stream-consumer"},
			TaskQueue:           tq,
			WorkflowRunTimeout:  durationpb.New(300 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            "tester",
		})
	s.NoError(err)
	w := &streamingWorkflow{
		s:         s,
		ns:        ns,
		execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.GetRunId()},
		tq:        tq,
	}

	w.runTask(0, publishCommand("a", "b", "c"))
	_, err = s.streamClient(0).SubscribeWorkflow(ctx, &streamlib.SubscribeWorkflowRequest{
		FrontendRequest: &streamlib.SubscribeWorkflowInput{
			Namespace: ns, WorkflowId: id, StreamName: chasmworkflow.DefaultStreamName, StartOffset: 0,
		},
	})
	s.NoError(err)
	consumed := w.currentSlice(w.runTask(0, nil))
	s.Equal([]string{"a", "b", "c"}, apiBodies(consumed.GetRecords()))

	activeNodes := s.streamNodesOn(ctx, 0, ns, w.execution)
	s.Equal([]string{
		"StreamCursors", "StreamCursors#output",
		"Streams", "Streams#output", "Streams$output$Batches", "Streams$output$Batches#0",
	}, activeNodes, "the active cluster holds the stream, its one batch and the cursor")
	return w, activeNodes
}

// With state-based replication the stream follows the namespace to the
// standby with its records, its consumer state and the subscription, and the
// workflow keeps consuming and publishing there.
func (s *StreamFailoverSuite) TestStreamFollowsTheNamespaceToTheStandby() {
	ns := s.createGlobalNamespace()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	w, activeNodes := s.startStreamingWorkflow(ctx, ns)

	// The standby holds the same tree before the namespace moves.
	await.Require(ctx, s.T(), func(t *await.T) {
		require.Equal(t, activeNodes, s.streamNodesOn(ctx, 1, ns, w.execution),
			"the standby's CHASM tree must carry the stream, its batches and the cursor")
	}, replicationWaitTime, replicationCheckInterval)

	s.failover(ns, 0, s.clusters[1].ClusterName(), 2)

	// The records and the frontier are readable on the new active cluster.
	described, err := s.streamClient(1).DescribeWorkflowStream(ctx,
		&streamlib.DescribeWorkflowStreamRequest{
			FrontendRequest: &streamlib.DescribeWorkflowStreamInput{
				Namespace: ns, WorkflowId: w.execution.GetWorkflowId(),
			},
		})
	s.NoError(err)
	s.Equal(int64(3), described.GetFrontendResponse().GetState().GetHeadOffset())

	polled, err := s.streamClient(1).PollWorkflowMessages(ctx, &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: ns, WorkflowId: w.execution.GetWorkflowId(), FromOffset: 0,
		},
	})
	s.NoError(err)
	s.Equal([]string{"a", "b", "c"}, storedBodies(polled.GetFrontendResponse().GetRecords()))

	// The workflow goes on consuming and publishing there. Its first task on
	// the new cluster is a cold one, so the range it consumed before the
	// failover is re-supplied from the replicated stream.
	w.signal(ctx, 1)
	afterFailover := w.runTask(1, publishCommand("d"))
	var replayed *streampb.StreamSlice
	for _, sl := range afterFailover {
		if sl.GetWorkflowTaskCompletedEventId() != 0 && sl.GetToOffset() > sl.GetFromOffset() {
			replayed = sl
		}
	}
	s.NotNil(replayed, "the cold task on the new cluster re-supplies the consumed range")
	s.Equal([]string{"a", "b", "c"}, apiBodies(replayed.GetRecords()))
	s.Equal(int64(3), w.currentSlice(afterFailover).GetFromOffset())

	next := w.currentSlice(w.runTask(1, nil))
	s.Equal(int64(3), next.GetFromOffset())
	s.Equal(int64(4), next.GetToOffset())
	s.Equal([]string{"d"}, apiBodies(next.GetRecords()))

	polled, err = s.streamClient(1).PollWorkflowMessages(ctx, &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: ns, WorkflowId: w.execution.GetWorkflowId(), FromOffset: 0,
		},
	})
	s.NoError(err)
	s.Equal([]string{"a", "b", "c", "d"}, storedBodies(polled.GetFrontendResponse().GetRecords()))
}

// With event-based replication the standby rebuilds its state from History
// events, and the events carry the subscription and the offsets it consumed
// but never a record. So the standby ends up with the cursor and without the
// stream: the frontier reads as an unwritten stream, no record can be polled,
// and the workflow's first task there cannot find the stream its cursor names.
// Carrying the stream under this mode would take a replication task of its
// own for the component, the way state-based replication ships the CHASM
// tree; the appended event cannot carry it without putting payloads in History.
func (s *StreamEventReplicationSuite) TestStreamDoesNotReplicateUnderEventBasedReplicationYet() {
	ns := s.createGlobalNamespace()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	w, _ := s.startStreamingWorkflow(ctx, ns)

	// Every replication task has been acknowledged by the standby, so what it
	// holds now is what this mode carries.
	s.waitForClusterSynced()
	await.Require(ctx, s.T(), func(t *await.T) {
		require.Equal(t, []string{"StreamCursors", "StreamCursors#output"},
			s.streamNodesOn(ctx, 1, ns, w.execution),
			"the standby rebuilds the cursor from the recorded ranges and nothing of the stream")
	}, replicationWaitTime, replicationCheckInterval)

	s.failover(ns, 0, s.clusters[1].ClusterName(), 2)

	described, err := s.streamClient(1).DescribeWorkflowStream(ctx,
		&streamlib.DescribeWorkflowStreamRequest{
			FrontendRequest: &streamlib.DescribeWorkflowStreamInput{
				Namespace: ns, WorkflowId: w.execution.GetWorkflowId(),
			},
		})
	s.NoError(err)
	s.Equal(int64(0), described.GetFrontendResponse().GetState().GetHeadOffset(),
		"the new active cluster sees a stream nothing has written to")

	polled, err := s.streamClient(1).PollWorkflowMessages(ctx, &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: ns, WorkflowId: w.execution.GetWorkflowId(), FromOffset: 0,
		},
	})
	s.NoError(err)
	s.Empty(polled.GetFrontendResponse().GetRecords(), "the records did not follow the namespace")
}
