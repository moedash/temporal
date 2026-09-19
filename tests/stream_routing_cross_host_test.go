package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// crossHostTopology is a two-host History cluster with the shards it was
// confirmed to split, plus the consumer and source shards each case uses.
type crossHostTopology struct {
	cluster       *testcore.TestCluster
	config        *testcore.TestClusterConfig
	ns            string
	nsID          string
	consumerHost  string
	remoteHost    string
	consumerShard int32
	localSource   int32
	remoteSource  int32
}

// A consumer on one History host reading a stream that lives on another. Every
// step that spans the two executions has to route rather than resolve the far
// shard locally: registering the pin, being told the frontier moved, reading
// the payload for the live task, and re-reading it for a cold replay.
//
// The subscription is made two ways, over the RPC and from the workflow's own
// command, because they take different paths to the stream's shard.
func TestStreamRoutedCrossHostDeliveryAndReplay(t *testing.T) {
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	topo := newCrossHostTopology(t, logger)
	artifacts := newRoutingArtifacts(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	artifacts.writeJSON("topology", topo.shardsByHost(ctx, t))

	frontend := topo.cluster.FrontendClient()
	conn, err := grpc.NewClient(topo.cluster.Host().FrontendGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	streams := streamlib.NewStreamServiceClient(conn)

	ownerConn, err := grpc.NewClient(topo.consumerHost,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ownerConn.Close()) })
	owner := historyservice.NewHistoryServiceClient(ownerConn)

	cases := []struct {
		name       string
		crossHost  bool
		viaCommand bool
	}{
		{name: "same-host", crossHost: false},
		{name: "cross-host", crossHost: true},
		{name: "cross-host-command", crossHost: true, viaCommand: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := topo.idOnShard(t, tc.name+"-consumer", topo.consumerShard)
			source := topo.idOnShard(t, tc.name+"-source", topo.sourceShard(tc.crossHost))
			_, err := streams.CreateStream(ctx, &streamlib.CreateStreamRequest{
				FrontendRequest: &streamlib.CreateStreamInput{Namespace: topo.ns, StreamId: source},
			})
			require.NoError(t, err)

			tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
			started, err := frontend.StartWorkflowExecution(
				ctx,
				&workflowservice.StartWorkflowExecutionRequest{
					RequestId:           uuid.NewString(),
					Namespace:           topo.ns,
					WorkflowId:          id,
					WorkflowType:        &commonpb.WorkflowType{Name: "cross-host-consumer"},
					TaskQueue:           tq,
					WorkflowRunTimeout:  durationpb.New(2 * time.Minute),
					WorkflowTaskTimeout: durationpb.New(10 * time.Second),
				},
			)
			require.NoError(t, err)
			execution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: started.GetRunId()}

			var delivered [][]*streampb.StreamSlice
			//nolint:staticcheck // SA1019: this test isolates the server response from SDK replay.
			poller := &testcore.TaskPoller{
				Client:    frontend,
				Namespace: topo.ns,
				TaskQueue: tq,
				Identity:  "cross-host-validation",
				Logger:    logger,
				T:         t,
				WorkflowTaskHandler: func(
					resp *workflowservice.PollWorkflowTaskQueueResponse,
				) ([]*commandpb.Command, error) {
					delivered = append(delivered, resp.GetStreamSlices())
					if tc.viaCommand && len(delivered) == 1 {
						return subscribeCommand(source), nil
					}
					return nil, nil
				},
			}
			_, err = poller.PollAndProcessWorkflowTask()
			require.NoError(t, err)

			topo.requireOwner(ctx, t, id, topo.consumerHost)
			topo.requireOwner(ctx, t, source, topo.hostFor(tc.crossHost))

			if !tc.viaCommand {
				_, err = streams.SubscribeWorkflow(ctx, &streamlib.SubscribeWorkflowRequest{
					FrontendRequest: &streamlib.SubscribeWorkflowInput{
						Namespace: topo.ns, WorkflowId: id, StreamId: source,
					},
				})
				require.NoError(t, err)
			}
			// Either way the pin has to have landed on the stream's own shard.
			desc, err := streams.DescribeStream(ctx, &streamlib.DescribeStreamRequest{
				FrontendRequest: &streamlib.DescribeStreamInput{Namespace: topo.ns, StreamId: source},
			})
			require.NoError(t, err)
			require.Len(t, desc.GetFrontendResponse().GetState().GetConsumers(), 1)

			_, err = streams.AddMessages(ctx, &streamlib.AddMessagesRequest{
				FrontendRequest: &streamlib.AddMessagesInput{
					Namespace: topo.ns, StreamId: source,
					Messages: streamMsgs("tokens", "retained-input"),
				},
			})
			require.NoError(t, err)

			history := func() []*historypb.HistoryEvent {
				return topo.history(ctx, t, execution)
			}
			latestScheduled := func() int64 {
				var scheduled int64
				for _, event := range history() {
					if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
						scheduled = event.GetEventId()
					}
				}
				return scheduled
			}
			// The append alone has to produce the task, through the routed push.
			await.RequireTrue(t, func() bool { return latestScheduled() > 2 },
				10*time.Second, 20*time.Millisecond)

			_, err = poller.PollAndProcessWorkflowTask()
			require.NoError(t, err)
			live := currentSlice(t, delivered[1])
			require.Equal(t, "retained-input", string(live.GetMessages()[0].GetBody().GetData()))
			artifacts.writeProto(tc.name+"-live-slice", live)
			artifacts.writeProto(tc.name+"-committed-history", &historypb.History{Events: history()})
			consumedAt := completedEventWithCursors(t, history())

			// Evict the consumer, then start its next task by hand so the cold
			// response can be inspected before matching hands it to a poller.
			_, err = owner.CloseShard(ctx, &historyservice.CloseShardRequest{ShardId: topo.consumerShard})
			require.NoError(t, err)
			_, err = frontend.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
				Namespace:         topo.ns,
				WorkflowExecution: execution,
				SignalName:        "cold-replay-boundary",
				RequestId:         uuid.NewString(),
			})
			require.NoError(t, err)
			response, err := owner.RecordWorkflowTaskStarted(
				ctx,
				&historyservice.RecordWorkflowTaskStartedRequest{
					NamespaceId:       topo.nsID,
					WorkflowExecution: execution,
					ScheduledEventId:  latestScheduled(),
					RequestId:         uuid.NewString(),
					PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
						Namespace: topo.ns, TaskQueue: tq, Identity: "cross-host-validation",
					},
				},
			)
			require.NoError(t, err)
			artifacts.writeProto(tc.name+"-cold-response", response)

			replayed := sliceForEvent(response.GetStreamSlices(), consumedAt)
			require.NotNil(t, replayed, "cold replay must re-supply the committed input range")
			require.Equal(t, "retained-input", string(replayed.GetMessages()[0].GetBody().GetData()))
		})
	}
}

func subscribeCommand(streamID string) []*commandpb.Command {
	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SUBSCRIBE_STREAM,
		Attributes: &commandpb.Command_SubscribeStreamCommandAttributes{
			SubscribeStreamCommandAttributes: &commandpb.SubscribeStreamCommandAttributes{
				StreamId: streamID, StartOffset: 0,
			},
		},
	}}
}

func newCrossHostTopology(t *testing.T, logger *testlogger.TestLogger) *crossHostTopology {
	t.Helper()
	config := &testcore.TestClusterConfig{
		HistoryConfig: testcore.HistoryConfig{NumHistoryHosts: 2, NumHistoryShards: 32},
		Persistence:   testcore.GetPersistenceTestDefaults(),
		WorkerConfig:  testcore.WorkerConfig{DisableWorker: true},
	}
	cluster, err := testcore.NewTestClusterFactory().NewCluster(t, config, logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cluster.TearDownCluster()) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	frontend := cluster.FrontendClient()
	ns := "validation-cross-host-" + uuid.NewString()
	_, err = frontend.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: ns, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	require.NoError(t, err)
	described, err := frontend.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	})
	require.NoError(t, err)

	topo := &crossHostTopology{
		cluster: cluster,
		config:  config,
		ns:      ns,
		nsID:    described.GetNamespaceInfo().GetId(),
	}
	hosts := topo.shardsByHost(ctx, t)
	require.Len(t, hosts, 2, "must verify two actual owners, not merely configure two services")
	for host, shards := range hosts {
		if topo.consumerHost == "" && len(shards) >= 2 {
			topo.consumerHost, topo.consumerShard, topo.localSource = host, shards[0], shards[1]
		}
	}
	require.NotEmpty(t, topo.consumerHost)
	for host, shards := range hosts {
		if host != topo.consumerHost {
			topo.remoteHost, topo.remoteSource = host, shards[0]
		}
	}
	t.Logf("consumer shard %d on %s, local source shard %d, remote source shard %d on %s",
		topo.consumerShard, topo.consumerHost, topo.localSource, topo.remoteSource, topo.remoteHost)
	return topo
}

// shardsByHost asks the admin service who owns every shard.
func (topo *crossHostTopology) shardsByHost(ctx context.Context, t *testing.T) map[string][]int32 {
	t.Helper()
	hosts := map[string][]int32{}
	for shard := int32(1); shard <= topo.config.HistoryConfig.NumHistoryShards; shard++ {
		host, err := topo.cluster.AdminClient().DescribeHistoryHost(ctx,
			&adminservice.DescribeHistoryHostRequest{ShardId: shard})
		require.NoError(t, err)
		require.NotEmpty(t, host.GetAddress())
		hosts[host.GetAddress()] = append(hosts[host.GetAddress()], shard)
	}
	return hosts
}

func (topo *crossHostTopology) hostFor(crossHost bool) string {
	if crossHost {
		return topo.remoteHost
	}
	return topo.consumerHost
}

func (topo *crossHostTopology) sourceShard(crossHost bool) int32 {
	if crossHost {
		return topo.remoteSource
	}
	return topo.localSource
}

// idOnShard finds a business id that hashes to the wanted shard.
func (topo *crossHostTopology) idOnShard(t *testing.T, prefix string, target int32) string {
	t.Helper()
	shards := topo.config.HistoryConfig.NumHistoryShards
	for i := range 10000 {
		id := fmt.Sprintf("%s-%d", prefix, i)
		if common.WorkflowIDToHistoryShard(topo.nsID, id, shards) == target {
			return id
		}
	}
	t.Fatal("could not select an id on the target shard")
	return ""
}

func (topo *crossHostTopology) requireOwner(
	ctx context.Context, t *testing.T, businessID string, host string,
) {
	t.Helper()
	desc, err := topo.cluster.AdminClient().DescribeHistoryHost(
		ctx,
		&adminservice.DescribeHistoryHostRequest{
			Namespace:         topo.ns,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: businessID},
		},
	)
	require.NoError(t, err)
	require.Equal(t, host, desc.GetAddress())
}

func (topo *crossHostTopology) history(
	ctx context.Context, t *testing.T, execution *commonpb.WorkflowExecution,
) []*historypb.HistoryEvent {
	t.Helper()
	resp, err := topo.cluster.FrontendClient().GetWorkflowExecutionHistory(ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: topo.ns, Execution: execution})
	require.NoError(t, err)
	require.Empty(t, resp.GetNextPageToken())
	return resp.GetHistory().GetEvents()
}

// routingArtifacts writes the responses this test observed to a directory, for
// the SDK validation runs that compare them against what a worker receives. Off
// unless the measurement switch is set, like the other evidence collectors.
type routingArtifacts struct {
	t   *testing.T
	dir string
}

func newRoutingArtifacts(t *testing.T) *routingArtifacts {
	t.Helper()
	if os.Getenv("TEMPORAL_STREAM_BENCH") != "1" {
		return &routingArtifacts{t: t}
	}
	dir := os.Getenv("AI198_ROUTING_RESULT_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	require.NoError(t, os.MkdirAll(dir, 0o755))
	return &routingArtifacts{t: t, dir: dir}
}

func (a *routingArtifacts) writeJSON(name string, value any) {
	if a.dir == "" {
		return
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	require.NoError(a.t, err)
	require.NoError(a.t, os.WriteFile(filepath.Join(a.dir, name+".json"), encoded, 0o644))
}

func (a *routingArtifacts) writeProto(name string, message proto.Message) {
	if a.dir == "" {
		return
	}
	encoded, err := protojson.MarshalOptions{Indent: "  "}.Marshal(message)
	require.NoError(a.t, err)
	require.NoError(a.t, os.WriteFile(filepath.Join(a.dir, name+".json"), encoded, 0o644))
	binary, err := proto.Marshal(message)
	require.NoError(a.t, err)
	require.NoError(a.t, os.WriteFile(filepath.Join(a.dir, name+".pb"), binary, 0o644))
}
