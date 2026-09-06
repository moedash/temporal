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

func TestStreamRoutedCrossHostDeliveryAndReplay(t *testing.T) {
	output := os.Getenv("AI198_ROUTING_RESULT_DIR")
	if output == "" {
		output = t.TempDir()
	}
	require.NoError(t, os.MkdirAll(output, 0o755))
	writeProto := func(name string, message proto.Message) {
		encoded, err := protojson.MarshalOptions{Indent: "  "}.Marshal(message)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(output, name+".json"), encoded, 0o644))
		binary, err := proto.Marshal(message)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(output, name+".pb"), binary, 0o644))
	}
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	config := &testcore.TestClusterConfig{
		HistoryConfig: testcore.HistoryConfig{NumHistoryHosts: 2, NumHistoryShards: 32},
		Persistence:   testcore.GetPersistenceTestDefaults(),
		WorkerConfig:  testcore.WorkerConfig{DisableWorker: true},
	}
	cluster, err := testcore.NewTestClusterFactory().NewCluster(t, config, logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cluster.TearDownCluster()) })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	frontend := cluster.FrontendClient()
	ns := "validation-cross-host-" + uuid.NewString()
	_, err = frontend.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: ns, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	require.NoError(t, err)
	namespace, err := frontend.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	require.NoError(t, err)
	nsID := namespace.GetNamespaceInfo().GetId()
	hosts := map[string][]int32{}
	for shard := int32(1); shard <= config.HistoryConfig.NumHistoryShards; shard++ {
		host, describeErr := cluster.AdminClient().DescribeHistoryHost(ctx, &adminservice.DescribeHistoryHostRequest{ShardId: shard})
		require.NoError(t, describeErr)
		require.NotEmpty(t, host.GetAddress())
		hosts[host.GetAddress()] = append(hosts[host.GetAddress()], shard)
	}
	require.Len(t, hosts, 2, "must verify two actual owners, not merely configure two services")
	topology, err := json.MarshalIndent(hosts, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(output, "topology.json"), topology, 0o644))
	var consumerHost, remoteHost string
	var consumerShard, localSourceShard, remoteSourceShard int32
	for host, shards := range hosts {
		if consumerHost == "" && len(shards) >= 2 {
			consumerHost, consumerShard, localSourceShard = host, shards[0], shards[1]
		}
	}
	require.NotEmpty(t, consumerHost)
	for host, shards := range hosts {
		if host != consumerHost {
			remoteHost, remoteSourceShard = host, shards[0]
		}
	}
	t.Logf("Confirmed history topology: %v; consumer shard %d on %s, local source shard %d on %s, remote source shard %d on %s",
		hosts, consumerShard, consumerHost, localSourceShard, consumerHost, remoteSourceShard, remoteHost)
	findID := func(prefix string, target int32) string {
		for i := 0; i < 10000; i++ {
			id := fmt.Sprintf("%s-%d", prefix, i)
			if common.WorkflowIDToHistoryShard(nsID, id, config.HistoryConfig.NumHistoryShards) == target {
				return id
			}
		}
		t.Fatal("could not select ID on target shard")
		return ""
	}
	conn, err := grpc.NewClient(cluster.Host().FrontendGRPCAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	streams := streamlib.NewStreamServiceClient(conn)
	ownerConn, err := grpc.NewClient(consumerHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ownerConn.Close()) })
	owner := historyservice.NewHistoryServiceClient(ownerConn)
	getHistory := func(execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
		history, getErr := frontend.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: ns, Execution: execution,
		})
		require.NoError(t, getErr)
		require.Empty(t, history.GetNextPageToken())
		return history.GetHistory().GetEvents()
	}
	for _, test := range []struct {
		name        string
		sourceShard int32
		crossHost   bool
	}{{"same-host", localSourceShard, false}, {"cross-host", remoteSourceShard, true}} {
		t.Run(test.name, func(t *testing.T) {
			id := findID(test.name+"-consumer", consumerShard)
			source := findID(test.name+"-source", test.sourceShard)
			_, err := streams.CreateStream(ctx, &streamlib.CreateStreamRequest{FrontendRequest: &streamlib.CreateStreamInput{Namespace: ns, StreamId: source}})
			require.NoError(t, err)
			tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
			started, err := frontend.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
				RequestId: uuid.NewString(), Namespace: ns, WorkflowId: id,
				WorkflowType: &commonpb.WorkflowType{Name: "cross-host-consumer"}, TaskQueue: tq,
				WorkflowRunTimeout: durationpb.New(2 * time.Minute), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			})
			require.NoError(t, err)
			execution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: started.GetRunId()}
			var delivered [][]*streampb.StreamSlice
			//nolint:staticcheck // SA1019: this test isolates the server response from SDK replay.
			poller := &testcore.TaskPoller{Client: frontend, Namespace: ns, TaskQueue: tq, Identity: "cross-host-validation", Logger: logger, T: t,
				WorkflowTaskHandler: func(resp *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
					delivered = append(delivered, resp.GetStreamSlices())
					return nil, nil
				},
			}
			_, err = poller.PollAndProcessWorkflowTask()
			require.NoError(t, err)
			for _, target := range []struct{ id, host string }{{id, consumerHost}, {source, map[bool]string{false: consumerHost, true: remoteHost}[test.crossHost]}} {
				desc, describeErr := cluster.AdminClient().DescribeHistoryHost(ctx, &adminservice.DescribeHistoryHostRequest{
					Namespace: ns, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: target.id},
				})
				require.NoError(t, describeErr)
				require.Equal(t, target.host, desc.GetAddress())
				t.Logf("Resolved %s to %s, loaded shards %v", target.id, desc.GetAddress(), desc.GetShardIds())
			}
			_, err = streams.SubscribeWorkflow(ctx, &streamlib.SubscribeWorkflowRequest{FrontendRequest: &streamlib.SubscribeWorkflowInput{
				Namespace: ns, WorkflowId: id, StreamId: source,
			}})
			require.NoError(t, err)
			_, err = streams.AddMessages(ctx, &streamlib.AddMessagesRequest{FrontendRequest: &streamlib.AddMessagesInput{
				Namespace: ns, StreamId: source, Messages: streamMsgs("tokens", "retained-input"),
			}})
			require.NoError(t, err)
			latestScheduled := func() int64 {
				var scheduled int64
				for _, event := range getHistory(execution) {
					if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
						scheduled = event.GetEventId()
					}
				}
				return scheduled
			}
			await.RequireTrue(t, func() bool { return latestScheduled() > 2 }, 10*time.Second, 20*time.Millisecond)
			{
				_, err = poller.PollAndProcessWorkflowTask()
				require.NoError(t, err)
				require.Equal(t, "retained-input", string(currentSlice(t, delivered[1]).GetMessages()[0].GetBody().GetData()))
				writeProto(test.name+"-live-slice", currentSlice(t, delivered[1]))
				writeProto(test.name+"-committed-history", &historypb.History{Events: getHistory(execution)})
				consumedAt := completedEventWithCursors(t, getHistory(execution))
				_, err = owner.CloseShard(ctx, &historyservice.CloseShardRequest{ShardId: consumerShard})
				require.NoError(t, err)
				_, err = frontend.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
					Namespace: ns, WorkflowExecution: execution, SignalName: "cold-replay-boundary", RequestId: uuid.NewString(),
				})
				require.NoError(t, err)
				response, err := owner.RecordWorkflowTaskStarted(ctx, &historyservice.RecordWorkflowTaskStartedRequest{
					NamespaceId: nsID, WorkflowExecution: execution, ScheduledEventId: latestScheduled(), RequestId: uuid.NewString(),
					PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{Namespace: ns, TaskQueue: tq, Identity: "cross-host-validation"},
				})
				require.NoError(t, err)
				writeProto(test.name+"-cold-response", response)
				found := false
				for _, slice := range response.GetStreamSlices() {
					if slice.GetWorkflowTaskCompletedEventId() == consumedAt {
						require.Equal(t, "retained-input", string(slice.GetMessages()[0].GetBody().GetData()))
						found = true
					}
				}
				require.True(t, found, "cold replay must re-supply the committed input range")
				t.Logf("%s delivered and committed the input, then re-supplied it after consumer CloseShard at completion %d; consumer=%s source=%s", test.name, consumedAt, consumerHost, map[bool]string{false: consumerHost, true: remoteHost}[test.crossHost])
				return
			}
		})
	}
}
