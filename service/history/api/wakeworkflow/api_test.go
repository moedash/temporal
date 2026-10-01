package wakeworkflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

const (
	currentRunID = "11111111-1111-1111-1111-111111111111"
	firstRunID   = "22222222-2222-2222-2222-222222222222"
	otherRunID   = "33333333-3333-3333-3333-333333333333"
)

type wakeFixture struct {
	shard   *historyi.MockShardContext
	checker *api.MockWorkflowConsistencyChecker
	context *historyi.MockWorkflowContext
	current *historyi.MockMutableState
	wf      *chasmworkflow.Workflow
	chasm   chasm.MutableContext
}

func newWakeFixture(t *testing.T) *wakeFixture {
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespaceByID(tests.GlobalNamespaceEntry.ID()).
		Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	shard := historyi.NewMockShardContext(ctrl)
	shard.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	shard.EXPECT().GetLogger().Return(log.NewTestLogger()).AnyTimes()
	shard.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	shard.EXPECT().GetNamespaceRegistry().Return(registry).AnyTimes()
	shard.EXPECT().GetClusterMetadata().Return(
		clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))).AnyTimes()

	f := &wakeFixture{
		shard:   shard,
		checker: api.NewMockWorkflowConsistencyChecker(ctrl),
		context: historyi.NewMockWorkflowContext(ctrl),
		current: newRun(ctrl, currentRunID, firstRunID, true),
		wf:      &chasmworkflow.Workflow{},
		chasm: &chasm.MockMutableContext{MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Unix(1_700_000_000, 0) },
		}},
	}
	f.current.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	f.current.EXPECT().HasChasmWorkflowComponent().Return(true).AnyTimes()
	f.current.EXPECT().ChasmWorkflowComponentReadOnly(gomock.Any()).
		Return(f.wf, f.chasm, nil).AnyTimes()
	f.current.EXPECT().EnsureChasmWorkflowComponent(gomock.Any()).AnyTimes()
	f.current.EXPECT().ChasmWorkflowComponent(gomock.Any()).Return(f.wf, f.chasm, nil).AnyTimes()

	f.checker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(),
		runKey(""), gomock.Any()).
		Return(api.NewWorkflowLease(f.context, wcache.NoopReleaseFn, f.current), nil).AnyTimes()
	return f
}

func runKey(runID string) definition.WorkflowKey {
	return definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, runID)
}

func newRun(
	ctrl *gomock.Controller,
	runID string,
	first string,
	running bool,
) *historyi.MockMutableState {
	ms := historyi.NewMockMutableState(ctrl)
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId:          tests.WorkflowID,
		FirstExecutionRunId: first,
	}).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	ms.EXPECT().IsWorkflowExecutionRunning().Return(running).AnyTimes()
	return ms
}

func (f *wakeFixture) invoke(
	runID string,
	counter int64,
) (*historyservice.WakeWorkflowExecutionResponse, error) {
	return Invoke(
		context.Background(),
		&historyservice.WakeWorkflowExecutionRequest{
			NamespaceId: tests.NamespaceID.String(),
			WakeRequest: &workflowservice.WakeWorkflowExecutionRequest{
				Namespace: tests.Namespace.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: tests.WorkflowID,
					RunId:      runID,
				},
				Wake: &workflowpb.Wake{Source: "orders", Position: []byte("p"), Counter: counter},
			},
		},
		f.shard,
		f.checker,
	)
}

func TestWakeAcceptsAndPersists(t *testing.T) {
	f := newWakeFixture(t)
	f.context.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), f.shard).Return(nil)

	resp, err := f.invoke("", 3)
	require.NoError(t, err)
	require.False(t, resp.GetFolded())
	require.Equal(t, currentRunID, resp.GetRunId())
	require.True(t, f.wf.HasUndeliveredWakes(f.chasm))
}

// A pure duplicate persists nothing: no update call is expected on the context.
func TestWakeFoldsWithoutPersisting(t *testing.T) {
	f := newWakeFixture(t)
	_, err := f.wf.AcceptWake(f.chasm, "orders", nil, 5, 32)
	require.NoError(t, err)

	resp, err := f.invoke(firstRunID, 5)
	require.NoError(t, err)
	require.True(t, resp.GetFolded())
	require.Equal(t, currentRunID, resp.GetRunId())
}

func TestWakeToAClosedRunIsNotFound(t *testing.T) {
	f := newWakeFixture(t)
	closed := newRun(gomock.NewController(t), currentRunID, firstRunID, false)
	f.checker = api.NewMockWorkflowConsistencyChecker(gomock.NewController(t))
	f.checker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(api.NewWorkflowLease(f.context, wcache.NoopReleaseFn, closed), nil)

	_, err := f.invoke(currentRunID, 1)
	require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
}

// A run id that is neither the current run nor its chain's first run is
// looked up, and a run of another chain is not found.
func TestWakeToAnotherChainIsNotFound(t *testing.T) {
	f := newWakeFixture(t)
	other := newRun(gomock.NewController(t), otherRunID, otherRunID, false)
	f.checker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(),
		runKey(otherRunID), gomock.Any()).
		Return(api.NewWorkflowLease(f.context, wcache.NoopReleaseFn, other), nil)

	_, err := f.invoke(otherRunID, 1)
	require.ErrorIs(t, err, consts.ErrWorkflowExecutionNotFound)
}

// A middle run of the current chain is looked up and lands on the current run.
func TestWakeToAMiddleRunLandsOnTheCurrentRun(t *testing.T) {
	f := newWakeFixture(t)
	middle := newRun(gomock.NewController(t), otherRunID, firstRunID, false)
	f.checker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(),
		runKey(otherRunID), gomock.Any()).
		Return(api.NewWorkflowLease(f.context, wcache.NoopReleaseFn, middle), nil)
	f.context.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), f.shard).Return(nil)

	resp, err := f.invoke(otherRunID, 1)
	require.NoError(t, err)
	require.Equal(t, currentRunID, resp.GetRunId())
}

// A wake at the same counter as one a started task carried is a new wake:
// only the task knows what it read.
func TestWakeAfterDeliveryIsAccepted(t *testing.T) {
	f := newWakeFixture(t)
	_, err := f.wf.AcceptWake(f.chasm, "orders", nil, 5, 32)
	require.NoError(t, err)
	f.wf.TakeWakesForTask(f.chasm)
	f.context.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), f.shard).Return(nil)

	resp, err := f.invoke("", 5)
	require.NoError(t, err)
	require.False(t, resp.GetFolded())
	require.True(t, f.wf.HasUndeliveredWakes(f.chasm))
}
