package recordworkflowtaskstarted

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.uber.org/mock/gomock"
)

func completedWithRange(eventID int64, from, to int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
				ConsumedStreamRanges: []*streampb.StreamRange{
					{StreamId: "output", FromOffset: from, ToOffset: to},
				},
			},
		},
	}
}

func resetPoint(eventID int64, baseRunID string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
			WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
				Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
				BaseRunId: baseRunID,
				NewRunId:  "reset-run",
			},
		},
	}
}

// A reset copies the base run's history into the new run. The ranges recorded
// before the reset point were consumed from the base run's own stream, so
// replay has to read them from that run and say so on the slice, while the
// ranges recorded after it are the reset run's own. A reset of a reset run
// leaves two such points, one per run in the chain.
func TestReplayReadsRangesBeforeAResetFromTheRunThatHoldsThem(t *testing.T) {
	var readFrom []string
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			ref chasm.ComponentRef,
			_ func(chasm.Context, chasm.Component) error,
			_ ...chasm.TransitionOption,
		) error {
			require.Equal(t, "consumer", ref.BusinessID)
			readFrom = append(readFrom, ref.RunID)
			return nil
		}).AnyTimes()

	supply := &replaySupply{
		ctx:       chasm.NewEngineContext(context.Background(), engine),
		consumer:  definition.NewWorkflowKey("ns", "consumer", "reset-run"),
		addresses: map[string]streamOrigin{"output": {name: "output"}},
		reached:   map[string]int64{},
	}

	// First page: the oldest run's era ends with the first reset point.
	require.NoError(t, supply.collect([]*historypb.HistoryEvent{
		completedWithRange(4, 0, 2),
		completedWithRange(8, 2, 2),
		resetPoint(10, "first-run"),
	}))
	// Second page: the middle run's era, then the reset that made this run.
	require.NoError(t, supply.collect([]*historypb.HistoryEvent{
		completedWithRange(14, 2, 5),
		resetPoint(16, "second-run"),
		completedWithRange(20, 5, 5),
		completedWithRange(24, 5, 7),
	}))
	require.NoError(t, supply.finish())
	require.NoError(t, supply.checkCoverage())

	require.Equal(t, []string{"first-run", "second-run", "reset-run"}, readFrom,
		"every non-empty range is read from the run whose era recorded it")

	runs := make([]string, 0, len(supply.slices))
	events := make([]int64, 0, len(supply.slices))
	for _, slice := range supply.slices {
		runs = append(runs, slice.GetRunId())
		events = append(events, slice.GetWorkflowTaskCompletedEventId())
	}
	require.Equal(t, []int64{4, 8, 14, 20, 24}, events, "slices keep the order the tasks ran in")
	require.Equal(t, []string{"first-run", "first-run", "second-run", "reset-run", "reset-run"}, runs,
		"an empty range names its era's run too")
}

// Without a reset point every owned range is the consumer's own.
func TestReplayWithoutAResetReadsFromTheConsumer(t *testing.T) {
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			ref chasm.ComponentRef,
			_ func(chasm.Context, chasm.Component) error,
			_ ...chasm.TransitionOption,
		) error {
			require.Equal(t, "consumer-run", ref.RunID)
			return nil
		}).Times(1)

	supply := &replaySupply{
		ctx:       chasm.NewEngineContext(context.Background(), engine),
		consumer:  definition.NewWorkflowKey("ns", "consumer", "consumer-run"),
		addresses: map[string]streamOrigin{"output": {name: "output"}},
		reached:   map[string]int64{},
	}
	require.NoError(t, supply.collect([]*historypb.HistoryEvent{completedWithRange(4, 0, 3)}))
	require.NoError(t, supply.finish())
	require.Len(t, supply.slices, 1)
	require.Equal(t, "consumer-run", supply.slices[0].GetRunId())
}
