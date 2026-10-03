package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func signalWorkflow(t *testing.T, s *streamTestEnv, workflowID, runID string) {
	t.Helper()
	_, err := s.env.FrontendClient().SignalWorkflowExecution(
		s.ctx(),
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.ns,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			SignalName:        "wake",
			Identity:          "tester",
			RequestId:         uuid.NewString(),
		},
	)
	require.NoError(t, err)
}
