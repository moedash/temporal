package tests

import (
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

func completeWorkflowCommand() []*commandpb.Command {
	attrs := &commandpb.CompleteWorkflowExecutionCommandAttributes{}
	return []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: attrs,
		},
	}}
}
