package frontend

import (
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// A channel call names the workflow that holds a linked channel as an
// Execution. The HTTP routes bind its business id from the path and cannot
// set the type, so an unset type is a workflow.

var (
	errExecutionBusinessIDNotSet = serviceerror.NewInvalidArgument(
		"Execution business id is not set on request.")
	errExecutionBusinessIDTooLong = serviceerror.NewInvalidArgument(
		"Execution business id length exceeds limit.")
	errChannelActivityUnsupported = serviceerror.NewInvalidArgument(
		"A channel linked to an activity is not supported.")
	errChannelNexusOperationUnsupported = serviceerror.NewInvalidArgument(
		"A channel linked to a Nexus operation is not supported.")
	errExecutionTypeUnknown = serviceerror.NewInvalidArgument("Unknown execution type.")
)

// resolveChannelExecution checks the execution a channel call names and
// settles its type. An unset type is a workflow, since a workflow is what
// holds linked channels. An activity or a Nexus operation holds none yet.
func resolveChannelExecution(
	exec *commonpb.Execution,
	maxIDLength int,
) (*commonpb.Execution, error) {
	if exec == nil {
		return nil, nil
	}
	if exec.GetBusinessId() == "" {
		return nil, errExecutionBusinessIDNotSet
	}
	if len(exec.GetBusinessId()) > maxIDLength {
		return nil, errExecutionBusinessIDTooLong
	}
	if exec.GetRunId() != "" {
		if err := uuid.Validate(exec.GetRunId()); err != nil {
			return nil, errInvalidRunID
		}
	}
	resolved := &commonpb.Execution{
		Type:       exec.GetType(),
		BusinessId: exec.GetBusinessId(),
		RunId:      exec.GetRunId(),
	}
	switch exec.GetType() {
	case enumspb.EXECUTION_TYPE_UNSPECIFIED:
		resolved.Type = enumspb.EXECUTION_TYPE_WORKFLOW
	case enumspb.EXECUTION_TYPE_WORKFLOW:
	case enumspb.EXECUTION_TYPE_ACTIVITY:
		return nil, errChannelActivityUnsupported
	case enumspb.EXECUTION_TYPE_NEXUS_OPERATION:
		return nil, errChannelNexusOperationUnsupported
	default:
		return nil, errExecutionTypeUnknown
	}
	return resolved, nil
}
