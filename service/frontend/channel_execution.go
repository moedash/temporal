package frontend

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/metadata"
)

// A channel call names the execution that holds a linked channel as an
// Execution. The HTTP routes bind its business id from the path and cannot
// set the type, so the gateway tells the handler which kind of route the
// call came in on, and the handler fills the type in from that.

// executionTypeHintHeader is the metadata key the HTTP gateway sets from the
// matched route. Values are the route's resource segment.
const executionTypeHintHeader = "temporal-execution-type-hint"

const (
	executionTypeHintWorkflow = "workflows"
	executionTypeHintActivity = "activities"
)

var (
	errExecutionBusinessIDNotSet = serviceerror.NewInvalidArgument(
		"Execution business id is not set on request.")
	errExecutionBusinessIDTooLong = serviceerror.NewInvalidArgument(
		"Execution business id length exceeds limit.")
	errChannelNexusOperationUnsupported = serviceerror.NewInvalidArgument(
		"A channel linked to a Nexus operation is not supported.")
	errExecutionTypeUnknown = serviceerror.NewInvalidArgument("Unknown execution type.")
)

// executionTypeHintAnnotator reads the kind of execution an HTTP route named
// off the matched pattern, for the handler to fill the type in with.
func executionTypeHintAnnotator(ctx context.Context, _ *http.Request) metadata.MD {
	pattern, ok := runtime.HTTPPathPattern(ctx)
	if !ok {
		return nil
	}
	switch {
	case strings.Contains(pattern, "/"+executionTypeHintActivity+"/{"):
		return metadata.Pairs(executionTypeHintHeader, executionTypeHintActivity)
	case strings.Contains(pattern, "/"+executionTypeHintWorkflow+"/{"):
		return metadata.Pairs(executionTypeHintHeader, executionTypeHintWorkflow)
	}
	return nil
}

// executionTypeHint is what the gateway said about the route, or empty for a
// gRPC call.
func executionTypeHint(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get(executionTypeHintHeader); len(values) > 0 {
		return values[0]
	}
	return ""
}

// resolveChannelExecution checks the execution a channel call names and
// settles its type. An unset type is what the route said, and a workflow
// where nothing said anything, since a workflow is what the field named
// before activities could hold channels. A Nexus operation holds none yet.
func resolveChannelExecution(
	exec *commonpb.Execution,
	hint string,
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
		if hint == executionTypeHintActivity {
			resolved.Type = enumspb.EXECUTION_TYPE_ACTIVITY
		}
	case enumspb.EXECUTION_TYPE_WORKFLOW, enumspb.EXECUTION_TYPE_ACTIVITY:
	case enumspb.EXECUTION_TYPE_NEXUS_OPERATION:
		return nil, errChannelNexusOperationUnsupported
	default:
		return nil, errExecutionTypeUnknown
	}
	return resolved, nil
}
