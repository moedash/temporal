package frontend

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// The execution a channel call names settles to a type the history service can
// route on: what the caller said, what the HTTP route said, or a workflow.
func TestResolveChannelExecution(t *testing.T) {
	runID := uuid.NewString()
	cases := []struct {
		name string
		in   *commonpb.Execution
		hint string
		want enumspb.ExecutionType
		err  bool
	}{
		{name: "unset is independent", in: nil},
		{
			name: "workflow stays",
			in:   &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_WORKFLOW, BusinessId: "wf"},
			hint: executionTypeHintActivity,
			want: enumspb.EXECUTION_TYPE_WORKFLOW,
		},
		{
			name: "activity stays",
			in:   &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: "act", RunId: runID},
			want: enumspb.EXECUTION_TYPE_ACTIVITY,
		},
		{
			name: "unspecified with no hint is a workflow",
			in:   &commonpb.Execution{BusinessId: "wf"},
			want: enumspb.EXECUTION_TYPE_WORKFLOW,
		},
		{
			name: "unspecified on the workflow route",
			in:   &commonpb.Execution{BusinessId: "wf"},
			hint: executionTypeHintWorkflow,
			want: enumspb.EXECUTION_TYPE_WORKFLOW,
		},
		{
			name: "unspecified on the activity route",
			in:   &commonpb.Execution{BusinessId: "act"},
			hint: executionTypeHintActivity,
			want: enumspb.EXECUTION_TYPE_ACTIVITY,
		},
		{
			name: "nexus operation is refused",
			in:   &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_NEXUS_OPERATION, BusinessId: "op"},
			err:  true,
		},
		{name: "no business id", in: &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_WORKFLOW}, err: true},
		{
			name: "run id must be a uuid",
			in:   &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_WORKFLOW, BusinessId: "wf", RunId: "r"},
			err:  true,
		},
		{name: "business id too long", in: &commonpb.Execution{BusinessId: "0123456789abcdef"}, err: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.in.GetType()
			got, err := resolveChannelExecution(tc.in, tc.hint, 12)
			if tc.err {
				var invalid *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalid)
				return
			}
			require.NoError(t, err)
			if tc.in == nil {
				require.Nil(t, got)
				return
			}
			require.Equal(t, tc.want, got.GetType())
			require.Equal(t, tc.in.GetBusinessId(), got.GetBusinessId())
			require.Equal(t, tc.in.GetRunId(), got.GetRunId())
			require.Equal(t, before, tc.in.GetType(), "the caller's message is left as it was")
		})
	}
}
