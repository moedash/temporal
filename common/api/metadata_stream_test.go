package api_test

// An external test package, because the generated stream package depends on
// this one and an internal test importing it would be a cycle.

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/testing/temporalapi"
)

// The access each stream method declares. Kept as a table so a new method
// fails here until someone decides what it needs, and so a change of mind
// shows up in the diff.
var expectedStreamAccess = map[string]api.Access{
	"CreateStream":           api.AccessWrite,
	"AddMessages":            api.AccessWrite,
	"FinishWriting":          api.AccessWrite,
	"SubscribeWorkflow":      api.AccessWrite,
	"PollMessages":           api.AccessReadOnly,
	"DescribeStream":         api.AccessReadOnly,
	"PollWorkflowMessages":   api.AccessReadOnly,
	"DescribeWorkflowStream": api.AccessReadOnly,
	"AddWorkflowMessages":    api.AccessWrite,
	"RegisterStreamConsumer": api.AccessAdmin,
	"AdvanceConsumerHead":    api.AccessAdmin,
	"CloseStream":            api.AccessWrite,
	"TruncateStream":         api.AccessWrite,
	"ListStreams":            api.AccessReadOnly,
	"DeleteStream":           api.AccessWrite,
}

func TestStreamServiceMetadata(t *testing.T) {
	var server streampb.StreamServiceServer
	seen := make(map[string]struct{})
	temporalapi.WalkExportedMethods(&server, func(method reflect.Method) {
		seen[method.Name] = struct{}{}
		md := api.GetMethodMetadata(api.StreamServicePrefix + method.Name)

		expected, ok := expectedStreamAccess[method.Name]
		require.Truef(t, ok,
			"%s has no entry in expectedStreamAccess: decide its access and add it", method.Name)
		require.Equalf(t, expected, md.Access, "%s access", method.Name)
		require.Equalf(t, api.ScopeNamespace, md.Scope, "%s scope", method.Name)

		// Namespace scope is only enforceable if the interceptors can find the
		// namespace on the request, and these requests carry it through a method
		// rather than a top-level field.
		requestType := method.Type.In(1)
		getter, ok := requestType.MethodByName("GetNamespace")
		require.Truef(t, ok, "%s request has no GetNamespace()", method.Name)
		require.Equal(t, reflect.TypeFor[string](), getter.Type.Out(0))
	})

	for name := range expectedStreamAccess {
		_, ok := seen[name]
		require.Truef(t, ok, "%s is in expectedStreamAccess but not on the service", name)
	}
}

func TestStreamPollsAreMarkedAsPolling(t *testing.T) {
	for _, name := range []string{"PollMessages", "PollWorkflowMessages"} {
		md := api.GetMethodMetadata(api.StreamServicePrefix + name)
		require.Equalf(t, api.PollingCapable, md.Polling, "%s blocks only when asked to", name)
	}
	md := api.GetMethodMetadata(api.StreamServicePrefix + "DescribeStream")
	require.Equal(t, api.PollingNone, md.Polling)
}
