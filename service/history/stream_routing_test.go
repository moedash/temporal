package history

import (
	"testing"

	"github.com/stretchr/testify/require"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

type testStreamRoutingClient struct {
	streampb.StreamServiceClient
}

func TestWithStreamClient(t *testing.T) {
	client := &testStreamRoutingClient{}
	options := applyEngineOptions([]EngineOption{WithStreamClient(client)})
	require.Same(t, client, options.streamClient)
}
