package tests

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/tests/testcore"
)

func TestSDKValidationServerHost(t *testing.T) {
	readyPath := os.Getenv("AI198_SDK_SERVER_READY_FILE")
	stopPath := os.Getenv("AI198_SDK_SERVER_STOP_FILE")
	if readyPath == "" || stopPath == "" {
		t.Skip("SDK validation host needs explicit ready and shutdown paths")
	}
	_, err := os.Stat(stopPath)
	require.ErrorIs(t, err, os.ErrNotExist, "a stale shutdown file must not silently stop this host")
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = env.RegisterNamespace(ctx, namespace.Name("default"), 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	require.NoError(t, err)
	config := env.GetTestClusterConfig()
	metadata := map[string]any{
		"status": "running", "pid": os.Getpid(), "target": env.FrontendGRPCAddress(),
		"namespace": "default", "persistence": "SQLite", "history_hosts": config.HistoryConfig.NumHistoryHosts,
		"history_shards": config.HistoryConfig.NumHistoryShards, "started_utc": time.Now().UTC().Format(time.RFC3339),
		"shutdown_file": stopPath, "source_provenance": "../server-source-provenance.json",
		"configuration": "tests/testcore dedicated test-cluster defaults; namespace default added for SDK clients",
	}
	writeMetadata := func() {
		data, marshalErr := json.MarshalIndent(metadata, "", "  ")
		require.NoError(t, marshalErr)
		require.NoError(t, os.WriteFile(readyPath, append(data, '\n'), 0o644))
	}
	writeMetadata()
	t.Logf("SDK validation server ready at %s in namespace default; shutdown file %s", env.FrontendGRPCAddress(), stopPath)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(2 * time.Hour)
	defer deadline.Stop()
	for {
		select {
		case <-ticker.C:
			if _, statErr := os.Stat(stopPath); statErr == nil {
				metadata["status"] = "stopping"
				metadata["stopped_utc"] = time.Now().UTC().Format(time.RFC3339)
				writeMetadata()
				return
			} else if !errors.Is(statErr, os.ErrNotExist) {
				require.NoError(t, statErr)
			}
		case <-deadline.C:
			metadata["status"] = "maximum-runtime-reached"
			writeMetadata()
			return
		}
	}
}
