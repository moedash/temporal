package respondworkflowtaskcompleted

import (
	"context"

	"go.temporal.io/server/chasm/lib/stream"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// flushStagedStreamAppends writes the log nodes staged by stream commands
// during this workflow task.
//
// Ordering is the whole point: nodes first, then the workflow task commit
// advances the stream's frontier as part of the workflow's own mutable state.
// Doing it the other way would publish offsets whose bytes are not yet durable.
// A crash in between leaves nodes at or past the frontier, which no reader can
// observe, and the retried task stages them again.
func flushStagedStreamAppends(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID string,
	staged []chasmworkflow.PendingStreamAppend,
) error {
	for _, p := range staged {
		if err := stream.WriteAppend(ctx, shardContext.GetExecutionManager(),
			shardContext.GetShardID(), namespaceID, p.CollectionID, p.Append); err != nil {
			return err
		}
	}
	return nil
}
