package stream

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
)

// A stream's payload bytes live in the history-node store, on branches of its
// own rather than on any workflow's. That store is already an offset-addressed,
// shard-fenced, forkable, trimmable append-only log, and its own interface
// describes it as decoupled from workflow concepts.
//
// It is not one branch per stream. The Cassandra table partitions on tree_id
// alone, which is safe for workflow history because history is capped and
// unsafe for a stream because it is not. So offsets roll to a new tree every
// bucketSize, and because the bucket is arithmetic and the tree ID is derived
// from it, there is no index to keep.

// streamLogNamespace anchors deterministic bucket tree IDs. Any fixed UUID
// works; it exists so two streams with the same ID in different namespaces
// cannot collide.
var streamLogNamespace = uuid.MustParse("6f2b4b4c-6f0e-4d9d-9f61-2f9d0f6a9c11")

// DefaultBucketSize bounds how many messages share one storage partition.
// Immutable per stream once chosen, because changing it renumbers offsets.
const DefaultBucketSize int64 = 100_000

// defaultReadPageSize applies when a caller does not cap the result.
const defaultReadPageSize = 256

// LogAppend is one node's worth of staged bytes. The component produces these
// during a transition; whoever drives the transaction writes them. Keeping the
// two apart is what lets the append ride a workflow's own commit later without
// the component knowing.
type LogAppend struct {
	Bucket      int64
	NodeID      int64
	TxnID       int64
	PrevTxnID   int64
	Blob        *commonpb.DataBlob
	IsNewBucket bool
}

// BucketOf returns the bucket an offset belongs to.
func BucketOf(offset, bucketSize int64) int64 {
	return offset / bucketSize
}

// NodeIDOf maps a global offset to a node ID within its bucket. Node IDs are
// bucket-relative and start at 1, because the store rejects a node ID below 1.
func NodeIDOf(offset, bucketSize int64) int64 {
	return offset%bucketSize + 1
}

// BucketStart is the first global offset in a bucket.
func BucketStart(bucket, bucketSize int64) int64 {
	return bucket * bucketSize
}

// branchToken derives a bucket's branch deterministically, so locating a bucket
// is arithmetic rather than a lookup in state that would grow with the stream.
func branchToken(
	branchUtil persistence.HistoryBranchUtil,
	namespaceID string,
	collectionID string,
	bucket int64,
) ([]byte, error) {
	seed := fmt.Sprintf("%s/%s/%d", namespaceID, collectionID, bucket)
	treeID := uuid.NewSHA1(streamLogNamespace, []byte(seed)).String()
	branchID := uuid.NewSHA1(streamLogNamespace, []byte(seed+"/branch")).String()

	return branchUtil.NewHistoryBranch(
		namespaceID,
		collectionID,
		treeID,
		treeID,
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		0, 0, 0,
	)
}

// WriteAppend persists one staged node. Nodes are written before the frontier
// advances, so a crash here leaves nodes at or past head_offset that no reader
// can see, and a retry supersedes them.
func WriteAppend(
	ctx context.Context,
	execMgr persistence.ExecutionManager,
	shardID int32,
	namespaceID string,
	collectionID string,
	op LogAppend,
) error {
	token, err := branchToken(execMgr.GetHistoryBranchUtil(), namespaceID, collectionID, op.Bucket)
	if err != nil {
		return err
	}
	_, err = execMgr.AppendRawHistoryNodes(ctx, &persistence.AppendRawHistoryNodesRequest{
		ShardID:           shardID,
		BranchToken:       token,
		NodeID:            op.NodeID,
		TransactionID:     op.TxnID,
		PrevTransactionID: op.PrevTxnID,
		IsNewBranch:       op.IsNewBucket,
		Info:              fmt.Sprintf("stream:%s:%s", namespaceID, collectionID),
		History:           op.Blob,
	})
	return err
}

// ReadRange returns the raw batches covering [fromOffset, toOffset), walking
// bucket by bucket. Blobs are returned unparsed: the server has no business
// decoding user payloads, and the codec runs in the SDK.
func ReadRange(
	ctx context.Context,
	execMgr persistence.ExecutionManager,
	shardID int32,
	namespaceID string,
	collectionID string,
	bucketSize int64,
	fromOffset int64,
	toOffset int64,
	maxBatches int,
) ([]*commonpb.DataBlob, []int64, error) {
	var blobs []*commonpb.DataBlob
	var startOffsets []int64
	if fromOffset >= toOffset {
		return blobs, startOffsets, nil
	}

	// maxBatches caps what the caller gets back; it is not the page size. The
	// store derives its paging token from whether a page came back full, so a
	// page size of zero makes it read past the end of an empty result.
	pageSize := maxBatches
	if pageSize <= 0 {
		pageSize = defaultReadPageSize
	}

	for bucket := BucketOf(fromOffset, bucketSize); BucketStart(bucket, bucketSize) < toOffset; bucket++ {
		token, err := branchToken(execMgr.GetHistoryBranchUtil(), namespaceID, collectionID, bucket)
		if err != nil {
			return nil, nil, err
		}
		bucketStart := BucketStart(bucket, bucketSize)
		bucketEnd := bucketStart + bucketSize

		minOffset := max(fromOffset, bucketStart)
		maxOffset := min(toOffset, bucketEnd)

		// A node ID is the first offset of its batch, so a read starting inside
		// a batch must begin at the node that contains it, not at the node ID
		// the offset maps to. Batch size is bounded on write, which bounds how
		// far back to start. Messages before fromOffset are dropped by the
		// caller.
		startNode := NodeIDOf(minOffset, bucketSize) - MaxMessagesPerBatch + 1
		if startNode < 1 {
			startNode = 1
		}

		var token2 []byte
		for {
			resp, err := execMgr.ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
				ShardID:       shardID,
				BranchToken:   token,
				MinEventID:    startNode,
				MaxEventID:    NodeIDOf(maxOffset-1, bucketSize) + 1,
				PageSize:      pageSize,
				NextPageToken: token2,
			})
			if err != nil {
				return nil, nil, err
			}
			for i, blob := range resp.HistoryEventBlobs {
				blobs = append(blobs, blob)
				startOffsets = append(startOffsets, bucketStart+resp.NodeIDs[i]-1)
			}
			token2 = resp.NextPageToken
			if len(token2) == 0 || (maxBatches > 0 && len(blobs) >= maxBatches) {
				break
			}
		}
		if maxBatches > 0 && len(blobs) >= maxBatches {
			break
		}
	}
	return blobs, startOffsets, nil
}

// DeleteBucket removes a whole bucket's tree. Reclaiming a truncated stream one
// bucket at a time is the point of bucketing: a partition is dropped outright
// rather than leaving a tombstone per message.
func DeleteBucket(
	ctx context.Context,
	execMgr persistence.ExecutionManager,
	shardID int32,
	namespaceID string,
	collectionID string,
	bucket int64,
) error {
	token, err := branchToken(execMgr.GetHistoryBranchUtil(), namespaceID, collectionID, bucket)
	if err != nil {
		return err
	}
	return execMgr.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: token,
	})
}

// ReclaimableBuckets lists buckets that lie entirely below the readable floor
// and can therefore be deleted. A bucket is only reclaimable once every offset
// it holds is unreadable, so this can never drop data a reader may still ask
// for.
func ReclaimableBuckets(previousBase, newBase, bucketSize int64) []int64 {
	var out []int64
	for b := BucketOf(previousBase, bucketSize); BucketStart(b+1, bucketSize) <= newBase; b++ {
		out = append(out, b)
	}
	return out
}
