package stream

import (
	"context"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
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
	Bucket int64
	// The offsets this batch covers, end exclusive. The start is the key the
	// row is written under, so a retry of this append addresses the same row
	// and replaces it rather than racing it.
	StartOffset int64
	NextOffset  int64
	Blob        *commonpb.DataBlob
}

// BucketOf returns the bucket an offset belongs to.
func BucketOf(offset, bucketSize int64) int64 {
	return offset / bucketSize
}

// BucketStart is the first global offset in a bucket.
func BucketStart(bucket, bucketSize int64) int64 {
	return bucket * bucketSize
}

// WriteAppend persists one staged batch.
//
// Batches are written before the frontier advances, so a crash here leaves
// rows at or past the head offset that no reader can see, and a retry
// overwrites them because the offset is the key.
func WriteAppend(
	ctx context.Context,
	execMgr persistence.ExecutionManager,
	shardID int32,
	namespaceID string,
	collectionID string,
	op LogAppend,
) error {
	return execMgr.AppendStreamLog(ctx, &persistence.InternalAppendStreamLogRequest{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		CollectionID: collectionID,
		Bucket:       op.Bucket,
		StartOffset:  op.StartOffset,
		NextOffset:   op.NextOffset,
		Node:         op.Blob,
	})
}

// ReadRange returns the raw batches covering [fromOffset, toOffset), walking
// bucket by bucket. Blobs are returned unparsed: the server has no business
// decoding user payloads, and the codec runs in the SDK.
//
// The store begins each bucket at the batch containing the first offset asked
// for, not at that offset, so a read landing mid-batch gets the batch holding
// it. Finding that batch is one indexed lookup, because a row is keyed by the
// offset it starts at.
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

	pageSize := maxBatches
	if pageSize <= 0 {
		pageSize = defaultReadPageSize
	}

	for bucket := BucketOf(fromOffset, bucketSize); BucketStart(bucket, bucketSize) < toOffset; bucket++ {
		bucketStart := BucketStart(bucket, bucketSize)
		bucketEnd := bucketStart + bucketSize

		resp, err := execMgr.ReadStreamLog(ctx, &persistence.InternalReadStreamLogRequest{
			ShardID:      shardID,
			NamespaceID:  namespaceID,
			CollectionID: collectionID,
			Bucket:       bucket,
			MinOffset:    max(fromOffset, bucketStart),
			MaxOffset:    min(toOffset, bucketEnd),
			PageSize:     pageSize,
		})
		if err != nil {
			return nil, nil, err
		}

		blobs = append(blobs, resp.Batches...)
		startOffsets = append(startOffsets, resp.StartOffsets...)

		if maxBatches > 0 && len(blobs) >= maxBatches {
			return blobs[:maxBatches], startOffsets[:maxBatches], nil
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
	return execMgr.DeleteStreamLogBucket(ctx, &persistence.InternalDeleteStreamLogBucketRequest{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		CollectionID: collectionID,
		Bucket:       bucket,
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
