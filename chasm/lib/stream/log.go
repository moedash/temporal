package stream

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/persistence"
)

// A dedicated store for stream payload bytes, keyed by collection and by the
// offset a batch starts at. Offsets roll to a new bucket every bucketSize so no
// single partition grows with the stream, and because the bucket is arithmetic
// there is no index to keep.
//
// The component holds its payload in a chasm.Map now, so nothing on the serving
// path comes through here. What is left is reached only by the storage-level
// test suite, and it goes when that does.

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
