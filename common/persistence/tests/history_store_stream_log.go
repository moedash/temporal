package tests

import (
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm/lib/stream"
	"go.temporal.io/server/common"
	p "go.temporal.io/server/common/persistence"
)

// Tests covering the history-node store's behaviour as a general append-only
// log, addressed by a branch whose tree ID is not a workflow run ID.
//
// These exist because a stream primitive reuses this store directly rather than
// standing up a parallel one. Two properties carry that decision: the node blob
// is never interpreted, and a stale node left behind by a shrinking retry is
// rejected on read even once the visibility frontier has moved past it. Neither
// was covered before, and the second is the one that decides whether the reuse
// is sound at all.

// newLogBranch mints a branch whose tree ID is a fresh UUID rather than a run ID.
func (s *HistoryEventsSuite) newLogBranch() []byte {
	branchID := uuid.NewString()
	branchToken, err := s.store.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(), // tree ID: not a run ID
		&branchID,
		[]*persistencespb.HistoryBranchRange{},
		time.Duration(0),
		time.Duration(0),
		time.Duration(0),
	)
	s.NoError(err)
	return branchToken
}

func (s *HistoryEventsSuite) appendLogNode(
	shardID int32,
	branchToken []byte,
	nodeID int64,
	txnID int64,
	prevTxnID int64,
	blob *commonpb.DataBlob,
) {
	_, err := s.store.AppendRawHistoryNodes(s.Ctx, &p.AppendRawHistoryNodesRequest{
		ShardID:           shardID,
		BranchToken:       branchToken,
		NodeID:            nodeID,
		TransactionID:     txnID,
		PrevTransactionID: prevTxnID,
		IsNewBranch:       nodeID == common.FirstEventID,
		Info:              "",
		History:           blob,
	})
	s.NoError(err)
}

func (s *HistoryEventsSuite) listRawLogNodes(
	shardID int32,
	branchToken []byte,
	minNodeID int64,
	maxNodeID int64,
) ([]*commonpb.DataBlob, []int64) {
	var token []byte
	var blobs []*commonpb.DataBlob
	var nodeIDs []int64
	for doContinue := true; doContinue; doContinue = len(token) > 0 {
		resp, err := s.store.ReadRawHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
			ShardID:       shardID,
			BranchToken:   branchToken,
			MinEventID:    minNodeID,
			MaxEventID:    maxNodeID,
			PageSize:      1,
			NextPageToken: token,
		})
		s.NoError(err)
		token = resp.NextPageToken
		blobs = append(blobs, resp.HistoryEventBlobs...)
		nodeIDs = append(nodeIDs, resp.NodeIDs...)
	}
	return blobs, nodeIDs
}

func (s *HistoryEventsSuite) eventIDsOf(events []*historypb.HistoryEvent) []int64 {
	ids := make([]int64, len(events))
	for i, e := range events {
		ids[i] = e.EventId
	}
	return ids
}

// TestStreamLogBlobIsOpaque checks the store round-trips a node blob it cannot
// parse. A stream keeps application payloads here, so any attempt to interpret
// the bytes as history events would reject them.
func (s *HistoryEventsSuite) TestStreamLogBlobIsOpaque() {
	branchToken := s.newLogBranch()

	payload := []byte("not a History proto \x00\x01\x02 arbitrary stream bytes")
	blob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         payload,
	}

	s.appendLogNode(s.ShardID, branchToken, common.FirstEventID, 100, 0, blob)

	blobs, nodeIDs := s.listRawLogNodes(s.ShardID, branchToken, common.FirstEventID, common.FirstEventID+1)
	s.Len(blobs, 1)
	s.Equal([]int64{common.FirstEventID}, nodeIDs)
	s.Equal(payload, blobs[0].Data)
}

// TestStreamLogShrinkingRetryDropsStaleNode covers the case where a retry writes
// fewer nodes than the attempt it replaces, leaving a stale node past the
// retry's extent. Once later appends move the frontier beyond it, clipping the
// read range no longer hides it, so the store's transaction-ID chain has to
// reject it.
//
// Layout: node 1 commits, then a failed attempt writes nodes 11 and 13, then a
// smaller retry writes node 11 alone, then node 12 commits. Node 13 is stale
// and now sits below the frontier.
func (s *HistoryEventsSuite) TestStreamLogShrinkingRetryDropsStaleNode() {
	branchToken := s.newLogBranch()

	committed := s.newHistoryEvents([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 100, 0)
	s.appendRawHistoryBatches(s.ShardID, branchToken, committed)

	// Attempt that fails after writing both nodes; the frontier never advances.
	staleFirst := s.newHistoryEvents([]int64{11, 12}, 200, 100)
	s.appendRawHistoryBatches(s.ShardID, branchToken, staleFirst)
	staleSecond := s.newHistoryEvents([]int64{13, 14}, 201, 200)
	s.appendRawHistoryBatches(s.ShardID, branchToken, staleSecond)

	// Retry carries fewer messages, so it covers node 11 only.
	retry := s.newHistoryEvents([]int64{11}, 300, 100)
	s.appendRawHistoryBatches(s.ShardID, branchToken, retry)

	// Next append moves the frontier past the stale node at 13.
	next := s.newHistoryEvents([]int64{12, 13, 14, 15}, 400, 300)
	s.appendRawHistoryBatches(s.ShardID, branchToken, next)

	events := s.listHistoryEvents(s.ShardID, branchToken, common.FirstEventID, 16)
	s.Equal(
		[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		s.eventIDsOf(events),
		"stale node 13 from the abandoned attempt must not be returned",
	)

	// The raw path carries no contiguity check of its own, so for a caller that
	// never parses the blob the transaction-ID chain is the only thing standing
	// between it and the stale node.
	_, nodeIDs := s.listRawLogNodes(s.ShardID, branchToken, common.FirstEventID, 16)
	s.Equal([]int64{1, 11, 12}, nodeIDs, "raw reads must drop the stale node too")
}

// TestStreamLogTrimReclaimsStaleNodes checks that trimming against the committed
// frontier reclaims stale nodes without disturbing the valid chain. This is what
// lets a stream clean up after a failed append promptly instead of waiting on a
// background scavenger.
func (s *HistoryEventsSuite) TestStreamLogTrimReclaimsStaleNodes() {
	branchToken := s.newLogBranch()

	committed := s.newHistoryEvents([]int64{1, 2, 3}, 100, 0)
	s.appendRawHistoryBatches(s.ShardID, branchToken, committed)

	stale := s.newHistoryEvents([]int64{4, 5}, 200, 100)
	s.appendRawHistoryBatches(s.ShardID, branchToken, stale)

	retry := s.newHistoryEvents([]int64{4}, 300, 100)
	s.appendRawHistoryBatches(s.ShardID, branchToken, retry)

	s.trimHistoryBranch(s.ShardID, branchToken, 4, 300)

	events := s.listHistoryEvents(s.ShardID, branchToken, common.FirstEventID, 5)
	s.Equal([]int64{1, 2, 3, 4}, s.eventIDsOf(events))
}

// The bucketing scheme in chasm/lib/stream splits a stream across one tree per
// fixed-size offset range, so a storage partition cannot grow with the stream.
// That also means a read restarts the transaction chain at every bucket, and
// the claim that this is still safe is the one worth testing rather than
// asserting: it is the same class of claim that turned out to be wrong about
// partition growth in the first place.

func (s *HistoryEventsSuite) streamAppend(
	collectionID string,
	bucketSize int64,
	firstOffset int64,
	count int64,
	txnID int64,
	prevTxnID int64,
	body string,
) {
	blob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte(body),
	}
	err := stream.WriteAppend(s.Ctx, s.store, s.ShardID, testStreamNamespaceID, collectionID, stream.LogAppend{
		Bucket:      stream.BucketOf(firstOffset, bucketSize),
		NodeID:      stream.NodeIDOf(firstOffset, bucketSize),
		TxnID:       txnID,
		PrevTxnID:   prevTxnID,
		Blob:        blob,
		IsNewBucket: stream.NodeIDOf(firstOffset, bucketSize) == 1,
	})
	s.NoError(err)
}

func (s *HistoryEventsSuite) streamRead(
	collectionID string,
	bucketSize int64,
	from int64,
	to int64,
) []string {
	blobs, _, err := stream.ReadRange(
		s.Ctx, s.store, s.ShardID, testStreamNamespaceID, collectionID, bucketSize, from, to, 0)
	s.NoError(err)
	out := make([]string, len(blobs))
	for i, b := range blobs {
		out[i] = string(b.Data)
	}
	return out
}

const testStreamNamespaceID = "0b9d2c3a-1f4e-4a7b-9c8d-5e6f70819203"

// TestStreamLogBucketedReadSpansTrees checks that a range read stitches buckets
// together, since each bucket is a separate tree and a caller only ever sees
// global offsets.
func (s *HistoryEventsSuite) TestStreamLogBucketedReadSpansTrees() {
	collectionID := uuid.NewString()
	const bucketSize = 4

	s.streamAppend(collectionID, bucketSize, 0, 4, 100, 0, "bucket0")
	s.streamAppend(collectionID, bucketSize, 4, 4, 200, 100, "bucket1")
	s.streamAppend(collectionID, bucketSize, 8, 2, 300, 200, "bucket2")

	s.Equal([]string{"bucket0", "bucket1", "bucket2"}, s.streamRead(collectionID, bucketSize, 0, 10))
	s.Equal([]string{"bucket1"}, s.streamRead(collectionID, bucketSize, 4, 8))
}

// TestStreamLogBucketBoundaryDropsStaleNode is the bucket-aware form of
// TestStreamLogShrinkingRetryDropsStaleNode. An abandoned append straddles a
// bucket boundary, so the stale node lands in a tree whose chain a reader
// starts fresh. It must still be rejected once the frontier moves past it.
func (s *HistoryEventsSuite) TestStreamLogBucketBoundaryDropsStaleNode() {
	collectionID := uuid.NewString()
	const bucketSize = 4

	s.streamAppend(collectionID, bucketSize, 0, 3, 100, 0, "committed")

	// Abandoned attempt: tail of bucket 0 plus the head of bucket 1.
	s.streamAppend(collectionID, bucketSize, 3, 1, 200, 100, "stale-bucket0")
	s.streamAppend(collectionID, bucketSize, 4, 2, 201, 200, "stale-bucket1")

	// Retry covers only bucket 0, so the bucket 1 node is orphaned.
	s.streamAppend(collectionID, bucketSize, 3, 1, 300, 100, "retry")

	// A later append reaches into bucket 1, moving the frontier past the orphan.
	s.streamAppend(collectionID, bucketSize, 4, 2, 400, 300, "real-bucket1")

	s.Equal(
		[]string{"committed", "retry", "real-bucket1"},
		s.streamRead(collectionID, bucketSize, 0, 6),
		"the orphaned bucket 1 node must not surface once the frontier passes it",
	)
}
