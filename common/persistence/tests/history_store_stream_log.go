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
	body string,
) {
	blob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte(body),
	}
	err := stream.WriteAppend(s.Ctx, s.store, s.ShardID, testStreamNamespaceID, collectionID, stream.LogAppend{
		Bucket:      stream.BucketOf(firstOffset, bucketSize),
		StartOffset: firstOffset,
		NextOffset:  firstOffset + count,
		Blob:        blob,
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

	s.streamAppend(collectionID, bucketSize, 0, 4, "bucket0")
	s.streamAppend(collectionID, bucketSize, 4, 4, "bucket1")
	s.streamAppend(collectionID, bucketSize, 8, 2, "bucket2")

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

	s.streamAppend(collectionID, bucketSize, 0, 3, "committed")

	// Abandoned attempt: tail of bucket 0 plus the head of bucket 1.
	s.streamAppend(collectionID, bucketSize, 3, 1, "stale-bucket0")
	s.streamAppend(collectionID, bucketSize, 4, 2, "stale-bucket1")

	// Retry covers only bucket 0, so the bucket 1 node is orphaned.
	s.streamAppend(collectionID, bucketSize, 3, 1, "retry")

	// A later append reaches into bucket 1, moving the frontier past the orphan.
	s.streamAppend(collectionID, bucketSize, 4, 2, "real-bucket1")

	s.Equal(
		[]string{"committed", "retry", "real-bucket1"},
		s.streamRead(collectionID, bucketSize, 0, 6),
		"the orphaned bucket 1 node must not surface once the frontier passes it",
	)
}

// TestStreamLogOrphanFromAnotherSequenceShadowsLaterWrites is the failure the
// prototype shipped with: two producers numbering from two unrelated sequences.
//
// The chain rule keeps the highest transaction id it has seen and drops
// everything below it, which orders contested nodes correctly only while every
// writer draws from one sequence. A node left behind by a write that never
// committed still counts, because the rule reads storage and not the frontier.
// So an uncommitted node numbered from a sequence that runs ahead hides every
// later write numbered from the one that runs behind, and the reader is not
// told: it simply stops seeing new messages.
//
// The fix is one sequence per shard for every producer. This test pins the
// behaviour that made the bug invisible, so a second sequence cannot come back.
func (s *HistoryEventsSuite) TestStreamLogOrphanFromAnotherSequenceShadowsLaterWrites() {
	branchToken := s.newLogBranch()

	// A committed append, numbered from the sequence the workflow used.
	first := s.newHistoryEvents([]int64{1, 2}, 32, 0)
	s.appendRawHistoryBatches(s.ShardID, branchToken, first)

	// A producer outside the workflow writes its node and then fails to commit,
	// so the frontier never covers it. Its id comes from the shard generator and
	// is far above anything the workflow task path produces.
	orphan := s.newHistoryEvents([]int64{3, 4}, 5000, 32)
	s.appendRawHistoryBatches(s.ShardID, branchToken, orphan)

	// Two more committed appends from the workflow. Both are numbered below the
	// orphan, which is the whole problem.
	second := s.newHistoryEvents([]int64{3, 4}, 33, 32)
	s.appendRawHistoryBatches(s.ShardID, branchToken, second)
	third := s.newHistoryEvents([]int64{5, 6}, 34, 33)
	s.appendRawHistoryBatches(s.ShardID, branchToken, third)

	events := s.listHistoryEvents(s.ShardID, branchToken, common.FirstEventID, 7)
	s.Equal(
		[]int64{1, 2, 3, 4},
		s.eventIDsOf(events),
		"the orphan is served and both committed appends after it are dropped, "+
			"which is why a second transaction-id sequence loses data",
	)

	// The same log, written the way the fix writes it: every producer numbering
	// from the shard, so the orphan is superseded rather than dominant.
	fixed := s.newLogBranch()
	s.appendRawHistoryBatches(s.ShardID, fixed, s.newHistoryEvents([]int64{1, 2}, 5001, 0))
	s.appendRawHistoryBatches(s.ShardID, fixed, s.newHistoryEvents([]int64{3, 4}, 5002, 5001))
	s.appendRawHistoryBatches(s.ShardID, fixed, s.newHistoryEvents([]int64{3, 4}, 5003, 5001))
	s.appendRawHistoryBatches(s.ShardID, fixed, s.newHistoryEvents([]int64{5, 6}, 5004, 5003))

	events = s.listHistoryEvents(s.ShardID, fixed, common.FirstEventID, 7)
	s.Equal(
		[]int64{1, 2, 3, 4, 5, 6},
		s.eventIDsOf(events),
		"one sequence per shard: the uncommitted node is superseded and nothing is lost",
	)
}

// TestStreamLogAppendIsIdempotentByOffset is the property the dedicated facet
// exists for.
//
// A row is keyed by the offset its batch starts at, so a retry of an append
// addresses the row it wrote before and replaces it. There is no chain to order
// two writers against, so there is nothing for an uncommitted write to outrank,
// which is the failure the previous substrate had.
func (s *HistoryEventsSuite) TestStreamLogAppendIsIdempotentByOffset() {
	const collectionID = "idempotent-by-offset"
	const bucketSize = 100

	s.streamAppend(collectionID, bucketSize, 0, 2, "first")

	// An attempt that wrote and never committed, at offsets the next attempt
	// will reuse. On the old substrate this could outrank what followed.
	s.streamAppend(collectionID, bucketSize, 2, 3, "abandoned")

	// The retry, covering fewer offsets from the same start.
	s.streamAppend(collectionID, bucketSize, 2, 1, "retry")

	// Whatever wrote last at that offset is what is there, and nothing before
	// or after it was disturbed.
	s.streamAppend(collectionID, bucketSize, 3, 1, "after")

	s.Equal(
		[]string{"first", "retry", "after"},
		s.streamRead(collectionID, bucketSize, 0, 4),
		"a rewrite at an offset replaces that batch and shadows nothing",
	)
}

// TestStreamLogReadFindsTheBatchHoldingAnOffset checks that a read starting
// inside a batch gets the batch containing it.
//
// The caller asks for an offset, not for a batch. Only the store can find the
// row holding it, which it does in one indexed lookup because the key is the
// offset a batch starts at. The previous substrate could not, and compensated
// by reading a whole batch's worth of rows backwards on every read.
func (s *HistoryEventsSuite) TestStreamLogReadFindsTheBatchHoldingAnOffset() {
	const collectionID = "mid-batch-read"
	const bucketSize = 100

	s.streamAppend(collectionID, bucketSize, 0, 10, "wide")
	s.streamAppend(collectionID, bucketSize, 10, 1, "narrow")

	// Offset 4 sits inside the first batch, which starts at 0.
	s.Equal(
		[]string{"wide", "narrow"},
		s.streamRead(collectionID, bucketSize, 4, 11),
		"a read landing mid-batch must be served the batch that holds it",
	)

	// And a read starting exactly on a boundary gets only what follows.
	s.Equal(
		[]string{"narrow"},
		s.streamRead(collectionID, bucketSize, 10, 11),
	)
}
