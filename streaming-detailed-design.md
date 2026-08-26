# Native Streams: Detailed Design

| | |
|---|---|
| Status | Draft for review |
| Ticket | [AI-198](https://temporalio.atlassian.net/browse/AI-198) |
| Author | Moe Dashti |
| Date | 2026-08-23 |
| Companion | `streaming-high-level-design.md`, `design-comparison.md` |

This document specifies the implementation. It assumes the high-level design and does not re-argue it. Line references are against `main` at `6805caea5`.

Changes adopted from the two existing prototypes are folded in here; `design-comparison.md` §7 records which change came from where.

---

## 1. Package layout

New CHASM library, following `docs/architecture/chasm.md` and modelled on `chasm/lib/activity`.

```
chasm/lib/stream/
├── proto/v1/
│   ├── stream_state.proto        # persisted component state
│   ├── message.proto             # StreamMessage, StreamMessageBatch
│   ├── request_response.proto
│   ├── service.proto             # StreamService
│   └── tasks.proto               # close / retention tasks
├── gen/streampb/v1/              # generated; picked up by CHASM_PROTO_FILES (Makefile:105)
├── stream.go                     # Stream component and its transitions
├── log.go                        # branch mint, append, range read
├── tailcache.go                  # shard-local ring of recent batches
├── config.go
├── handler.go                    # history-side gRPC handler
├── frontend.go                   # namespace name to ID, forward via layered client
├── library.go
├── fx.go                         # Module (history) and FrontendModule
└── tasks.go
```

---

## 2. Wire types

### 2.1 Item and batch

```protobuf
// message.proto
message StreamMessage {
  temporal.api.common.v1.Payload body = 1;
  // Producer-supplied and, later, server-enriched provenance
  // (workflow_id, run_id, original_run_id, attempt). Off by default.
  map<string, temporal.api.common.v1.Payload> metadata = 2;
  string topic = 3;
  // Position within this topic. The global offset orders the whole stream;
  // this lets a consumer reason about one topic without decoding the rest.
  int64 topic_sequence = 4;
  // Ordinary payload, or an in-band control marker such as a flush.
  StreamMessageKind kind = 5;
}

enum StreamMessageKind {
  STREAM_MESSAGE_KIND_UNSPECIFIED = 0;
  STREAM_MESSAGE_KIND_DATA = 1;
  // Producer signalling a delivery boundary; see 4.1b.
  STREAM_MESSAGE_KIND_FLUSH = 2;
}

// One append is one batch, and one batch is one history node.
message StreamMessageBatch {
  repeated StreamMessage messages = 1;
}
```

`StreamMessageBatch` is what gets serialized into the node blob. The server does not deserialize it on the normal read path; see §4.3 for the one case where it does.

### 2.2 Component state

```protobuf
// stream_state.proto
message StreamState {
  bytes branch_token = 1;

  // Visibility frontier. Readers never observe an offset at or past this.
  int64 head_offset = 2;
  // Truncation floor. Offsets below this are gone.
  int64 base_offset = 3;
  // Chains history nodes; see AppendRawHistoryNodesRequest.PrevTransactionID.
  int64 last_txn_id = 4;

  bool closed = 5;
  temporal.api.common.v1.Payload close_reason = 6;

  // Bumped on ownership change so a stale producer's write fails.
  int64 owner_epoch = 7;

  // Immutable once set. Offsets are bucketed into separate history-node trees
  // so no Cassandra partition grows with the stream; see 3.1a.
  int64 bucket_size = 11;

  // Set when a successor run takes ownership, so an in-flight poll can follow
  // the chain instead of stalling on a superseded run; see 9a.
  string redirect_run_id = 12;

  // producer_id -> last accepted (seq, first_offset). Bounded by producer count.
  map<string, ProducerCursor> producers = 8;
  // Registered in-workflow consumers; bounds truncation. Bounded by subscriber count.
  map<string, ConsumerCursor> consumers = 9;

  StreamLifecycle lifecycle = 10;
}

message ProducerCursor {
  int64 seq = 1;
  int64 first_offset = 2;   // replayed on a duplicate append
  int64 count = 3;
  // Distinguishes a genuine retry from a client reusing a sequence with
  // different content, which must be rejected rather than deduplicated.
  bytes content_hash = 6;
  // Set by FinishWriting. Ends this producer's writes without closing
  // the stream for anyone else.
  bool fenced = 4;
}

message ConsumerCursor {
  string workflow_id = 1;
  string run_id = 2;
  int64 offset = 3;
  // Pins truncation; see 8.4.
  bool active = 4;
}

message StreamLifecycle {
  google.protobuf.Duration retention = 1;
  int64 max_items = 2;      // 0 = unbounded
  int64 max_bytes = 3;      // 0 = unbounded
}
```

Size is O(producers + consumers), not O(items). That is the property that keeps this off the CHASM partial-read critical path.

### 2.3 Service

```protobuf
service StreamService {
  rpc CreateStream(...)   { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
  rpc AddMessages(...)    { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
  rpc FinishWriting(...)  { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
  rpc PollMessages(...)   { business_id = "frontend_request.stream_id"; category = API_CATEGORY_LONG_POLL; }
  rpc DescribeStream(...) { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
  rpc CloseStream(...)    { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
  rpc TruncateStream(...) { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
  rpc DeleteStream(...)   { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
  rpc ListStreams(...)    { category = API_CATEGORY_STANDARD; }   // visibility-routed, not shard-routed
}
```

Options follow `chasm/lib/activity/proto/v1/service.proto`. `business_id` drives shard routing; `API_CATEGORY_LONG_POLL` puts `PollMessages` in the right quota bucket. `ListStreams` goes through the CHASM visibility manager (`chasm.VisibilityManager.ListExecutions`) rather than shard routing, which means the `Stream` component declares a `Visibility` field and a business-ID alias.

`FinishWriting` records a per-producer fence and does not close the stream. It orders behind every append that producer already issued, so the claim holds under concurrency. Closing is stream-wide and separate.

---

## 3. Storage mapping

### 3.1 Branch

Each stream mints one branch at creation:

```go
branchToken, err := shard.GetExecutionManager().GetHistoryBranchUtil().NewHistoryBranch(
    namespaceID, streamID, runID,
    treeID,      // the stream's CHASM execution RunID
    nil,         // branchID: generated
    nil,         // no ancestors
    0, 0, retention,
)
```

The OSS implementation (`common/persistence/history_branch_util.go:49`) ignores namespace, workflow, and run, and returns `{TreeId, BranchId, Ancestors}`. Passing them anyway keeps the SaaS override (Walker) able to do whatever it needs. See §11.

### 3.1a Offset bucketing

`history_node` in Cassandra is `PRIMARY KEY ((tree_id), branch_id, node_id, txn_id)`, so the partition key is `tree_id` alone and every node of a tree shares one partition. That is safe for Workflow History because history is capped by `HistorySizeLimitError` and `HistoryCountLimitError`. A stream is deliberately uncapped, so a single tree per stream would grow a Cassandra partition without bound.

So a stream is not one tree. It is a sequence of trees, one per fixed-size offset bucket:

```
bucket   = offset / bucket_size
treeID   = uuid.NewSHA1(streamNamespaceUUID, []byte(streamID + "/" + bucket))
branchID = uuid.NewSHA1(streamNamespaceUUID, []byte(streamID + "/" + bucket + "/b"))
```

Both are derived arithmetically, so there is no index to store and no index to grow. `bucket_size` is chosen by the server at creation and is immutable for the stream's life, because changing it would renumber existing offsets.

Consequences:

- A batch never crosses a bucket boundary. Split at the boundary before staging.
- A read spanning buckets issues one range read per bucket and concatenates.
- Whole-bucket truncation is `DeleteHistoryBranch` on that bucket's token, which reclaims a whole partition instead of leaving per-row tombstones. Partial truncation inside the live bucket just advances `base_offset`.

**Bucketing does not weaken the chain rule of §3.5**, but the argument is worth stating because it is not obvious. Reads restart the transaction chain per tree, so in principle a stale node could be accepted as a bucket's first node. It cannot, because bucket boundaries are offset-aligned: a stale node is either the bucket's first node, in which case it shares a node ID with the real first node and loses on transaction ID, or it is preceded within its bucket by valid nodes, in which case the chain rule applies exactly as it does within one tree. This needs a test, listed in §15.

### 3.2 Offset to node ID

`serializeAppendRawHistoryNodesRequest` rejects `nodeID <= 0` with "eventID cannot be less than 1" (`common/persistence/history_manager.go:429-433`). So:

```
nodeID = (offset % bucket_size) + 1
```

API offsets start at 0 and are global across buckets. The node ID is bucket-relative, which is why the `+ 1` and the modulo both matter. This mapping is internal and must never leak into the wire protocol.

### 3.3 Append

```go
persistence.AppendRawHistoryNodesRequest{
    ShardID:           shardID,
    BranchToken:       s.BranchToken,
    IsNewBranch:       s.HeadOffset == 0,
    Info:              streamInfo(namespaceID, streamID),
    History:           blob,              // serialized StreamMessageBatch, opaque to the store
    NodeID:            s.HeadOffset + 1,
    PrevTransactionID: s.LastTxnID,
    TransactionID:     txnID,             // from the shard's transaction ID generator
}
```

Verified opaque: `AppendRawHistoryNodes` (`history_manager.go:501`) passes `request.History` straight through and only reads `len(.Data)` for size accounting. Nothing parses it.

`transactionSizeLimit()` caps the blob (`history_manager.go:437-442`), so a batch that exceeds it must be split across nodes before the transition commits.

### 3.4 Read

```go
persistence.ReadHistoryBranchRequest{
    ShardID:     shardID,
    BranchToken: s.BranchToken,
    MinEventID:  fromOffset + 1,
    MaxEventID:  s.HeadOffset + 1,     // exclusive
    PageSize:    pageSize,
    NextPageToken: token,
}
```

`ReadRawHistoryBranch` returns `HistoryEventBlobs []*DataBlob`, `NodeIDs []int64`, and a page token, without parsing.

### 3.5 The clip invariant and the transaction-ID chain

> **Reads clip to `HeadOffset`, and the store drops any node whose transaction ID went backwards. Together those are sufficient; no prepare phase is required.**

Node append and frontier update are not one atomic store operation, and never were for workflow history either. Cassandra's `execution_store.go:110-126` appends history nodes in a loop and then calls `UpdateWorkflowExecution`; SQL does the same at `sql/execution.go:339`.

Three failure shapes:

1. **Nodes written, frontier not advanced.** Orphan nodes sit at offsets at or past `HeadOffset`. `MaxEventID` excludes them, so no reader observes them.
2. **Duplicate node IDs from a retry.** `filterHistoryNodes` (`history_manager.go:1039-1073`) keeps the highest transaction ID per node ID.
3. **Orphans beyond the retry's extent, later shadowed by the advancing frontier.** This is the case clipping alone does not cover, and it is the one an objection to this approach would reach for.

Concretely for (3): an append writes nodes at offsets 100 and 110 under transaction `T1` and fails. The producer retries with a smaller batch, writing only node 100 under `T2`, and the frontier advances to 105. A later append writes node 105 under `T3`. Node 110 is now below `HeadOffset` and clipping would expose it.

The store already handles this. `filterHistoryNodes` requires transaction IDs to be non-decreasing as node IDs increase and skips anything that regresses:

```go
if node.TransactionID < lastTransactionID {
    continue
}
```

with the contract stated in its own comment at `:1063-1066`: "event batches with larger node ID -> batch with lower transaction ID is invalid (happens before)". Node 110 carries `T1 < T3`, so it is dropped. This is the same mechanism that protects workflow history after a failed transaction, and it is why `PrevTransactionID` exists on the append request.

**Requirement this places on us:** transaction IDs must come from the shard's monotonic generator, never from a per-stream counter, and each attempt must take a fresh one. Reusing a transaction ID across attempts breaks the chain rule.

**Verified.** `TestStreamLog_ShrinkingRetryDropsStaleNode` in `common/persistence/tests/history_store_stream_log.go` builds exactly the layout above and asserts the stale node is absent from both `ReadHistoryBranch` and `ReadRawHistoryBranch`. Passing on SQLite and Postgres. The raw assertion is the load-bearing one: `ReadHistoryBranch` has a contiguity check that independently catches a leak, and a stream never goes through it.

There is no window in which a reader sees a gap, and no window in which two readers disagree about a prefix.

### 3.6 Orphan reclamation

Correctness does not depend on cleanup, but storage does. Streams produce orphans more often than workflow history does, through publish retries and truncate races, so a slow background scavenger is the wrong cadence.

`TrimHistoryBranch(BranchToken, NodeID, TransactionID)` takes a known-good frontier and removes everything off the valid chain. The `Stream` component holds exactly that frontier in `head_offset` and `last_txn_id`.

So: on a failed append, schedule a CHASM pure task that calls `TrimHistoryBranch` with the committed frontier. Cheap, targeted, and it runs seconds after the failure rather than hours. Coalesce with `WithSingletonTask` so a burst of failures produces one trim.

If measurement shows orphan volume outrunning this, a periodic per-shard sweep is the fallback. Instrument orphan bytes from the first benchmark so the question is answered with data.

---

## 4. RPCs

### 4.1 `AddMessages`

```
AddMessages(namespace, stream_id, producer_id?, seq?, expected_offset?, owner_epoch?, messages[])
  -> { first_offset, next_offset, head_offset }
```

Handler calls `chasm.UpdateComponent(ctx, ref, (*Stream).AddMessages, req)`. Inside the transition, in this order:

1. `Closed` -> `FailedPrecondition` with reason `StreamClosed`.
2. **Dedup.** If `producer_id` is set and `producers[producer_id].seq == seq`, compare `content_hash`. Matching content returns the recorded `first_offset` and `count` without appending. **Differing content is rejected** with `InvalidArgument`, because silently returning the old offsets would drop the caller's data and look like success. A stale or skipped sequence is rejected with the last accepted sequence in the error details so the caller can resynchronise.
3. **Write fence.** If `producers[producer_id].fenced` -> `FailedPrecondition` with reason `ProducerFinished`.
4. **Ownership fence.** If `owner_epoch` supplied and below `state.owner_epoch`, return `FailedPrecondition` with reason `ProducerFenced`.
5. **Compare-and-append.** If `expected_offset` supplied and it differs from `head_offset`, return `AlreadyExists` carrying `head_offset` so the caller can resynchronise.
6. Serialize `StreamMessageBatch`, splitting if over `transactionSizeLimit`.
7. Emit pending log appends (§5) at `nodeID = head_offset + 1`, taking a fresh transaction ID from the shard generator (§3.5).
8. `head_offset += len(messages)`; `last_txn_id = txnID`; record the producer cursor.
9. **Inline retention truncation.** If `lifecycle.max_items` or `max_bytes` is set and now exceeded, advance `base_offset` and queue the trim, bounded by the consumer pin (§8.4). Doing this at the end of a successful append avoids a separate sweeper.

Acknowledge after the transaction commits. `first_offset` is the offset of the first message; the caller derives per-message offsets by position.

### 4.1a Group commit (designed, deferred)

**Not in the first cut.** The serialized one-transition-per-append path is easier to reason about and to prove, and the case for group commit rests on Cassandra numbers we are not measuring in this prototype. The design is recorded here so the public offset contract does not have to change when it lands.

A stream linearizes through one CHASM execution, so its ceiling is the transition rate on that execution. On Cassandra a transition is a lightweight transaction, and per-partition LWT throughput is the binding constraint.

This design costs **one** transition per append, because there is no prepare phase. Group commit divides even that: concurrent `AddMessages` calls for the same stream that arrive while a transition is in flight are queued, and the next transition applies all of them in order, assigning each a contiguous offset range and emitting one log append per member.

- Each member keeps its own dedup entry and its own `first_offset`, so idempotency is unaffected.
- A member that fails validation (closed, fenced, offset mismatch) is rejected individually without aborting the group.
- Group size is capped by `stream.maxGroupCommitSize` and by the aggregate blob size against `transactionSizeLimit`.

Combined with clients batching several items into one call, this is what keeps a hot stream inside the ceiling. The 1-pager forbids user-facing batching knobs, and this respects that: group size is chosen by the server from what happens to be in flight, not configured by the application.

`producer_id` and `expected_offset` are alternative idempotency mechanisms. `producer_id` suits a retrying activity; `expected_offset` suits a caller that already tracks position. Supplying neither gives at-least-once, which is a valid choice for a caller that does not care.

### 4.1b Flush

A producer can append a `STREAM_MESSAGE_KIND_FLUSH` marker to say "the turn is finished, deliver what you have". It carries no payload and consumes one offset like any other message.

It matters for Path C. Without it, a consumer that has drained the current slice has no way to distinguish "the producer is still generating" from "the producer is done", and the only remaining signal is a timeout. An LLM turn has a natural end, the producer knows when it happens, and making it say so removes a tuning knob rather than adding one.

The server does not interpret flush beyond delivering it. Whether a flush ends an iteration is the consuming application's decision, which keeps the semantics out of the server and matches leaving topic meaning to the application.

Note this is a delivery hint, not a lifecycle event. It is weaker than `FinishWriting` (which fences a producer) and much weaker than `CloseStream` (which seals the stream). All three exist because they answer different questions: "this turn is done", "I am done", "the stream is done".

### 4.2 `PollMessages`

```
PollMessages(namespace, stream_id, from_offset, max_items, max_bytes,
             topics[], wait_new_messages, wait_timeout)
  -> { messages[], first_offset, next_offset, closed, close_reason, head_offset }
```

`topics` filters at read time. Filtering is by exact topic match only, no predicates, because a predicate would have to run server-side over payloads the server cannot decode. Offsets are assigned over the unfiltered stream, so `next_offset` always advances past everything read, filtered out or not. That keeps cross-topic ordering available to callers who pass no filter.

Topic filtering forces the server to decode the batch envelope (not the payloads) to inspect each message's `topic`. That is the second case, alongside §4.3, where a batch is deserialized; both are bounded by batch size.

1. `from_offset < base_offset` -> `OutOfRange` with reason `Truncated`, carrying `base_offset` so the reader can jump forward rather than fail.
2. `from_offset > head_offset` -> `InvalidArgument`.
3. `from_offset < head_offset`: serve. Tail cache first (§6); on miss, `ReadRawHistoryBranch` across the buckets the range spans. Trim to `max_items` and `max_bytes`. Return.
   **If the read returns fewer messages than `[from_offset, head_offset)` implies and the shortfall is at or above `base_offset`, return `DataLoss`, never a short or empty page.** Committed data that has gone missing is the worst failure a durable stream can have, and reporting it as an empty stream lets a consumer conclude the producer simply had nothing to say. The frontier in the component is what makes this checkable without a separate manifest.
4. `from_offset == head_offset` and `closed`: return empty with `closed = true`.
5. `from_offset == head_offset`, not closed, `wait_new_messages`: long-poll (§4.4).
6. Otherwise return empty immediately.

### 4.3 Reading from mid-batch

A batch is one node, so `from_offset` can land inside one. Two options:

- **Chosen:** the server deserializes only the boundary batch and drops the leading messages. One batch, bounded by batch size, and only on the first page.
- Rejected: return whole batches and let the reader skip. That leaks framing into the protocol and makes `max_bytes` unenforceable.

Every other batch on the page stays opaque and is forwarded as-is.

### 4.4 Long-poll

```go
chasm.PollComponent(ctx, ref, func(s *Stream, ctx chasm.Context, from int64) (pollOut, bool, error) {
    return pollOut{head: s.HeadOffset, closed: s.Closed},
           s.HeadOffset > from || s.Closed,
           nil
}, fromOffset)
```

The predicate is monotonic, which `chasm.PollComponent` requires (`chasm/engine.go:426-429`). `HeadOffset` only increases and `Closed` never clears, so it holds.

`PollComponent` subscribes before releasing the execution lease (`service/history/chasm_engine.go:743-765`), which closes the subscribe/notify race that `get_workflow_util.go:136` has to re-read around.

Timeout uses `contextutil.WithDeadlineBuffer` with per-namespace `stream.longPollTimeout` and `stream.longPollBuffer`, matching `chasm/lib/activity/handler.go:210-215`. On soft timeout, return an empty response with `next_offset = from_offset`, and the client re-polls. That is the established convention and it keeps a slow stream from looking like an error.

**Long-poll, not gRPC server-streaming.** Frontend runs 24 unary interceptors and 2 streaming ones (`service/frontend/fx.go:285-328`), and `common/authorization/interceptor.go:224` cannot resolve a namespace at stream handshake because there is no request body to read. Server-streaming is the right eventual read API. It needs new auth, rate-limit, and redirection plumbing, and it is not on the path to proving the cost claim.

---

## 5. The CHASM transaction hook

This is the one framework change, and the only item with an owner outside this project.

### 5.1 Problem

A CHASM component cannot contribute append-log batches at transaction close. `ChasmTree` (`service/history/interfaces/chasm_tree.go:19-53`) has no method for it. Without the hook, an append is two persistence calls: `AppendRawHistoryNodes`, then a separate CHASM update to advance the frontier.

### 5.2 What already works

`UpdateWorkflowExecutionRequest.UpdateWorkflowEvents` is `[]*WorkflowEvents`, and each entry carries its own `BranchToken`. Multi-branch appends in a single request are structurally supported. `WorkflowEvents.Events` is typed `[]*historypb.HistoryEvent` though, so it cannot carry an opaque blob.

### 5.3 Change

Add a sibling list rather than overloading `WorkflowEvents`:

```go
// common/persistence/data_interfaces.go
type LogAppend struct {
    BranchToken []byte
    NodeID      int64
    PrevTxnID   int64
    TxnID       int64
    Blob        *commonpb.DataBlob
    IsNewBranch bool
    Info        string
}

type UpdateWorkflowExecutionRequest struct {
    // ... existing fields
    UpdateLogAppends []*LogAppend
    NewLogAppends    []*LogAppend
}
```

`executionManagerImpl.UpdateWorkflowExecution` (`common/persistence/execution_manager.go:168`) folds these into the same `[]*InternalAppendHistoryNodesRequest` it already builds from `UpdateWorkflowEvents`, reusing `serializeAppendRawHistoryNodesRequest`.

On the CHASM side, `CloseTransaction` gains a way to surface pending appends:

```go
// service/history/interfaces/chasm_tree.go
CloseTransaction() (chasm.NodesMutation, []*persistence.LogAppend, error)
```

with `MutableStateImpl.closeTransaction` passing them through. A component registers a pending append during its transition via a new `MutableContext` method.

### 5.4 Fallback

If the hook slips, both producer paths still work as two persistence calls in the same order, with the clip invariant unchanged. Cost is one extra round trip per batch, which is still far below a signal. Sequence the hook first but do not block Stage 1 on it.

---

## 6. Tail cache

A shard-local ring per open stream, holding the last N appended blobs keyed by offset.

- Populated on append, so the producer's own bytes are already in memory.
- Read path checks it before touching persistence, which makes a reader at the tail a memcopy.
- Bounded per stream and in aggregate per shard, evicting oldest first. A miss falls through to `ReadRawHistoryBranch`, so the cache is never load-bearing for correctness.
- Dropped when the shard loses ownership. A new owner rebuilds it from appends.

This is what makes fan-out cheap. N readers at the tail cost N memcopies rather than N range scans, which is the difference between the 10-subscriber limit and no meaningful limit.

`service/history/chasm_notifier.go` uses one global mutex and says so in TODOs at lines 16 and 18. Acceptable for a prototype; a real item before fan-out at scale.

---

## 7. Path A: workflow publishes to its own stream

An attached stream is a subcomponent of the workflow's execution, so it is already on the workflow's shard and inside the workflow's lock.

New command handled in `RespondWorkflowTaskCompleted`:

```protobuf
COMMAND_TYPE_ADD_STREAM_MESSAGES

message AddStreamMessagesCommandAttributes {
  string stream_id = 1;                  // empty = the workflow's default output stream
  repeated StreamMessage messages = 2;
}
```

Handling goes in `chasm/lib/workflow/` next to the existing command handlers. The handler resolves the attached `Stream` subcomponent and runs the §4.1 transition against it. The appends ride the workflow task's existing commit via §5.

Cost of publishing: one extra blob in a write that was already happening. No history event, no extra round trip, and the workflow is not rescheduled.

Publishing to a stream the workflow does not own is not supported by this command. Use `AddMessages`. See §12.

---

## 8. Path C: workflow consumes a stream

### 8.1 Mechanism

A workflow subscribes by recording a `ConsumerCursor`. Thereafter, when the server builds a workflow task for that execution, it attaches the pending slice:

```
PollWorkflowTaskQueueResponse.stream_slices: [
  { stream_id, from_offset, to_offset, messages[] }
]
```

and records the range it delivered as an attribute on the event that closes the task:

```
WorkflowTaskCompletedEventAttributes.stream_cursors: [
  { stream_id, from_offset, to_offset }
]
```

The response field carries bytes and serves the live delivery. The event
attribute carries offsets only. Replay is served from the event attribute, not
from the response field, for the reason in §8.3.

**No new event type.** The range rides an event that already exists once per task, so in-workflow consumption adds zero events to history.

### 8.1a Where the cursor lives, and how a range becomes a fact

The cursor is a subcomponent of the **consuming workflow**, not of the stream.

That placement is what makes the advance atomic. A workflow's CHASM nodes and its History events travel in the same `WorkflowMutation` (`UpsertChasmNodes` alongside the events, `common/persistence/data_interfaces.go:367`), so folding a delivered range into the cursor lands in the same transaction as the `WorkflowTaskCompleted` event that records it. Held on the stream instead, every advance would be a cross-execution write, and a crash between the two writes would either redeliver a range or skip one with nothing in History to show it.

This also settles a question left open in §5: **the CHASM transaction hook is not required for Path C.** It remains wanted for the producer side, where the log append is a genuinely separate persistence write, but consumption needs nothing new.

A range therefore becomes a fact in two steps:

1. **At delivery**, the range attached to the task is staged on the cursor as a pending range. Staging never advances the cursor.
2. **At completion**, the pending range is recorded on the event and folded into the cursor, in one transaction.

A task that fails or times out recorded nothing, so its staged range never became history. The next delivery re-reads from the unchanged cursor and simply overwrites what was staged, which is why redelivery is allowed to produce a different range than the attempt before it.

The stream keeps a separate `ConsumerCursor` as a **truncation floor** only. It is advisory for retention and is not the position anything is served from, so it can lag without affecting correctness.

### 8.1b What the prototype implements

Delivery, staging, recording and the cursor advance are built and covered by `tests/stream_consume_test.go`. Two limits are worth naming rather than leaving to be discovered:

**Only a stream in the consuming workflow's own execution can be consumed.** Reading a stream owned by another execution needs that stream's frontier, and the frontier lives on the stream component. Reaching it from inside the consuming workflow's transaction means a cross-execution read while holding the workflow lock, and the CHASM engine is not reachable from `RecordWorkflowTaskStarted` without re-threading it through the history engine. Subscribing to a stream the workflow does not own is rejected rather than silently returning nothing.

**Replay reassembly is built.** When a workflow task carries History, every `WorkflowTaskCompleted` in it that recorded a range gets its payloads re-read from the log and attached, tagged with that event's id. A response therefore holds at most one untagged slice, for the task about to run, plus one per recorded range being replayed. No SDK reads the field yet, so the consuming end is still unproven.

The cost is the one §8.3 flagged: a delivery carrying full History re-reads every range that History records. Sticky delivery carries only the tail and pays proportionally less, but a cold replay of a long-lived consumer re-reads everything it ever consumed, and that still has no bound.

One implementation detail with a cost attached: both the delivery and completion paths resolve the workflow component **read-only first**, and only take the mutable path when a cursor exists. Reaching it mutably marks the node dirty, which would add a node to the transaction of every workflow in the cluster, subscribed or not. That showed up as a task-generation change in `TestRefreshSubStateMachineTasks` before the read-only check was added.

### 8.2 What must be recorded, and why empty counts

Two rules, both load-bearing:

**Record on every task where the stream is subscribed, including when `from_offset == to_offset`.** A task in which the subscription observed nothing is a fact replay must reproduce, not an absence of a fact. If empty ranges are omitted, replay has no record that the workflow reached that point with nothing available, so it is free to deliver items the workflow did not have then. The resulting divergence does not surface at the point of the error; it surfaces later as an unrelated nondeterminism failure, which makes it expensive to diagnose.

Riding `WorkflowTaskCompleted` is what makes this affordable. A separate event per task per subscribed stream would have made the idle case cost an event.

**The first record for a subscription carries the resolved start offset.** "Subscribe from the current tail" resolves against `head_offset` at subscribe time, which is a nondeterministic reading. Recording the resolved value turns it into a fact. This applies whether or not anything was delivered on that task.

### 8.3 Determinism, and where the bytes come from on replay

On replay the server reads `[from_offset, to_offset)` from the same branch and attaches the same bytes. That read is deterministic because:

- the log is immutable, so a given offset always holds the same bytes;
- the range is recorded, so it does not depend on when replay happens;
- `filterHistoryNodes` resolves the node chain the same way on every read;
- every task carries a record, so the sequence of observations is fully reconstructible.

What that leaves open is the carrier: which channel hands those bytes to the worker when the task being replayed is not the current one. The response field cannot do it. It is built once per delivery (`CreateRecordWorkflowTaskStartedResponseWithRawHistory`, `service/history/api/recordworkflowtaskstarted/api.go:400`), so it holds a single slice set, while a cache miss replays every prior task.

The Go SDK already answers this for Updates, and the answer is a constraint on us rather than a choice. In `ProcessWorkflowTask` the message index is built from one of two sources depending on the path (`internal/internal_task_handlers.go:1106`):

- live: `indexMessagesByEventID(taskMessages)`, from the poll response;
- replay: `indexMessagesByEventID(historyMessages)`, synthesized from the event stream by `inferMessageFromAcceptedEvent` while iterating events (`:518`).

Two consequences:

1. **On replay the SDK takes per-event data from History only.** It never calls back to the server inside the replay loop, and nothing in that loop does I/O. So "the SDK re-reads each recorded range during replay" is not a small change. It puts a blocking network call inside the deterministic replay path.
2. **A missing body is already a hard failure rather than a nondeterminism error.** `if historyMessages[i].Body == nil { return nil, fmt.Errorf("missing body in message for update ID %v", ...) }` (`:1120`). That is the shape §8.4 asks for when stream data has expired, and it already exists.

Delivery position is solved too: messages are drained by event ID, before and after each `ProcessEvent` (`:1203`, `:1221`), so a slice keyed to the `WorkflowTaskCompleted` event ID lands at the same point live and on replay.

**Decision: the server reassembles slices on the History read path, keyed by the recorded cursor.** Storage keeps the property the whole design rests on, because only offsets are written to History. Reassembly happens on read: the bytes are fetched from the stream branch and attached to the event that recorded the range. This matches the carrier the SDK already expects, so it needs no change to the replay loop.

The Update precedent buys the same guarantee by writing the payload into the event (`accepted_request`, whose proto comment says it exists "so that the worker can recreate and deliver that same message as part of replay"). We keep the recreate-from-History contract and move the cost from write to read.

That cost is real and belongs on the read path's budget: one stream read per subscribed workflow per cache miss, on a path shared with `GetWorkflowExecutionHistory` and the UI. §6's tail cache covers the common case, where replay follows soon after the writes. Cold replay of an old workflow is the expensive case and needs a bound before this is more than a prototype.

**Segmentation is not a concern here.** One slice arrives per workflow task, the SDK drains it once, and replay drains an identical slice once. A client-side design that reads a backend continuously while a task is open has to reconstruct how many times the consumer was woken within the task, because condition evaluation happens per wake. Putting the delivery boundary at the task boundary, which is already a durable boundary, removes that problem rather than solving it.

### 8.4 Truncation interlock

Replay needs the bytes to still exist. So truncation is bounded:

```
effective_base = min(requested_base, min over consumers of consumer.offset)
```

`TruncateStream` below a registered consumer cursor is rejected with `FailedPrecondition`. A consumer is deregistered when its workflow closes, which releases the floor.

This is the one place where a consumer constrains the stream, and it is unavoidable: recording a cursor instead of the data means the data has to outlive the cursor.

Implemented for attached streams: subscribing registers a pin on the stream in the same transaction that creates the cursor, and committing a delivered range advances the pin with it. `Truncate` rejects a base past the lowest active pin, and the message cap yields to it rather than dropping a range a consumer recorded a cursor for. Registering and advancing in the cursor's own transaction is what keeps the two from drifting: a pin lost while its cursor survived would leave truncation free to take a range the cursor still points at.

The interlock covers deliberate truncation. It cannot cover retention expiry on a stream whose consumer outlives it, or out-of-band deletion. If replay finds a recorded range below `base_offset`, the workflow task **fails retryably** with a distinct error rather than raising a nondeterminism error. The distinction matters operationally: a nondeterminism error looks like a code bug and gets triaged as one, while "the stream data this workflow needs is gone" is an infrastructure condition with a different fix. An operator can restore or extend retention and the workflow proceeds.

### 8.4a The alternative: holding the task open

Option 7 in the canonical options doc solves the same problem by keeping the workflow task open and reading an external store directly inside it, completing when the stream is idle or a flush arrives. It is cheaper, because no data enters Temporal, and it needs no server work.

The structural difference is where the delivery boundary sits. Ours is the workflow task boundary, which is already durable and already recorded, so replay reproduces it for free. Option 7's boundary is wherever the reader happened to be when the task ended, so it has to be reconstructed: stream-empty markers, and per its own prototype's ADR-018, the number of drains as well, because `wait_condition` evaluates once per activation.

The cost of our choice is a workflow task per slice instead of one long task. The benefit is bounded task duration, which keeps us clear of the workflow-task timeout question (gap K3, still open) and avoids pinning a worker slot for the length of an LLM response. `design-comparison.md` §4 has the full comparison.

### 8.5 Bounded attachment and the wake exception

The attached slice is capped by **both** `stream.maxConsumeBytesPerTask` and `stream.maxConsumeItemsPerTask`. Bytes alone is not enough: a burst of many tiny messages stays under a byte cap while producing a slice large enough to make one task's drain unboundedly long. Whichever limit binds first, attach a prefix, record only that range, and schedule a follow-up workflow task.

That is the single intentional exception to "publishing never wakes a workflow".

Implemented in `closeTransactionHandleWorkflowTaskScheduling` rather than at workflow task completion. Completion is the obvious place and it is not enough: registering a subscription against a stream that already holds data completes no workflow task of its own, so a completion-time check leaves that workflow waiting for unrelated traffic. Transaction close covers both, and the pending-task check runs first so a workflow that already owes a task never pays for the subscription lookup. Publishing does not. An **active in-workflow subscription** does, because the workflow asked to be woken. Worth stating explicitly, because it is the property that keeps Path C from silently reintroducing the cost Path B removes. A workflow that does not subscribe is never woken by stream traffic.

### 8.6 Continue-as-new and reset

- **Continue-as-new**: the cursor is workflow state, carried in the continue-as-new input. The stream is untouched. Nothing is duplicated or dropped.
- **Reset**: the workflow rewinds; the stream does not. Cursor events before the reset point are intact, so replay works, and the new run re-consumes from the cursor as of that point. Relative to the abandoned run, some messages are delivered twice. That is visible to the application by design, matching the decision that rewinds are the application's concern rather than something the system hides.

---

## 8a. Run transitions for an attached stream

A standalone stream is unaffected by anything the workflow does. An attached stream lives in the workflow's execution, so a run transition moves it, and a reader addressed at the old run has to be able to follow.

### 8a.1 Continue-as-new, retry, cron

Successor creation already persists the old-run mutation and the new-run snapshot as one update. The stream rides that:

1. Apply any `AddStreamMessages` commands before the closing command. A stream command after a closing command is invalid.
2. Copy the bounded state into an equivalent child under the successor root, preserving stream ID, first execution run ID, `base_offset`, `head_offset`, `last_txn_id`, `bucket_size`, and the producer map.
3. Set `redirect_run_id` on the old child to the successor's run ID.
4. Persist old mutation, new snapshot, staged log appends, and the current-run pointer together.

Continue-as-new does not close the stream, and an append in the same workflow task as the continue-as-new is carried at its post-append head.

A poll holding a reference to the old run sees `redirect_run_id` set, follows it, and keeps its offset. Offsets are global across runs, so nothing about the cursor changes. The old child stays as a redirect target through retention, which is what stops an in-flight long poll from stalling on a superseded run.

### 8a.2 Reset

Reset rebuilds workflow state from an earlier point. It must not rewind the stream, because consumers may already have read past that point, and offsets never decrease.

Stream commands emit no history events, so replay cannot reconstruct stream state. Reset therefore carries it forward explicitly:

1. Take the current execution's lock through the existing reset path.
2. Rebuild the target mutable state from history.
3. Read the current run's stream children under that lock.
4. Replace any replay-derived stream children in the reset snapshot with copies of the **current** state, not the state as of the reset point.
5. Mark the replaced run's streams redirected to the reset run.
6. Commit through the existing conflict-resolution request.

An append racing a reset conflicts on the current run's database condition, so one of them retries. If the append commits first, the reset copies the advanced head. If the reset commits first, the append follows the redirect and lands on the reset run. There is no interleaving in which the reset becomes current with a head older than an acknowledged append.

Work repeated after a reset appends new messages at new offsets. The server does not try to detect that they are semantically the same as earlier ones; the producer metadata is there so the application can.

---

## 9. Lifecycle

| Operation | Mechanism |
|---|---|
| Identity, standalone | caller-supplied `stream_id`, unique in the namespace |
| Identity, attached | `(namespace, workflow_id, first_execution_run_id, stream_name)`. The first execution run ID is stable across a continue-as-new chain and prevents collisions after workflow ID reuse |
| Create, standalone | `CreateStream`, or implicitly on first `AddMessages` when the caller opts in |
| Create, attached | Materialized by the server when a stream reference is passed to `StartWorkflow` |
| Producer done | `FinishWriting` sets a per-producer fence; the stream stays open for others |
| Close | `CloseStream`, or a pure task fired when the owning execution completes. The owner link is on the **business ID**, not the run ID, so it survives continue-as-new |
| Truncate, explicit | `TrimHistoryBranch` plus advancing `base_offset`, bounded by §8.4 |
| Truncate, cap-driven | `max_items` / `max_bytes` evaluated inline at the end of a successful append (§4.1 step 9). No sweeper: the append transition is already writing, so folding the check into it costs nothing and keeps the cap tight |
| Retention | Side-effect task at `close_time + retention`, then `DeleteHistoryBranch` and `chasm.DeleteExecution` |
| Continue-as-new | Standalone: nothing to do. Attached: copy state to the successor and redirect the old child (§8a.1) |
| Reset | Carry the current head forward, do not rewind (§8a.2) |

Close seals, it does not delete. A closed stream stays readable through retention, which is what removes the shutdown handshake that Workflow Streams needs today.

Archival is out of scope, matching non-workflow CHASM executions today, which take a delete path rather than an archival path. When generic CHASM archival lands, streams pick it up.

---

## 10. Failure analysis

| Scenario | Behaviour |
|---|---|
| Crash between node append and frontier advance | Orphan nodes at or past `head_offset`, invisible to readers. Retry rewrites the same node IDs; the store keeps the highest transaction ID. Trim task reclaims the space (§3.6). |
| Retry writes a **smaller** batch than the failed attempt | Trailing orphan nodes end up below the advancing frontier. The transaction-ID chain rule in `filterHistoryNodes` drops them (§3.5). This is the case clipping alone does not cover. |
| Producer retries after a timeout it did not observe | Dedup on `(producer_id, seq)` returns the original `first_offset`. No double append. The producer never has to reason about whether the append landed. |
| Two producers race | `expected_offset` mismatch returns `AlreadyExists` with the current head. With `owner_epoch`, the stale producer is fenced outright. |
| Producer publishes after `FinishWriting` | `FailedPrecondition` with reason `ProducerFinished`. Other producers are unaffected. |
| Shard failover mid-stream | New owner reloads the component. `owner_epoch` bump fences the old producer. Readers resume from their own offset. The tail cache is rebuilt. |
| Reader disconnects mid-page | No server state to clean up. The reader re-polls from its last offset. |
| Reader polls a truncated range | `OutOfRange` carrying `base_offset`, so the reader can jump forward. |
| Close races an in-flight append | Both serialize through the component transition. Either the append lands before close, or it fails `FailedPrecondition`. |
| Workflow consuming a stream that gets truncated | Prevented by §8.4. |
| Workflow replays a range lost to retention expiry | Retryable workflow task failure with a distinct error, not a nondeterminism error (§8.4). |
| Batch exceeds `transactionSizeLimit` | Split across nodes inside the transition, before commit. |
| One member of a group commit fails validation | Rejected individually; the rest of the group commits (§4.1a). |
| Subscribed workflow's task carries no new messages | An empty range is still recorded, so replay reproduces the observation (§8.2). |
| Retried sequence carries different content | Rejected with `InvalidArgument`. Never deduplicated into a silent data drop (§4.1). |
| Committed node missing under the frontier | `DataLoss`, never a short page (§4.2). |
| Poll addressed at a run that continued as new | Follows `redirect_run_id` and keeps its offset (§8a.1). |
| Append races a reset | Conflicts on the current-run condition; one side retries and follows the redirect (§8a.2). |
| Stream outgrows one Cassandra partition | Cannot happen; offsets roll to a new tree every `bucket_size` (§3.1a). |

---

## 11. Cross-boundary check: Walker

OSS `NewHistoryBranch` ignores `namespaceID`, `workflowID`, and `runID`, but the `HistoryBranchUtil` interface accepts them because the SaaS storage layer overrides it. Minting branches whose tree ID is not a workflow run ID is a change to an assumption Walker may rely on.

This needs a read of `saas-temporal/walker/` and a conversation with that team before this goes beyond prototype. It does not block an OSS prototype, and it is listed here so it is not discovered late.

---

## 12. Explicitly out of scope

- **Cross-shard publish to a stream the producer does not own, atomic with the producer's own state transition.** Producers use `AddMessages`, which is idempotent by offset and reliable. Two-phase commit or an outbox only becomes necessary if atomicity with the producer's transition is required, and token streaming does not require it.
- **Nexus and cross-namespace.** A layer on top.
- **gRPC server-streaming reads.** See §4.4.
- **A non-durable tier.** Reintroduces the tuning knob the design removes.
- **Server-populated rewind metadata.** The field exists; filling it in is a later opt-in.
- **Cassandra.** See §16.
- **A pluggable external backend.** Moving payloads to Redis or similar is a real product option and a working prototype of it exists (`design-comparison.md` §4), but it puts durability outside Temporal, which is the thing this design exists to avoid. The read API here is offset plus long-poll, the same shape an external adapter exposes, so the two could later sit behind one client API without either being rewritten.
- **Push-based in-workflow delivery.** Delivering items to a consuming workflow as signals would put per-item cost back into the consumer's history and hand dedup to user code. Pull-at-task-build (§8) avoids both.

---

## 13. Configuration

| Key | Default | Purpose |
|---|---|---|
| `stream.enabled` | false | Per-namespace kill switch |
| `stream.longPollTimeout` | 20s | Matches history long-poll convention |
| `stream.longPollBuffer` | 3s | Deadline buffer |
| `stream.maxBatchBytes` | 2MB | Bounded by `transactionSizeLimit` |
| `stream.bucketSize` | 100000 | Messages per history-node tree (§3.1a). Immutable per stream once created; changing the default affects new streams only |
| `stream.maxMessagesPerPoll` | 1000 | Read page bound |
| `stream.maxBytesPerPoll` | 4MB | Read page bound |
| `stream.tailCacheBytesPerStream` | 1MB | Fan-out cache |
| `stream.tailCacheBytesPerShard` | 256MB | Aggregate bound |
| `stream.maxConsumeBytesPerTask` | 1MB | Path C attachment bound |
| `stream.maxConsumeItemsPerTask` | 1000 | Path C attachment bound; binds where messages are tiny |
| `stream.deliverOnFlush` | true | Cut a Path C slice at a flush marker rather than only at the size bounds |
| `stream.maxGroupCommitSize` | 16 | Appends coalesced into one transition (§4.1a) |
| `stream.maxSubscribersPerStream` | 0 | 0 = unbounded; present as a safety valve |

No user-facing batching knob. Batching is the client's choice of how many messages to put in one `AddMessages` call, and server-side coalescing is internal. Removing the tuning knobs is a stated goal of the 1-pager, and it is why `maxGroupCommitSize` is an operator dial rather than an API field.

## 14. Metrics

The claims in the high-level design are unmeasured, so the prototype has to emit what settles them:

| Metric | Why |
|---|---|
| persistence round trips per append | the core cost claim |
| conditional writes per append, and realised group size | tests whether group commit does what §4.1a says |
| orphan bytes outstanding, and trim task latency | tests the objection in `design-comparison.md` §5 |
| append to reader-receipt latency, p50 and p99 | the 100ms batching bar |
| tail-cache hit rate | tests the fan-out claim |
| stream count, bytes, and items per namespace | capacity planning and, later, pricing |
| long-poll wakes per delivered message | the CHASM notifier is execution-scoped, so an attached stream's pollers wake on unrelated workflow changes. Wake amplification is a real risk and needs measuring, not assuming |

**Stream and topic names must never be metric tags.** They are user-supplied and unbounded, so tagging on them is a cardinality incident waiting to happen.

---

## 15. Test plan

**Unit** (`chasm/chasmtest`, in-memory engine):
- Offset assignment across batches, including split batches.
- Dedup by `(producer_id, seq)` returns the original offsets.
- `expected_offset` mismatch returns the current head.
- Epoch fencing rejects a stale producer.
- `FinishWriting` fences one producer and leaves others writing.
- Close rejects subsequent appends.
- Truncation floor respects registered consumers.
- Cap-driven truncation fires inline on append and respects the consumer pin.
- Mid-batch read trims correctly at every boundary.
- Topic filter returns the right subset and still advances `next_offset` past filtered-out messages.
- Group commit: N concurrent appends produce contiguous non-overlapping ranges, one transition, and per-member dedup entries; one invalid member does not abort the group.

**Storage-level** (against the real `history_node` store, `common/persistence/tests`):
- **The shrinking-retry case from §3.5.** Write nodes at 100 and 110 under `T1`, then node 100 alone under `T2`, advance the frontier to 105, write node 105 under `T3`, and assert a read of `[100, 120)` never returns the node at 110. This is the single most important test in the suite: it is the case that decides whether reusing `history_node` is sound, and it is the objection an external reviewer will raise first.
- `TrimHistoryBranch` with the committed frontier reclaims orphans and leaves the valid chain intact.
- **Bucket-boundary staleness.** The §3.5 shrinking-retry layout, arranged so the abandoned attempt straddles a bucket boundary and the stale node is the first node of the next bucket. Asserts the chain rule still rejects it once the frontier moves past. This is the test behind the claim in §3.1a that bucketing is safe, and it is the one most likely to surprise us.

**Functional** (`tests/stream_test.go`, against SQLite and Postgres):
- Produce and consume end to end, single and many subscribers.
- Long-poll wakes on append and returns empty on soft timeout.
- Reader below `base_offset` gets `OutOfRange` with a usable floor.
- Stream stays readable after the owning workflow closes.
- Continue-as-new: a poll in flight across the boundary follows the redirect and returns a contiguous offset sequence with no gap and no repeat.
- Reset: the reset run inherits the current head, and an append racing the reset ends up on exactly one of them.
- Paths A and C using `tests/testcore/taskpoller.go:29`, whose `WorkflowTaskHandler func(task) ([]*commandpb.Command, error)` lets a test emit `AddStreamMessages` and read the attached slice directly. **No SDK fork is needed to prove either path.**

**Durability:**
- Kill the server mid-append, restart, assert readers see a prefix and never a gap.
- Assert a producer retry after the kill does not double-append.

**Failover:**
- Force shard movement mid-stream, assert the epoch fence rejects the stale producer and readers resume.

**Replay (Path C):**
- Run to completion, evict the workflow cache, force replay, assert attached slices are byte-identical.
- **Idle-task replay.** A workflow subscribed to a stream that receives nothing for several tasks must replay identically. Assert an empty range was recorded per task, and that replay delivers nothing on those tasks. Without the §8.2 rule this test fails, so it is the guard on that fix.
- **Resolved start offset.** Subscribe from the tail while the stream already has items, complete, replay, and assert the workflow sees the same starting position rather than re-reading from zero.
- **Attachment bound.** A backlog larger than both `maxConsumeItemsPerTask` and `maxConsumeBytesPerTask` is split across tasks, and replay reproduces the same split.
- **Retention loss.** Delete the branch data underneath a workflow that recorded a range, force replay, and assert a retryable task failure rather than a nondeterminism error.

---

## 16. Benchmark methodology

Same workload both ways, on SQLite and Postgres.

Workload: one LLM-shaped producer emitting 40 messages per second of roughly 20 bytes each for 60 seconds, with 1, 5, and 25 concurrent subscribers.

Baseline: today's Workflow Streams pattern (batched Signals plus a polling Update), reproduced as a functional-test workload.

Report:
- persistence round trips per message,
- history bytes written per message,
- Actions per message,
- p50 and p99 time from append to reader receipt,
- marginal cost of the Nth subscriber,
- whether 100ms batching holds up, which is the bar the 1-pager sets.

The benchmark is the deliverable that makes the September 14 decision possible. Everything else is in service of it.

---

## 17. Sequencing

| Stage | Content | Notes |
|---|---|---|
| 0 | Baseline harness and measurements | Nothing to compare against without it |
| 0b | **Storage-level spike** | **Done.** `common/persistence/tests/history_store_stream_log.go`, passing on SQLite and Postgres, with a negative control. Blob opacity and the §3.5 shrinking-retry case both hold |
| 1 | Component, log helpers, unit tests | |
| 1b | CHASM transaction hook (§5) | Raise with the CHASM owner in week one; has a fallback |
| 2 | RPC surface and wiring | `service/frontend/service.go:507`, `service/frontend/fx.go`, `common/api/metadata.go`, `service/frontend/configs/quotas.go` |
| 3 | Long-poll, tail cache, group commit | Where the fan-out and throughput claims get proven |
| 4 | Lifecycle: close, write fence, truncate, retention, owner binding, `ListStreams` | |
| 5 | Path A, workflow publish | Needs the api-go additions |
| 6 | Path C, workflow consume | Highest risk, sequenced last |
| 7 | Benchmark, demo, write-up | |

**API dependency.** Stages 5 and 6 need `go.temporal.io/api` changes: `temporal.api.stream.v1`, `COMMAND_TYPE_ADD_STREAM_MESSAGES`, a `stream_slices` field on `PollWorkflowTaskQueueResponse`, and a `stream_cursors` field on `WorkflowTaskCompletedEventAttributes`. Note there is **no new event type**: the consumed range rides an event that already exists (§8.1). Plan is one branch on `temporalio/api` pinned by pseudo-version rather than a local `replace`, so the branch stays buildable by others. `make update-go-api` is the existing path.

---

## 18. What this design does not answer

- Whether per-row cost on Cassandra changes the batching decision. Out of scope here, and the SQLite and Postgres numbers must not be read as answering it.
- Pricing. The design makes cost track bytes rather than item count, which is the shape the 1-pager wants, but the model is not settled.
- Whether the collection primitive Metablock needs should be built on this or the reverse. This design does not need it, which is a scheduling argument rather than an architectural one.
- Naming.
- The UI story: reconstructing a stream in the Web UI, and dropping the Signal and Update clutter.
- Whether orphan volume under real retry rates stays inside what the eager trim reclaims (§3.6). Instrumented from Stage 0, not answered by design.
- What bounds the cost of cold replay under the §8.3 decision. Reassembling slices on the History read path is cheap while the tail cache is warm and unbounded when it is not. A very old workflow with a long consumed history is the case that needs a limit, and it does not have one yet.
- Whether `GetWorkflowExecutionHistory` should reassemble at all, or only the worker-facing read. The UI and `tctl` share that path, so reassembly there means stream payloads appear in operator tooling that today only sees offsets. That is arguably desirable for debugging and arguably a size and redaction problem. Not decided.
- Cross-cluster replication. History-node data already replicates, so the mechanism is inherited rather than designed, but the conflict semantics for a stream written on two sides of a failover are not worked out. Last-writer-wins is the assumed answer and it is lossy.
