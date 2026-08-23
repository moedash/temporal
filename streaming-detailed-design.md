# Native Streams: Detailed Design

| | |
|---|---|
| Status | Draft for review |
| Ticket | [AI-198](https://temporalio.atlassian.net/browse/AI-198) |
| Author | Moe Dashti |
| Date | 2026-08-23 |
| Companion | `streaming-high-level-design.md` |

This document specifies the implementation. It assumes the high-level design and does not re-argue it. Line references are against `main` at `6805caea5`.

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
}

message ConsumerCursor {
  string workflow_id = 1;
  string run_id = 2;
  int64 offset = 3;
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
  rpc PollMessages(...)   { business_id = "frontend_request.stream_id"; category = API_CATEGORY_LONG_POLL; }
  rpc DescribeStream(...) { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
  rpc CloseStream(...)    { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
  rpc TruncateStream(...) { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
  rpc DeleteStream(...)   { business_id = "frontend_request.stream_id"; category = API_CATEGORY_STANDARD; }
}
```

Options follow `chasm/lib/activity/proto/v1/service.proto`. `business_id` drives shard routing; `API_CATEGORY_LONG_POLL` puts `PollMessages` in the right quota bucket.

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

### 3.2 Offset to node ID

`serializeAppendRawHistoryNodesRequest` rejects `nodeID <= 0` with "eventID cannot be less than 1" (`common/persistence/history_manager.go:429-433`). So:

```
nodeID = offset + 1
```

API offsets start at 0. This mapping is internal and must never leak into the wire protocol.

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

### 3.5 The clip invariant

> **Every read clips to `HeadOffset`. Nothing else is required for correctness.**

Node append and frontier update are not one atomic store operation, and never were for workflow history either. Cassandra's `execution_store.go:110-126` appends history nodes in a loop and then calls `UpdateWorkflowExecution`; SQL does the same at `sql/execution.go:339`.

Two failure shapes, both already handled:

1. **Nodes written, frontier not advanced.** Orphan nodes sit at offsets at or past `HeadOffset`. `MaxEventID` excludes them, so no reader observes them. A retry rewrites the same node IDs with a higher transaction ID.
2. **Duplicate node IDs from a retry.** `filterHistoryNodes` (`history_manager.go:1039-1073`) keeps the highest transaction ID per node ID and drops the rest. The comment at `:1066` states the ordering contract the store provides.

There is no window in which a reader sees a gap, and no window in which two readers disagree about a prefix.

---

## 4. RPCs

### 4.1 `AddMessages`

```
AddMessages(namespace, stream_id, producer_id?, seq?, expected_offset?, owner_epoch?, messages[])
  -> { first_offset, next_offset, head_offset }
```

Handler calls `chasm.UpdateComponent(ctx, ref, (*Stream).AddMessages, req)`. Inside the transition, in this order:

1. `Closed` -> `FailedPrecondition` with reason `StreamClosed`.
2. **Dedup.** If `producer_id` set and `producers[producer_id].seq >= seq`, return the recorded `first_offset` and `count` without appending. Idempotent retry.
3. **Fence.** If `owner_epoch` supplied and below `state.owner_epoch`, return `FailedPrecondition` with reason `ProducerFenced`.
4. **Compare-and-append.** If `expected_offset` supplied and it differs from `head_offset`, return `AlreadyExists` carrying `head_offset` so the caller can resynchronise.
5. Serialize `StreamMessageBatch`, splitting if over `transactionSizeLimit`.
6. Emit pending log appends (§5) at `nodeID = head_offset + 1`.
7. `head_offset += len(messages)`; `last_txn_id = txnID`; record the producer cursor.

Acknowledge after the transaction commits. `first_offset` is the offset of the first message; the caller derives per-message offsets by position.

`producer_id` and `expected_offset` are alternative idempotency mechanisms. `producer_id` suits a retrying activity; `expected_offset` suits a caller that already tracks position. Supplying neither gives at-least-once, which is a valid choice for a caller that does not care.

### 4.2 `PollMessages`

```
PollMessages(namespace, stream_id, from_offset, max_items, max_bytes, wait_new_messages, wait_timeout)
  -> { messages[], first_offset, next_offset, closed, close_reason, head_offset }
```

1. `from_offset < base_offset` -> `OutOfRange` with reason `Truncated`, carrying `base_offset` so the reader can jump forward rather than fail.
2. `from_offset > head_offset` -> `InvalidArgument`.
3. `from_offset < head_offset`: serve. Tail cache first (§6); on miss, `ReadRawHistoryBranch`. Trim to `max_items` and `max_bytes`. Return.
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

A workflow subscribes by recording a `ConsumerCursor`. Thereafter, when the server builds a workflow task for that execution, it attaches the pending slice and records the range:

```
PollWorkflowTaskQueueResponse.stream_slices: [
  { stream_id, from_offset, to_offset, messages[] }
]
```

and one event per task per stream:

```
WorkflowStreamConsumed { stream_id, from_offset, to_offset }
```

History grows with workflow tasks, not with messages.

### 8.2 Determinism

On replay the server reads `[from_offset, to_offset)` from the same branch and attaches the same bytes. This is deterministic because:

- the log is immutable, so a given offset always holds the same bytes;
- the range is recorded in history, so it does not depend on when replay happens;
- `filterHistoryNodes` resolves duplicate node IDs the same way on every read.

No timing dependency, so no versioning hazard.

### 8.3 Truncation interlock

Replay needs the bytes to still exist. So truncation is bounded:

```
effective_base = min(requested_base, min over consumers of consumer.offset)
```

`TruncateStream` below a registered consumer cursor is rejected with `FailedPrecondition`. A consumer is deregistered when its workflow closes, which releases the floor.

This is the one place where a consumer constrains the stream, and it is unavoidable: recording a cursor instead of the data means the data has to outlive the cursor.

### 8.4 Bounded attachment and the wake exception

The attached slice is capped by `stream.maxConsumeBytesPerTask`. If `head_offset - cursor` exceeds it, attach a prefix, record only that range, and schedule a follow-up workflow task.

That is the single intentional exception to "publishing never wakes a workflow". Publishing does not. An **active in-workflow subscription** does, because the workflow asked to be woken. Worth stating explicitly, because it is the property that keeps Path C from silently reintroducing the cost Path B removes. A workflow that does not subscribe is never woken by stream traffic.

### 8.5 Continue-as-new and reset

- **Continue-as-new**: the cursor is workflow state, carried in the continue-as-new input. The stream is untouched. Nothing is duplicated or dropped.
- **Reset**: the workflow rewinds; the stream does not. Cursor events before the reset point are intact, so replay works, and the new run re-consumes from the cursor as of that point. Relative to the abandoned run, some messages are delivered twice. That is visible to the application by design, matching the decision that rewinds are the application's concern rather than something the system hides.

---

## 9. Lifecycle

| Operation | Mechanism |
|---|---|
| Create, standalone | `CreateStream`, or implicitly on first `AddMessages` when the caller opts in |
| Create, attached | Materialized by the server when a stream reference is passed to `StartWorkflow` |
| Close | `CloseStream`, or a pure task fired when the owning execution completes |
| Truncate | `TrimHistoryBranch` plus advancing `base_offset`, bounded by §8.3 |
| Cap | `max_items` / `max_bytes` in `StreamLifecycle` drive automatic truncation |
| Retention | Side-effect task at `close_time + retention`, then `DeleteHistoryBranch` and `chasm.DeleteExecution` |
| Continue-as-new | No handling required; the stream is not in the workflow's history |

Close seals, it does not delete. A closed stream stays readable through retention, which is what removes the shutdown handshake that Workflow Streams needs today.

---

## 10. Failure analysis

| Scenario | Behaviour |
|---|---|
| Crash between node append and frontier advance | Orphan nodes at or past `head_offset`, invisible to readers. Retry rewrites the same node IDs; the store keeps the highest transaction ID. |
| Producer retries after a timeout it did not observe | Dedup on `(producer_id, seq)` returns the original `first_offset`. No double append. |
| Two producers race | `expected_offset` mismatch returns `AlreadyExists` with the current head. With `owner_epoch`, the stale producer is fenced outright. |
| Shard failover mid-stream | New owner reloads the component. `owner_epoch` bump fences the old producer. Readers resume from their own offset. The tail cache is rebuilt. |
| Reader disconnects mid-page | No server state to clean up. The reader re-polls from its last offset. |
| Reader polls a truncated range | `OutOfRange` carrying `base_offset`, so the reader can jump forward. |
| Close races an in-flight append | Both serialize through the component transition. Either the append lands before close, or it fails `FailedPrecondition`. |
| Workflow consuming a stream that gets truncated | Prevented by §8.3. |
| Batch exceeds `transactionSizeLimit` | Split across nodes inside the transition, before commit. |

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
- **Cassandra.** See §14.

---

## 13. Configuration

| Key | Default | Purpose |
|---|---|---|
| `stream.enabled` | false | Per-namespace kill switch |
| `stream.longPollTimeout` | 20s | Matches history long-poll convention |
| `stream.longPollBuffer` | 3s | Deadline buffer |
| `stream.maxBatchBytes` | 2MB | Bounded by `transactionSizeLimit` |
| `stream.maxMessagesPerPoll` | 1000 | Read page bound |
| `stream.maxBytesPerPoll` | 4MB | Read page bound |
| `stream.tailCacheBytesPerStream` | 1MB | Fan-out cache |
| `stream.tailCacheBytesPerShard` | 256MB | Aggregate bound |
| `stream.maxConsumeBytesPerTask` | 1MB | Path C attachment bound |
| `stream.maxSubscribersPerStream` | 0 | 0 = unbounded; present as a safety valve |

No user-facing batching knob. Batching is the client's choice of how many messages to put in one `AddMessages` call, and server-side coalescing is internal. Removing the tuning knobs is a stated goal of the 1-pager.

---

## 14. Test plan

**Unit** (`chasm/chasmtest`, in-memory engine):
- Offset assignment across batches, including split batches.
- Dedup by `(producer_id, seq)` returns the original offsets.
- `expected_offset` mismatch returns the current head.
- Epoch fencing rejects a stale producer.
- Close rejects subsequent appends.
- Truncation floor respects registered consumers.
- Mid-batch read trims correctly at every boundary.

**Functional** (`tests/stream_test.go`, against SQLite and Postgres):
- Produce and consume end to end, single and many subscribers.
- Long-poll wakes on append and returns empty on soft timeout.
- Reader below `base_offset` gets `OutOfRange` with a usable floor.
- Stream stays readable after the owning workflow closes.
- Continue-as-new leaves the stream unaffected.
- Paths A and C using `tests/testcore/taskpoller.go:29`, whose `WorkflowTaskHandler func(task) ([]*commandpb.Command, error)` lets a test emit `AddStreamMessages` and read the attached slice directly. **No SDK fork is needed to prove either path.**

**Durability:**
- Kill the server mid-append, restart, assert readers see a prefix and never a gap.
- Assert a producer retry after the kill does not double-append.

**Failover:**
- Force shard movement mid-stream, assert the epoch fence rejects the stale producer and readers resume.

**Replay (Path C):**
- Run to completion, evict the workflow cache, force replay, assert attached slices are byte-identical.

---

## 15. Benchmark methodology

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

## 16. Sequencing

| Stage | Content | Notes |
|---|---|---|
| 0 | Baseline harness and measurements | Nothing to compare against without it |
| 1 | Component, log helpers, unit tests | Verify blob opacity first; it is cheap and load-bearing |
| 1b | CHASM transaction hook (§5) | Raise with the CHASM owner in week one; has a fallback |
| 2 | RPC surface and wiring | `service/frontend/service.go:507`, `service/frontend/fx.go`, `common/api/metadata.go`, `service/frontend/configs/quotas.go` |
| 3 | Long-poll and tail cache | Where the fan-out claim gets proven |
| 4 | Lifecycle: close, truncate, retention, owner binding | |
| 5 | Path A, workflow publish | Needs the api-go additions |
| 6 | Path C, workflow consume | Highest risk, sequenced last |
| 7 | Benchmark, demo, write-up | |

**API dependency.** Stages 5 and 6 need `go.temporal.io/api` changes: `temporal.api.stream.v1`, `COMMAND_TYPE_ADD_STREAM_MESSAGES`, a `stream_slices` field on `PollWorkflowTaskQueueResponse`, and the `WorkflowStreamConsumed` event type. Plan is one branch on `temporalio/api` pinned by pseudo-version rather than a local `replace`, so the branch stays buildable by others. `make update-go-api` is the existing path.

---

## 17. What this design does not answer

- Whether per-row cost on Cassandra changes the batching decision. Out of scope here, and the SQLite and Postgres numbers must not be read as answering it.
- Pricing. The design makes cost track bytes rather than item count, which is the shape the 1-pager wants, but the model is not settled.
- Whether the collection primitive Metablock needs should be built on this or the reverse. This design does not need it, which is a scheduling argument rather than an architectural one.
- Naming.
- The UI story: reconstructing a stream in the Web UI, and dropping the Signal and Update clutter.
