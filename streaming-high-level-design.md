# Native Streams: High-Level Design

| | |
|---|---|
| Status | Draft for review |
| Ticket | [AI-198](https://temporalio.atlassian.net/browse/AI-198), epic [AI-37](https://temporalio.atlassian.net/browse/AI-37) |
| Project | D1, Native streaming (Win the Agent Loop) |
| Author | Moe Dashti |
| Date | 2026-08-23 |
| Companion | `streaming-detailed-design.md` |

This is a clean-room design, derived from Temporal's storage invariants. It was written without building on the earlier prototype branches; comparing notes with those is a separate exercise, deliberately left until after this design settles.

---

## 1. Problem

Interactive agents produce a continuous stream of tokens, tool calls, reasoning traces, and progress updates. Users need to see them as they happen.

Today the answer is **Workflow Streams** (Public Preview): an Activity batches output into **Signals**, and an app server long-polls the workflow with a `poll_events` **Update**. The developer-facing shape is right. The mechanics are not:

- Batching intervals sit at seconds, not milliseconds, to amortise per-item overhead.
- Items land in the workflow's Event History, so they count against the 50MB cap, are re-read on every replay, and are duplicated or dropped across continue-as-new.
- `MaximumSignalsPerExecution` defaults to 10000 (`common/dynamicconfig/constants.go:2630`). A token-per-signal stream exhausts that inside one long response.
- At most 10 concurrent subscribers.
- The stream is unreadable once the workflow closes, so producer and consumer have to coordinate a shutdown.
- Cost. Customers describe it as a non-starter, and several run Redis alongside Temporal instead.

The last point is the commercial one. Streaming is cited in lost and degraded accounts (Replit, Adobe, Dust, Harvey), and competitors market our lack of a stream primitive as a differentiator.

## 2. First principles

### 2.1 What Temporal is, mechanically

Strip away the programming model and Temporal is a sharded, single-writer, fenced state store. Every entity hashes to one history shard, that shard is owned by one history host at a time under `range_id` fencing, and all mutations linearize through that owner. That single serialization point is the whole source of Temporal's consistency.

Behind that writer sit **two storage shapes**, and they have opposite cost curves:

| | Mutable state | History nodes |
|---|---|---|
| Access pattern | read-modify-write | append-only |
| Addressing | by entity | by offset, within a branch |
| Cost of a change | proportional to **total size** | proportional to the **delta** |
| Read granularity | whole blob, loaded eagerly | paged range `[min, max)` |
| Practical size bound | yes | none |
| Already has | fork, conditional update | fork, trim, delete, range read, replication |

Temporal's own Workflow implementation exploits this asymmetry: the events go in the log, and only the summary goes in mutable state.

### 2.2 What a stream is, mechanically

An ordered sequence of immutable opaque blobs, written by one logical producer at a time, read concurrently by many readers each holding their own cursor, terminated by a close marker.

Read that against the table above:

- A stream is **never** read-modify-write. Nothing mutates an item after it is written.
- A stream is **unbounded**. That is the requirement, not an accident.
- A stream is **not a decision input**. The producer's next action does not depend on any consumer.

A stream is exactly the append-only log shape, and exactly not the mutable-state shape.

### 2.3 Where the cost comes from today

Workflow Streams have no choice but to use the wrong shape, because the only append-only log a user can write to is workflow history, and every append to it schedules a workflow task. Per batch, that buys:

- a `WorkflowExecutionSignaled` event plus a mutable-state update,
- a `WorkflowTaskScheduled` event plus a transfer-task row,
- a full worker round trip (matching dispatch, `RecordWorkflowTaskStarted` re-taking the workflow lock, replay, `RespondWorkflowTaskCompleted` as another two-write transaction),
- and then a polling Update to get the data back out, which is another state transition.

None of that is buying durability. It is buying *workflow semantics* for data that has none.

### 2.4 The three separations

Each separation removes exactly one cost centre, and each rests on something Temporal already relies on.

**1. The stream gets its own log, not the workflow's.**

The history-node store is already documented as decoupled from workflow concepts (`common/persistence/data_interfaces.go:1158`: "V2 regards history events growing as a tree, decoupled from workflow concepts"). `NewHistoryBranch` (`common/persistence/history_branch_util.go:49`) ignores namespace, workflow, and run entirely; a branch token is just `{TreeId, BranchId, Ancestors}`.

So give each stream its own branch. That alone delivers: no history bloat, no 50MB cap, no continue-as-new entanglement, readable after the workflow closes, unbounded size, and independent retention.

**2. Appending does not schedule a workflow task.**

A token is data *produced by* an execution, not a decision input *to* it. Nothing in the workflow's state machine advances because a token arrived. Scheduling a workflow task for it is a category error.

Removing it removes the transfer-task row and the worker round trip, which is the dominant per-batch cost.

**3. Reading is not a state transition.**

The reader's position is the reader's state. If the reader supplies its offset, the server keeps no durable per-subscriber record. Subscriber count stops being a durable-state problem and becomes a memory problem, so the limit of 10 goes away and a poll costs no Actions.

## 3. The model

### 3.1 Entity

A **Stream** is a first-class entity addressed by `(namespace, streamId)`, implemented as a CHASM component. It exists in two arrangements:

- **Standalone**: its own CHASM execution, routed by `streamId`. Independent of any workflow.
- **Attached**: a subcomponent of a workflow's execution, so it is co-located on that workflow's shard.

Both are readable and writable by clients over the same API. The difference is only which shard owns it and, therefore, whether the owning workflow can publish to it for free.

### 3.2 What lives where

Only the **frontier** needs to be linearized, so only the frontier lives in mutable state:

```
Stream (CHASM component; size is O(1) regardless of stream length)
  BranchToken       its own history-node branch
  HeadOffset        visibility frontier; readers never see at or past this
  BaseOffset        truncation floor
  LastTxnID         node chaining
  Closed, CloseReason
  OwnerEpoch        producer fence
  ProducerCursors   per-producer dedup, bounded by producer count
  Consumers         registered in-workflow cursors, bounded
  Owner             optional execution ref, for lifecycle only
```

Payload bytes go to the stream's own branch and never enter the CHASM tree.

That last sentence is the design's main claim. It means the component does not grow with the stream, there is no segment-index to blow up mutable state, and **there is no dependency on CHASM partial reads** (`OSS-4917` and `OSS-4918`, both still `To Do`). The history-node store already does paged range reads; that is its job.

### 3.3 Guarantees

- **Total order** within a stream, by offset.
- **Exactly-once write**: an acknowledged append appears exactly once, under producer retries, shard failover, and concurrent producers.
- **Durable before acknowledged.** No fire-and-forget tier. Durability is the reason to be on Temporal at all.
- **Readers see a prefix.** A reader never sees a gap and never sees an item that a later reader will not see.
- **At-least-once delivery to the reader, made exactly-once by the reader's cursor.** The reader owns its offset, so a duplicate poll is idempotent.

### 3.4 Why exactly-once needs no new commit protocol

The write pair here is (append log nodes, then conditionally advance the frontier). That is what workflow history has done for a decade, and the invariant that makes it safe is one line:

> **Readers clip to `HeadOffset`.**

A crash between the two steps leaves orphan nodes at offsets at or past `HeadOffset`, which no reader can observe. A producer retry rewrites the same node IDs with a higher transaction ID, and the store's documented larger-`TransactionID`-wins rule resolves it. Nothing is lost, nothing is double-delivered.

With that invariant in place, exactly-once reduces to two single-field checks inside the conditional update that is already happening:

- **Producer fence.** `OwnerEpoch`, bumped on ownership change. A stale producer's update fails.
- **Idempotency.** The producer supplies `(producerId, seq)` or an `expectedOffset`. A retry returns the original result instead of appending again.

This is the design's second claim: getting the shape right removes the need for a two-phase commit and the machinery that goes with it.

## 4. Access paths

### 4.1 Path B: off-shard producer

The common case. LLM tokens come from an Activity calling the model, not from workflow code.

```
Activity / client ──AddMessages(streamId, items)──▶ stream's shard
                                                        │
                                          append blob to stream branch
                                          advance HeadOffset
                                                        │
App server        ◀──PollMessages(from=N, wait)─────────┘
                  ──SSE──▶ browser
```

One gRPC hop, one log append, one small conditional update. The workflow is not involved at all: no workflow task, no history event, no worker round trip, no lock on the workflow.

### 4.2 Path A: the workflow publishes to its own stream

For application-level progress updates ("planning", "calling search", "writing file"), which Johann's own analysis argues matter more than token streaming once models get fast.

An attached stream is on the workflow's shard, under the workflow's lock, inside the workflow task's existing commit. A new `AddStreamMessages` command appends to the stream's branch as part of that transaction.

The marginal cost of publishing is one extra blob in a write that was already happening. Zero history events, zero extra round trips.

### 4.3 Path C: the workflow consumes a stream

This is the case the 2026-07-23 discussion recorded as having no proposed solution, and the Bellevue session deferred. The design admits an answer.

**Record the cursor in history, not the data.**

The server attaches the pending slice `[cursor, HeadOffset)` to the workflow task response, and writes exactly one event per task:

```
WorkflowStreamConsumed { streamId, fromOffset, toOffset }
```

On replay the server re-reads the same offset range from the same branch. That is deterministic by construction, because the log is immutable and offset-addressed. Nothing about the replay depends on timing.

History then grows with **workflow tasks, not with items**. A 50,000-token response consumed across 8 workflow tasks costs 8 small events instead of 50,000 large ones.

This is the third claim, and it inverts the framing from the July discussion. That discussion looked for ways to make many small workflow tasks cheaper (pipelining). The cheaper move is to make one workflow task carry many items.

One intentional exception falls out here. Publishing never wakes a workflow. But an *active in-workflow subscription* does: if a subscribed workflow's cursor is behind `HeadOffset` when its task completes, the server schedules another task. That is the workflow asking to be woken, which is a different thing from a producer waking it.

## 5. Lifecycle

- **Creation** is explicit for standalone streams, and implicit for attached ones (pass a stream reference to `StartWorkflow` and the server materialises it), so producers and consumers never have to coordinate on who creates it.
- **Close** is explicit, and automatic when the owning execution completes. Close seals the stream; it does not delete it.
- **Retention** works like a workflow's. A closed stream stays readable through retention, then `DeleteHistoryBranch` reclaims it.
- **Truncate** advances `BaseOffset` and calls `TrimHistoryBranch`. A reader below `BaseOffset` gets a distinguishable error carrying `BaseOffset`, so it can jump forward rather than fail.
- **Continue-as-new** needs no handling. The stream is not in the workflow's history, so there is nothing to duplicate or drop.

## 6. Cost

Per 100ms batch, steady state. The middle column is what we measure in Stage 0, not an estimate.

| | Workflow Streams today | This design (Path B) |
|---|---|---|
| gRPC hops | 2, plus a worker round trip | 1 |
| History events written | 2 per signal, plus WFT completion | 0 |
| Log appends | 1, into the workflow's own history | 1, into the stream's branch |
| Mutable-state updates | 2 or more | 1, small and fixed-size |
| Transfer-task rows | 1 | 0 |
| Worker round trips | 1 | 0 |
| Workflow lock acquisitions | 3 or more | 0 |
| Counts against 50MB history | yes | no |
| Hard ceiling | 10000 signals per execution | none |
| Cost of the Nth subscriber | an Update per poll | a memcopy |

Substantiating this table against a real workload is the point of the prototype. The claim to test is that 100ms batching becomes practical, which is the bar the Native Streams 1-pager sets.

## 7. Non-goals

- **Sub-100ms realtime voice.** Different point in the design space; durable-per-item is the wrong trade there.
- **Kafka-scale firehose.** Many modest-volume streams, not few enormous ones.
- **Nexus and cross-namespace.** A layer on top, deliberately later.
- **A non-durable tier for token deltas.** Splitting durable application events from ephemeral deltas reintroduces exactly the tuning knob we are trying to remove. If the cost work lands, the split is unnecessary.
- **Automatic rewind handling.** On workflow retry or reset the stream keeps appending; the rewind surfaces as item metadata. Hiding it from the user would be worse than exposing it.
- **Cross-shard atomic publish** to a stream the producer does not own. Producers use the RPC, which is already idempotent by offset. Two-phase commit only becomes necessary if the publish must be atomic with the *producer's own* state transition, and token streaming does not need that.

## 8. Dependencies and risks

**The one framework change.** `ChasmTree` (`service/history/interfaces/chasm_tree.go:19-53`) gives a component no way to contribute append-log batches at transaction close. `UpdateWorkflowExecutionRequest.UpdateWorkflowEvents` is already `[]*WorkflowEvents`, each carrying its own `BranchToken`, so multi-branch appends in one transaction are structurally supported; the tree just cannot reach them. This hook is what makes Paths A and B single-round-trip. It has an owner outside this project (Yichao, CHASM) and should be raised in week one. If it slips, both paths still work as two persistence calls with the same invariant and one extra round trip.

**Blob framing.** The design assumes the raw history-node paths (`AppendRawHistoryNodes` / `ReadRawHistoryBranch`) treat the blob as opaque. If any surrounding machinery insists on `historypb.History` framing, items get wrapped in a synthetic event. This needs verifying before implementation starts, and it is cheap to verify.

**Walker.** OSS `NewHistoryBranch` ignores namespace, workflow, and run, but the interface accepts them because the SaaS storage layer uses them. Minting branches that are not tied to a run needs a check against `saas-temporal/walker/` before this goes past prototype.

**Notifier scaling.** `service/history/chasm_notifier.go` uses a single global mutex and carries TODOs to that effect. Fine for a prototype, real work for fan-out at scale.

**Cassandra.** Deliberately out of scope for the prototype, and it is the open question gating the batching decision for production. SQLite and Postgres numbers must not be read as answering it.

## 9. Open questions

- Offset as an integer or an opaque token. Integers are better ergonomics; tokens leave room to change the addressing later.
- Whether per-item metadata (workflow, run, original run, attempt) is populated by the server, and whether it is on by default. The rewind model depends on it existing; whether we fill it in is separable.
- Pricing. Data transferred, storage, and active minutes are the candidates. This design deliberately makes cost track bytes rather than item count, which is the shape the 1-pager asks for.
- Naming. "Stream" collides with Kafka Streams. Using `stream` for now.
- Whether the collection primitive that Metablock needs should be built on this, or the other way round. This design does not need it, which is a scheduling argument, not an architectural one.
