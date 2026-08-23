# Native Streams: High-Level Design

| | |
|---|---|
| Status | Draft for review |
| Ticket | [AI-198](https://temporalio.atlassian.net/browse/AI-198), epic [AI-37](https://temporalio.atlassian.net/browse/AI-37) |
| Project | D1, Native streaming (Win the Agent Loop) |
| Author | Moe Dashti |
| Date | 2026-08-23 |
| Companion | `streaming-detailed-design.md`, `design-comparison.md` |

This is a clean-room design, derived from Temporal's storage invariants. It was written without reading the earlier prototypes. Those have since been compared against it in `design-comparison.md`, and the changes that comparison produced are folded in here.

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
  ProducerCursors   per-producer dedup and write fence, bounded by producer count
  Consumers         registered in-workflow cursors, bounded
  Owner             optional execution ref, for lifecycle only
  Lifecycle         retention, item and byte caps
```

Payload bytes go to the stream's own branch and never enter the CHASM tree.

That last sentence is the design's main claim. It means the component does not grow with the stream, there is no segment-index to blow up mutable state, and **there is no dependency on CHASM partial reads** (`OSS-4917` and `OSS-4918`, both still `To Do`). The history-node store already does paged range reads; that is its job.

### 3.3 Guarantees

- **Total order** within a stream, by offset.
- **Exactly-once write**: an acknowledged append appears exactly once, under producer retries, shard failover, and concurrent producers.
- **Durable before acknowledged.** No fire-and-forget tier. Durability is the reason to be on Temporal at all.
- **Readers see a prefix.** A reader never sees a gap and never sees an item that a later reader will not see.
- **At-least-once delivery to the reader, made exactly-once by the reader's cursor.** The reader owns its offset, so a duplicate poll is idempotent. In-workflow consumption is exactly-once outright, because the delivered range is recorded (§4.3).
- **The server never sees plaintext.** Items are opaque blobs on the write path, in storage, and on the read path. The payload codec runs entirely in the SDK. This falls out of using the raw append and range-read paths, which never deserialize.
- **Multiple topics per stream**, filtered at subscribe time, so cross-topic ordering is preserved for callers who want it.
- **A producer can finish without closing the stream.** A write fence lets one producer declare it is done while others keep publishing. Closing is a separate, stream-wide act.

### 3.4 Why exactly-once needs no new commit protocol

The write pair here is (append log nodes, then conditionally advance the frontier). That is what workflow history has done for a decade, and two mechanisms already in the store make it safe.

**First, readers clip to `HeadOffset`.** A crash between the two steps leaves orphan nodes at offsets at or past `HeadOffset`, which no reader can observe.

**Second, the store enforces a transaction-ID chain.** `filterHistoryNodes` (`common/persistence/history_manager.go:1039-1073`) does more than prefer the higher transaction ID when two rows share a node ID. It requires transaction IDs to be non-decreasing as node IDs increase, and discards any node whose transaction ID went backwards. Its own comment states the rule: "event batches with larger node ID -> batch with lower transaction ID is invalid (happens before)".

The second mechanism is what covers the case the first does not. Suppose an append writes two nodes, at offsets 100 and 110, then fails. A retry writes only one node at 100, and the frontier advances past it. A later append writes at 105. The stale node at 110 is now *below* the frontier, so clipping alone would expose it. The chain rule drops it, because its transaction ID precedes the one at 105.

With both in place, exactly-once reduces to two single-field checks inside the conditional update that is already happening:

- **Producer fence.** `OwnerEpoch`, bumped on ownership change. A stale producer's update fails.
- **Idempotency.** The producer supplies `(producerId, seq)` or an `expectedOffset`. A retry returns the original result instead of appending again.

This is the design's second claim: getting the storage shape right removes the need for a prepare phase, a two-phase commit, and the machinery that goes with them.

Orphan rows still occupy space until reclaimed. `TrimHistoryBranch` exists for exactly this, takes a known-good frontier as its input, and the Stream component holds that frontier, so the sweep is a cheap task fired after a failed append rather than a slow background scavenger. This matters because streams generate orphans more often than workflow history does, through publish retries and truncate races.

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

The 2026-07-23 sync recorded no proposed solution for this and the Bellevue session deferred it. That has since changed: **Option 7** in the [canonical options doc](https://app.notion.com/p/3b28fc567738812f8c67ca3ebdf9ce38), added from the 2026-08-14 call, targets bucket 2 by holding a workflow task open and reading an external store directly. So what follows is an alternative to Option 7, not the only answer on the table. `design-comparison.md` §4 puts the two side by side.

**Record the cursor in history, not the data.**

The server attaches the pending slice `[cursor, HeadOffset)` to the workflow task response, and records the range it delivered on the `WorkflowTaskCompleted` event that closes that task:

```
WorkflowTaskCompletedEventAttributes.stream_slices: [
  { streamId, fromOffset, toOffset }
]
```

On replay the server re-reads the same offset range from the same branch. That is deterministic by construction, because the log is immutable and offset-addressed. Nothing about the replay depends on timing.

Two details make it actually correct rather than nearly correct:

- **The range is recorded on every task where the stream is subscribed, including when it is empty.** A task in which the subscription observed nothing is a fact replay has to reproduce, not an absence of a fact. Recording only non-empty deliveries would let replay hand the workflow items it did not have at that point, and the divergence would surface much later as an unrelated nondeterminism error. Riding the existing `WorkflowTaskCompleted` event rather than writing a separate event is what makes the empty case free.
- **The first record for a subscription carries the resolved start offset**, because "start from the current tail" is otherwise a nondeterministic decision made at subscribe time.

History then grows with **workflow tasks, not with items**, and it adds no events at all. A 50,000-token response consumed across 8 workflow tasks costs 8 small attribute entries instead of 50,000 events.

This is the third claim, and it inverts the framing from the July discussion. That discussion looked for ways to make many small workflow tasks cheaper (pipelining). The cheaper move is to make one workflow task carry many items.

Two properties fall out of putting the boundary at the workflow task:

- **Segmentation is not a problem.** One slice arrives per task, the SDK drains it once, and replay drains an identical slice once. There is no need to reconstruct how many times the consumer was woken within a task, which is the hardest part of doing this from the client side.
- **Publishing still never wakes a workflow, but an active subscription does.** If a subscribed workflow's cursor is behind `HeadOffset` when its task completes, the server schedules another task. That is the workflow asking to be woken, which is a different thing from a producer waking it. A workflow that does not subscribe is never woken by stream traffic.

## 5. Lifecycle

- **Creation** is explicit for standalone streams, and implicit for attached ones (pass a stream reference to `StartWorkflow` and the server materialises it), so producers and consumers never have to coordinate on who creates it.
- **Identity.** An attached stream is keyed on `(namespace, workflowId, firstExecutionRunId, streamName)`. The first execution run ID keeps the identity stable across a continue-as-new chain while preventing collisions after workflow ID reuse.
- **Write fence.** A producer can declare it has finished writing without closing the stream. The fence orders behind everything that producer published before it, so the claim holds even with concurrent callers.
- **Close** is explicit, and automatic when the owning execution completes. Close seals the stream; it does not delete it. The owner link is on the business ID, so it survives continue-as-new.
- **Retention** works like a workflow's. A closed stream stays readable through retention, then `DeleteHistoryBranch` reclaims it.
- **Truncate** advances `BaseOffset` and calls `TrimHistoryBranch`. A reader below `BaseOffset` gets a distinguishable error carrying `BaseOffset`, so it can jump forward rather than fail. Cap-driven truncation is evaluated inline at the end of a successful append rather than by a background sweeper, and it is pinned by any registered in-workflow consumer's cursor.
- **Continue-as-new** needs no handling. The stream is not in the workflow's history, so there is nothing to duplicate or drop.

## 6. Positioning and the throughput ceiling

The target is **many modest-volume streams**, not few enormous ones: thousands to millions of concurrent streams per namespace, tens to thousands of items each, up to a few hundred items per second on a hot one. For cross-business aggregation the right tool is Kafka, and this design does not try to be that.

That positioning is not a marketing choice, it follows from the architecture. A stream linearizes through one CHASM execution, so its throughput ceiling is the transition rate on a single execution. On Cassandra that is a lightweight-transaction rate per partition, conservatively budgeted at around 100 per second.

Two things keep us under it:

- **One transition per append, not two.** Because there is no prepare phase, an append costs one conditional update plus one non-conditional log write. A design that needs prepare-then-commit pays two.
- **Group commit.** Concurrent appends to the same stream coalesce into a single transition, dividing the per-append transition cost by the group size. Combined with client-side batching of items into one call, this is what puts the target comfortably inside the ceiling.

A stream that consistently needs a large group to keep up is a signal to split traffic across streams. That is the application's call, not a server tuning knob.

## 7. Cost

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
| Hard ceiling | 10000 signals per execution | per-stream transition rate (§6) |
| Cost of the Nth subscriber | an Update per poll | a memcopy |
| Conditional writes per batch | 2 or more | 1, divided by the group-commit size |

Substantiating this table against a real workload is the point of the prototype. The claim to test is that 100ms batching becomes practical, which is the bar the Native Streams 1-pager sets.

## 8. Non-goals

- **Sub-100ms realtime voice.** Different point in the design space; durable-per-item is the wrong trade there.
- **Kafka-scale firehose.** Many modest-volume streams, not few enormous ones.
- **Nexus and cross-namespace.** A layer on top, deliberately later.
- **A non-durable tier for token deltas.** Splitting durable application events from ephemeral deltas reintroduces exactly the tuning knob we are trying to remove. If the cost work lands, the split is unnecessary.
- **Automatic rewind handling.** On workflow retry or reset the stream keeps appending; the rewind surfaces as item metadata. Hiding it from the user would be worse than exposing it.
- **Cross-shard atomic publish** to a stream the producer does not own. Producers use the RPC, which is already idempotent by offset. Two-phase commit only becomes necessary if the publish must be atomic with the *producer's own* state transition, and token streaming does not need that.
- **A pluggable external backend.** This is Option 7, and it is a legitimate product option with a working prototype behind it. It is not this design, because it moves durability outside Temporal, which the Native Streams 1-pager currently rules out as a principle. The two are not exclusive though: our read contract is append plus read-from-offset, the same contract Option 7's pluggable store expects, so a Temporal-native stream could sit behind Option 7's SDK model as one provider among several. `design-comparison.md` §4 argues that sequence is better than a fork.

## 9. Dependencies and risks

**The one framework change.** `ChasmTree` (`service/history/interfaces/chasm_tree.go:19-53`) gives a component no way to contribute append-log batches at transaction close. `UpdateWorkflowExecutionRequest.UpdateWorkflowEvents` is already `[]*WorkflowEvents`, each carrying its own `BranchToken`, so multi-branch appends in one transaction are structurally supported; the tree just cannot reach them. This hook is what makes Paths A and B single-round-trip. It has an owner outside this project (Yichao, CHASM) and should be raised in week one. If it slips, both paths still work as two persistence calls with the same invariant and one extra round trip.

**Blob framing and orphan rejection: verified.** Both properties the storage choice rests on now have running tests (`common/persistence/tests/history_store_stream_log.go`), passing on SQLite and Postgres:

- an arbitrary non-proto blob round-trips byte-identical on a branch whose tree ID is not a run ID;
- a stale node left by a shrinking retry is dropped once the frontier moves past it, on both the parsing and the raw read path.

The second was checked with a negative control: giving the stale node a higher transaction ID makes the test fail, so it is not passing vacuously. One detail that changes nothing but is worth knowing: the contiguity check that catches the bad case lives on the parsing path, which a stream does not use, so for streams the transaction-ID chain is the only defence. It holds, and the raw-path assertion covers it.

**Walker.** OSS `NewHistoryBranch` ignores namespace, workflow, and run, but the interface accepts them because the SaaS storage layer uses them. Minting branches that are not tied to a run needs a check against `saas-temporal/walker/` before this goes past prototype.

**Notifier scaling.** `service/history/chasm_notifier.go` uses a single global mutex and carries TODOs to that effect. Fine for a prototype, real work for fan-out at scale.

**Cassandra.** Deliberately out of scope for the prototype, and it is the open question gating the batching decision for production. SQLite and Postgres numbers must not be read as answering it. The per-stream ceiling in §6 is a budgeted figure carried over from Johann's analysis, not something we have measured.

**Orphan volume.** The eager trim assumes failed appends are occasional. If real retry rates under load produce orphans faster than the trim reclaims them, a periodic sweeper comes back. Worth instrumenting from the first benchmark rather than discovering later.

**This is a design, not working code.** Both prototypes it is compared against run today, one with a verified TLA+ spec and one with a conformance suite and around 40 recorded failure modes. Every cost claim here is unmeasured until Stage 7.

## 10. Open questions

- Offset as an integer or an opaque token. Integers are better ergonomics; tokens leave room to change the addressing later. Note that server-assigned dense offsets sidestep a problem an external store has: a reader parked at the tail can always name its position, because the next offset is simply `HeadOffset`. Redis IDs cannot be named before they are written, which is why Max's design needs a cursor to be a position boundary rather than a record identity.
- Whether per-item metadata (workflow, run, original run, attempt) is populated by the server, and whether it is on by default. The rewind model depends on it existing; whether we fill it in is separable.
- Pricing. Data transferred, storage, and active minutes are the candidates. This design deliberately makes cost track bytes rather than item count, which is the shape the 1-pager asks for.
- Naming. "Stream" collides with Kafka Streams. Using `stream` for now.
- Whether the collection primitive that Metablock needs should be built on this, or the other way round. This design does not need it, which is a scheduling argument, not an architectural one.
