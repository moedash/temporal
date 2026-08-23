# Native Streams: Design Comparison

| | |
|---|---|
| Status | Draft for review |
| Ticket | [AI-198](https://temporalio.atlassian.net/browse/AI-198) |
| Author | Moe Dashti |
| Date | 2026-08-23 |
| Canonical option list | [Native Streaming: Options Discussed](https://app.notion.com/p/3b28fc567738812f8c67ca3ebdf9ce38) in Notion |
| Compares | `streaming-high-level-design.md` + `streaming-detailed-design.md` against the implemented prototypes |

**The Notion page is the single source of truth for the option comparison.** This document is narrower and does not duplicate it: it compares our design against the two prototypes that exist as running code, and against Option 7 specifically, since Option 7 targets the same problem as our Path C.

Our design was written clean-room, before reading either prototype. Sections 8 and 9 are the actionable part.

Where our design sits in the canonical list: it is closest to **Option 5** (CHASM-backed append-only collection) but differs in two ways that matter. It does not depend on the CHASM collection primitive, because payload bytes never enter the CHASM tree, and it carries a bucket-2 mechanism that differs from Option 7's. It is arguably a separate option and could be added to the Notion page as one.

---

## 1. What is being compared

**Ours (server-side log).** The stream is a CHASM component holding only a frontier; payload bytes live in the stream's own `history_node` branch. Appends do not schedule workflow tasks. Readers own their cursor. In-workflow consumption records the consumed offset range and attaches the bytes out of band.

**Option 7 / Max's prototype (external store, long-lived workflow task).** These are the same thing, which is worth stating plainly because the canonical page lists Option 7 with status "prototype intended". **It already exists**: `mfateev/sdk-python` branch `task/python-sdk-streaming`, roughly 9,500 lines under `temporalio/contrib/external_workflow_streams/`, plus about 40 ADRs and a conformance suite in `mfateev/sdk-core` `arch_docs/streaming-poc-docs/`. Its own overview describes exactly Option 7: "while a Workflow Task is open, the SDK runtime reads the external stream directly. No Temporal Server changes are required." The ADRs contain findings the one-paragraph summary does not carry; see §6.

**Johann's prototype (server-side facet).** `temporalio/internal-ai-prototypes` branch `2026/05/native-streams`, server code at `origin/native-streams-prototype`, roughly 9,890 lines across 67 files. A CHASM `Stream` component plus a new `stream_segments` persistence facet on four backends, a 3-step cross-facet commit protocol, and a TLC-verified TLA+ spec. Python client. No workflow integration. This is the implementation of Option 5, minus the shared collection primitive.

---

## 2. Side by side

| | Ours | Max's | Johann's |
|---|---|---|---|
| Where payload bytes live | existing `history_node` branch | external store (Redis) | new `stream_segments` table |
| Server changes | CHASM lib + one framework hook | none | CHASM lib + new persistence facet + schema on 4 backends |
| SDK core changes | none required for the client path | required (`ReplayExternalStreams` activation job, new commands) | none |
| New DB schema | none | none (external) | 2 tables x 4 backends |
| Durability owner | Temporal | the external store | Temporal |
| Replication | rides history-node replication | not covered | deferred, facet is replication-friendly |
| Exactly-once write | server-enforced, `(producerId, seq)` | backend adapter idempotency, explicitly a non-goal | server-enforced, `(publisher_id, sequence)` |
| Commit protocol | none beyond append-then-advance | n/a | 3-step cross-facet, TLA+ verified |
| LWTs per publish (Cassandra) | 1 | 0 (no Temporal write) | 2, or 2/G with group commit |
| Offset to storage lookup | node-ID range read, no index | provider offsets | `SegmentIndex` on the chasm node, binary search, close-time offload |
| Client tail | long-poll via `chasm.PollComponent` | direct backend read plus wake Signal | not built (`ReadRange` is non-blocking) |
| Subscriber limit | memory-bound | backend-bound | bounded outstanding window per subscription |
| Workflow publish | command inside the existing WFT commit | producer handle to the external store | designed, not built |
| Workflow consume | offset range in history, bytes out of band | marker annotation, bytes from the backend | items pushed **as signals into the consumer's history** |
| Consumer-side dedup burden | none | none (ranges are recorded) | **on the workflow handler**, at-least-once |
| Maturity | design only | working, heavily tested, ~40 ADRs | working primitive, TLA+ spec, no workflow integration |

---

## 3. Where all three agree

Worth stating, because the convergence is evidence the shape is right:

- Items must not go into workflow history one per event.
- Offsets are the addressing model, and the reader carries its own cursor.
- Publishing must not wake the owning workflow.
- The stream outlives the workflow task that produced it, and close is distinct from delete.
- Rewinds on retry or reset are surfaced to the application rather than hidden.
- Topics, not predicates, for filtering.

Most significantly, **Max and we independently arrived at the same replay model**: record the consumed offset range in history and re-read the bytes from an immutable log. He calls it a marker annotation, we call it a cursor event. That two designs built without knowledge of each other landed on the same mechanism is the strongest single signal in this comparison.

---

## 4. Option 7 / Max's prototype

### What it gets right

- **No server changes.** It could ship on today's server, which no other option can claim.
- **The replay model is correct and battle-tested.** Around 40 ADRs, each pairing a rule with the failure that occurs without it, and a conformance suite for backends.
- **It found the hard cases.** Cursor as a position boundary rather than a record identity (ADR-002), because a consumer parked at the tail has no next-record ID to persist. Progress as an observation delta emitted on every completion path (ADR-005), because a subscription that observed nothing still has to record that it observed nothing. Activation segmentation reproduced rather than collapsed (ADR-018), because `wait_condition` fires once per activation and collapsing k drains into one changes when conditions fire.
- **Cost claim is sharp**: history event cost scales with consumption batches and idle-to-active transitions, not item count, and marker bytes are capped by a byte budget that forces rollover instead of growing a marker.

### Where it does not fit our goal

- **Durability moves outside Temporal.** This is the objection the 7/23 options doc already recorded: if workflows replicate and the external stream does not, the guarantees become inconsistent. Structural immutability of the backend has to be asserted at registration time (ADR-003) rather than being true by construction, and the doc states plainly that a provider which silently violates it delivers altered bytes on replay with no error raised.
- **It needs an external system.** That is the Redis workaround the feature exists to remove, made official. The PRD lists extra infrastructure, split observability, and a second failure domain as the reasons the workaround is not sufficient.
- **Exactly-once producer execution is an explicit non-goal**, delegated to backend adapter idempotency.
- **The replay machinery is inherently complex** because the SDK reads the backend continuously while a workflow task is open. The boundary of what was observed is not a natural artifact of anything, so it has to be reconstructed: runs, segments, per-segment end reasons, sparse control positions, and a byte budget.
- Requires sdk-core changes, so "client side" means "no server changes", not "no protocol changes".

### Option 7 against our Path C

Both target bucket 2. This is now the live disagreement, so it is worth being precise rather than summarising.

| | Option 7 | Ours (Path C) |
|---|---|---|
| Where payload lives | external store (Redis, Postgres) | Temporal-owned log |
| Server changes | none, SDK-only | CHASM lib, transaction hook, api-go |
| Workflow task shape | held open while reading, completes on idle (~1-2s) or explicit flush | normal duration, one slice per task |
| Recorded for replay | "stream-empty" markers at task completion | delivered range on every `WorkflowTaskCompleted`, including empty |
| Failure mid-stream | application-level reset protocol, visible to users | nothing to do; the log is immutable and the cursor is recorded |
| Exactly-once | not provided; idempotent append via the backend adapter | server-enforced |
| Cost per LLM invocation | about 1 signal + 1 marker, data cost paid to Redis | 0 events, data cost paid to Temporal storage |
| Time to ship | fastest of any option | slowest |

The honest summary: **Option 7 is cheaper and faster to ship because the data never enters Temporal at all.** That is the same trade as Option 1 against Option 5, applied to bucket 2. It is not a trade we can argue our way out of, and pretending our design wins on cost would be wrong.

Where I think Option 7 needs scrutiny, phrased as questions rather than objections:

1. **Long-lived workflow tasks against the workflow-task timeout.** Holding a task open for the length of an LLM response means the task lives for seconds to minutes. Gap K3 in the 8/10 WTAL list is exactly "you still can't raise a WFT timeout; need to not kill a WFT that's still making progress", and it is marked *needs investigation*. Is Option 7 gated on K3? Separately, while a task is open, incoming signals buffer, and `MaximumBufferedEventsBatch` defaults to 100 with a 2MB cap (`common/dynamicconfig/constants.go:2610`), after which the task is force-failed. A workflow taking sustained signal traffic during a long read would hit that.
2. **Worker slot occupancy.** A held task pins a worker slot for the LLM's duration, which changes worker sizing in a way per-task execution does not. Worth measuring, not assuming.
3. **Is the replay analysis complete?** The summary says the only non-determinism risk is empty-on-original, non-empty-on-replay. Max's own prototype found more: ADR-018 concludes that replay must reproduce **activation segmentation**, the number of drains, not just the record order, because `wait_condition` predicates evaluate once per activation and collapsing k drains into one changes when conditions fire. ADR-005 adds that the first observation of a subscription must carry provider identity and resolved start boundary even when nothing was delivered. Those are real and already solved in the branch, but they are not in the summary, so the summary understates the replay work.
4. **Durability of the wake-up.** The notes say the signal must be durable, then float a rejected update as a lighter-weight substitute. A rejected update leaves no history event. If the workflow is not running when the stream opens, what guarantees the wake is not lost?

And where Path C is genuinely simpler, for a structural reason rather than a cleverness one: our delivery boundary **is** the workflow task boundary, and that boundary is already durable. So there is no segmentation to reproduce, no idle heuristic to tune, and no reset protocol, because a failed task leaves an immutable log and a cursor that was never advanced. Option 7 has to reconstruct all three because it reads continuously inside a task, which is also precisely what buys it its lower latency and lower cost.

### They are not mutually exclusive

Worth putting on the table before the comparison hardens into a choice.

Option 7 is a **consumption mechanism** over a pluggable store whose contract is append plus read-from-offset. Our design is a **store** with that same contract, plus a consumption mechanism. If the SDK-side reading model is written provider-agnostic, then a Temporal-native stream is simply one more provider behind it.

That suggests a sequence rather than a fork: ship Option 7's SDK model with Redis and Postgres bindings first, because it is fastest and the urgency is real, and add a Temporal-backed provider for customers who need durability and replication inside Temporal. Customers who are happy running Redis get unblocked now; customers who cannot run a second stateful system get an answer later without a second API.

The cost of not doing this is two stream APIs.

### What we take

1. **Record the range even when it is empty**, and carry the resolved start offset on first observation (ADR-005). Our Path C as written only wrote an event when items were delivered, which leaves replay unable to reproduce a task where the subscription observed nothing. That is a correctness hole, and this is the fix.
2. **A per-producer write fence distinct from closing the stream** (ADR-040). A producer can declare it is done without ending the stream for other producers.
3. **Bound delivery by record count as well as bytes** (ADR-026 with ADR-007).
4. **Attached-stream identity keyed on the first execution run ID**, so it is stable across a continue-as-new chain and does not collide after workflow ID reuse.
5. **Missing data on replay blocks rather than fails** (ADR-014): surface a retryable workflow task failure, not a nondeterminism error.
6. **An explicit flush control message.** Option 7 ends a read when the stream is idle for a second or two, or when the writer sends an explicit flush. Our design bounds a slice by size and count but gives the producer no way to say "the turn is finished, deliver now". Without it a consumer waits on a timeout it should not need. Cheap to add and it removes a tuning knob rather than adding one.
7. **A per-topic sequence number alongside the global offset.** Option 7 multiplexes logical streams over one physical stream with per-stream IDs so one tool call can reset without disturbing the others. Our offsets are global across topics, which is right for ordering but leaves a consumer no cheap way to reason about one topic's progress.

### What we do not take

The external backend itself. It is a legitimate product option for customers who want to pay a different price for volume, and the 1-pager says so. It is not this design. Our read API is offset plus long-poll, which is the same shape a backend adapter would expose, so the two can sit behind one client API later if we choose.

---

## 5. Johann's approach

### What it gets right

- **The durability story is ours too**, and it is the right one: every item a subscriber sees is already persisted by the history server.
- **It is the most rigorous artifact of the three on the write path.** A TLA+ spec that TLC exhausts in about 15 seconds, with every action mapped to a Go function in `spec/SPEC_TO_CODE.md`.
- **It settled a lot of product surface** we would otherwise re-litigate: multi-topic streams with subscribe-time filtering, `ListStreams` for operators, owner link on business ID so it survives continue-as-new, close-not-delete on owner completion, no invented per-stream TTL, and end-to-end codec so the server never sees plaintext.
- **Positioning is explicit and honest**: many modest-volume streams, not a firehose, with per-stream throughput bounded by the CHASM transition rate on a single execution.
- **Inline retention truncation** at the end of each successful publish transition, with a consumer pin, rather than a separate sweeper.

### Where we differ, and why

**The persistence choice is the crux.** Johann's `persistence-abi.html` evaluates four approaches and picks C, a dedicated append-log facet. Its own stated rationale is that this "mirrors workflow history events, which the team already operates at scale". Our design takes that observation one step further: rather than building a facet that mirrors `history_node`, use `history_node`.

The doc anticipates this and rejects it in one line: history's pattern is "inspiration but not a drop-in template" because "history's 12-hour scavenger and 60-day min-age work because history orphans are rare; streams generate them routinely (publish retries, truncate races, backpressure) and need a prepare / write / commit protocol with explicit visibility frontier and a 1-5 minute sweeper."

That objection is about garbage, not correctness, and it has an answer that was not considered:

- **Correctness is already handled by the store.** `filterHistoryNodes` (`common/persistence/history_manager.go:1039-1073`) does not merely prefer the higher transaction ID for the same node ID. It requires transaction IDs to be non-decreasing along the node chain and drops any node whose transaction ID went backwards. Its own comment says it: "event batches with larger node ID -> batch with lower transaction ID is invalid (happens before)". So an orphan beyond a retry's extent is dropped on read, not just an orphan at the same offset. Combined with clipping reads to `HeadOffset`, no reader can observe an orphan.
- **Garbage collection already has a purpose-built tool.** `TrimHistoryBranch(BranchToken, NodeID, TransactionID)` takes a known-valid frontier and removes everything off the valid chain. The Stream component holds exactly that frontier. So the sweep is a cheap pure task fired after a failed append, not a 12-hour scavenger. The premise that we would inherit history's coarse cleanup cadence does not hold.

With those two, the prepare/write/commit protocol is not needed, and the design collapses to append-then-advance, which is the pair Temporal has run in production for a decade.

The consequences are concrete:

| | Johann's C | Ours |
|---|---|---|
| New tables | `stream_segments` + `stream_sealed_indexes`, 4 backends | none |
| Schema migrations | Cassandra 1.14, MySQL/PG 1.20, SQLite 0.12 | none |
| Commit protocol | 3 steps, TLA+ needed to trust it | append-then-advance |
| Cassandra LWTs per publish | 2 | 1 |
| Offset lookup structure | `SegmentIndex` on the chasm node, binary search, plus a close-time offload to a sibling table when it grows | none; the branch token plus a node-ID range read |
| Tentative-row sweeper | 1 to 5 minutes, required | pure task on failure, opportunistic |
| Replication | new facet needs its own story | rides history-node replication |

Halving the LWT count matters more than it looks. Johann's own `spec/perf-back-of-envelope.md` identifies the per-partition LWT rate (budgeted at 100/sec) as the per-stream ceiling, and concludes that group commit is "load-bearing, not optional" to hit the 300 items/sec target. Starting at one LWT per publish rather than two doubles the base before group commit is applied, and group commit applies to our design equally.

**The second difference is in-workflow consumption.** Johann's decision D9 pushes matching items to the workflow **as signals or updates on the consumer's own history**, at-least-once, with handlers deduplicating on item offset. That keeps items out of the *publisher's* history but puts them into the *consumer's*, which is the same cost in a different place, and it puts the dedup burden on user code. The Bellevue session later set the opposite requirement: "the end user never handles delivery deduplication". D9 predates that session, so this is a case of the session superseding an earlier decision rather than a disagreement.

Our Path C records only the offset range and attaches the bytes to the workflow task response out of band, so nothing per-item enters the consumer's history and delivery is exactly-once by construction. It also needs no subscription state machine, no three-phase delivery protocol, and no outstanding-bytes arm step, because delivery is pulled at task-build time rather than pushed.

There is a further advantage that only becomes visible next to Max's design. Because our delivery boundary is the workflow task boundary, and that boundary is already in history, we do not have the segmentation problem ADR-018 solves. One slice arrives per task, the SDK drains it once, and replay drains the identical slice once. No runs, no segments, no per-segment end reasons.

### What we take

1. **Group commit.** Coalesce concurrent appends to one stream into a single CHASM transition. This is the difference between meeting and missing the throughput target on Cassandra.
2. **Explicit positioning and a stated per-stream ceiling**, rather than letting readers assume a firehose.
3. **Inline retention truncation** at the end of a publish transition, with a consumer pin, instead of a sweeper.
4. **Multi-topic streams with subscribe-time filtering**, and `ListStreams` for operators.
5. **Owner link on business ID, close-not-delete on owner completion**, and no invented per-stream TTL beyond the lifecycle policy the Bellevue session asked for.
6. **Say out loud that the server never sees plaintext.** Our blobs are opaque already, so the codec property is free; it just was not written down.

---

## 6. What our design has that neither does

Stated plainly so it can be attacked:

1. **No new storage.** Reusing `history_node` removes two tables, four schema migrations, an index structure with its own offload path, and a sweeper.
2. **No commit protocol.** The clip invariant plus the transaction-ID chain gives exactly-once without a prepare phase, which is why no TLA+ spec is required to trust it.
3. **No dependency on CHASM partial reads.** `OSS-4917` and `OSS-4918` are both still `To Do`. Johann's revised plan (analysis §5.1) puts streams on a shared CHASM collection primitive owned by another team, which is the current critical path. Payload bytes never enter the CHASM tree in our design, so that dependency disappears. This is a scheduling argument, not an architectural one, but D1 is due at the September 14 check-in.
4. **In-workflow consumption at no per-item history cost**, and exactly-once without user-side dedup. Option 7 also targets bucket 2, so this is no longer unique; what is different is that ours needs no reset protocol and no idle heuristic, at the price of server work and Temporal-side storage cost.
5. **Replication is inherited** rather than designed, because history-node data already replicates.

And the honest counterweight: **ours is a design, theirs are working code.** Johann's has a verified spec and a passing end-to-end test. Max's has a conformance suite and around 40 ADRs recording failure modes we have not hit yet. The claims in the table above are unmeasured. The benchmark is what settles it.

---

## 7. Changes we are making

Applied to both design documents.

| # | Change | Source |
|---|---|---|
| 1 | Record the consumed range on `WorkflowTaskCompleted` rather than as a separate event, and record it on **every** task for a subscribed stream, including empty ranges. First record for a subscription carries the resolved start offset. | Max ADR-005 (correctness fix) |
| 2 | Strengthen the clip-invariant argument to cite the transaction-ID chain rule, not just same-node-ID resolution. | Johann's orphan objection |
| 3 | Eager orphan trim via `TrimHistoryBranch` as a pure task after a failed append. | Johann's orphan objection |
| 4 | Group commit: coalesce concurrent appends into one CHASM transition. | Johann |
| 5 | Per-producer write fence, distinct from close. | Max ADR-040 |
| 6 | Attached-stream identity keyed on first execution run ID. | Max |
| 7 | Bound Path C delivery by record count as well as bytes. | Max ADR-026, ADR-007 |
| 8 | Missing data on replay blocks with a retryable task failure rather than a nondeterminism error. | Max ADR-014 |
| 9 | Inline retention truncation at end of publish, with consumer pin. No sweeper. | Johann D7 |
| 10 | Multi-topic with subscribe-time filtering; add `ListStreams`. | Johann D5, operators |
| 11 | State the per-stream throughput ceiling and the positioning explicitly. | Johann §1a |
| 12 | State the codec property: the server never sees plaintext. | Johann §3a |
| 13 | Explicit flush control message from the producer, so a consumer does not wait out an idle timeout. | Option 7 |
| 14 | Per-topic sequence number carried alongside the global offset. | Option 7 |

## 8. What we are deliberately not taking

- **A pluggable external backend.** Complementary product option, not this design. Our read API shape would let it sit behind the same client API later.
- **A dedicated `stream_segments` facet.** Section 5 is the argument.
- **The three-step commit protocol and its TLA+ spec.** Not needed once the storage choice removes the cross-facet problem. If review disagrees with section 5, this comes back with it.
- **Push-based in-workflow delivery via signals.** Superseded by the Bellevue no-user-dedup requirement.
- **Marker annotation grammar with runs and segments.** Not needed when the delivery boundary is the workflow task boundary.

## 9. What would change our mind

- The persistence argument in §5 has now been checked with running tests (`common/persistence/tests/history_store_stream_log.go`, passing on SQLite and Postgres): blobs round-trip opaquely on a non-run branch, and the transaction-ID chain drops a stale node from a shrinking retry on both the parsing and raw read paths. A negative control confirms the test is not passing vacuously. So the specific objection quoted in §5 does not hold on OSS storage.
- What that does **not** cover is the SaaS layer. `saas-temporal/walker/` overrides `HistoryBranchUtil`, so minting branches whose tree ID is not a run ID still needs a read of that code. If it turns out to be blocked there, the fallback is Johann's dedicated facet, and most of the rest of our design carries over unchanged.
- If orphan volume under real retry rates is worse than the eager trim can keep up with, the sweeper comes back.
- If measured LWT cost per publish does not come out at 1, the whole persistence argument needs rechecking.
- **The strategic question is not ours to settle.** Option 7 moves stream durability outside Temporal. The Native Streams 1-pager currently states the opposite as a principle: "Durable streams. We are not going to sacrifice durability to improve latency or cost", plus an exactly-once write guarantee. Option 7 provides neither, deliberately. That is a legitimate product choice and the CTO can change the principle, but it should be changed explicitly rather than decided by which prototype lands first. If the answer is that Temporal does not need to own the stream, our design is the wrong bet and the sequencing in §4 is the right one.
