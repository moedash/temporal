# Native Streams: Design Comparison

| | |
|---|---|
| Status | Draft for review |
| Ticket | [AI-198](https://temporalio.atlassian.net/browse/AI-198) |
| Author | Moe Dashti |
| Date | 2026-08-23 |
| Compares | `streaming-high-level-design.md` + `streaming-detailed-design.md` against two existing prototypes |

Our design was written clean-room, before reading either prototype. This document compares the three, then records what we change as a result. Sections 7 and 8 are the actionable part.

---

## 1. The three designs

**Ours (server-side log).** The stream is a CHASM component holding only a frontier; payload bytes live in the stream's own `history_node` branch. Appends do not schedule workflow tasks. Readers own their cursor. In-workflow consumption records the consumed offset range in history and attaches the bytes out of band.

**Max's (external store, client-side).** `mfateev/sdk-python` branch `task/python-sdk-streaming`, roughly 9,500 lines under `temporalio/contrib/external_workflow_streams/`, plus about 40 ADRs in `mfateev/sdk-core` `arch_docs/streaming-poc-docs/`. Payloads live in a pluggable external backend (Redis Streams is the worked example). No Temporal server changes. Replay is preserved with compact marker events recording consumed offset ranges and observation boundaries. A reserved Signal `__temporal_external_stream_wake` provides the wakeup.

**Johann's (server-side facet).** `temporalio/internal-ai-prototypes` branch `2026/05/native-streams`, with server code at `origin/native-streams-prototype`, roughly 9,890 lines across 67 files. A CHASM `Stream` component holding control state, plus a new `stream_segments` persistence facet with its own table on four backends, an exactly-once cross-facet commit protocol, and a TLC-verified TLA+ spec. Python client. No workflow integration yet.

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

## 4. Max's approach

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

### What we take

1. **Record the range even when it is empty**, and carry the resolved start offset on first observation (ADR-005). Our Path C as written only wrote an event when items were delivered, which leaves replay unable to reproduce a task where the subscription observed nothing. That is a correctness hole, and this is the fix.
2. **A per-producer write fence distinct from closing the stream** (ADR-040). A producer can declare it is done without ending the stream for other producers.
3. **Bound delivery by record count as well as bytes** (ADR-026 with ADR-007).
4. **Attached-stream identity keyed on the first execution run ID**, so it is stable across a continue-as-new chain and does not collide after workflow ID reuse.
5. **Missing data on replay blocks rather than fails** (ADR-014): surface a retryable workflow task failure, not a nondeterminism error.

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
4. **In-workflow consumption at no per-item history cost**, and exactly-once without user-side dedup.
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
