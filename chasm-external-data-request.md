# CHASM needs a node kind for data a component owns but does not store inline

Audience: CHASM owners. Written from the streaming prototype (AI-198), which
hit this, but the gap is not specific to streams.

## The ask

A CHASM component can own bulk data that is too large to live in mutable
state. Today it has no way to say so, so the framework does not know the data
exists: it does not replicate it, does not reclaim it, and does not count it.
Every component with this shape has to hand-roll all three.

Requested: a node kind that holds a locator for externally stored data plus
enough metadata for the framework to replicate and reclaim it. The concrete
proposal for the replication half is in "A protocol that works" below.

## What exists today

Four node kinds, in `chasm.proto:27-32`: component, data, collection, pointer.
All four store their bytes inline, in `WorkflowMutableState.chasm_nodes`
(`workflow_mutable_state.proto:19`). That is the correct design for state. It is
not a place to put payload:

- `chasmNodeSizes` (`mutable_state_impl.go:170`) feeds `approximateSize`
- checked against `MutableStateSizeLimitError`, 8 MB, warn at 1 MB
  (`dynamicconfig/constants.go:473-481`)
- over the error limit the execution is **force-terminated**
  (`context.go:1381`, `maxMutableStateSizeExceeded`)
- the check is archetype-tagged, so a standalone CHASM entity is subject to it
  exactly as a workflow is

For scale: a measured 100k-token stream is 5.84 MB of payload. Inline, that is
one long agent conversation before the entity is killed, and every append
rewrites the whole record on the way there.

So the bytes go in a side store. That part is not controversial and it is what
history already does: the branch token lives in mutable state, the bytes live
in `history_node`. The gap is that history's arrangement is bespoke. There is
no general way to express it, so the next component to need it starts over.

## Working assumption: the store is external and shared

For the streaming prototype the current implementation is a `stream_log` table
in the execution database, but **the intended target is an external shared
store**, not Temporal's own database. That is the right assumption for this
request, and it sharpens it: the locator points outside the database entirely,
so there is no chance of the framework quietly reaching the bytes through an
existing persistence path. It has to be told.

It also surfaces the one thing the current prototype has in the wrong place:
`AppendStreamLog`, `ReadStreamLog` and `DeleteStreamLogBucket` are methods on
`ExecutionStore` (`persistence_interface.go:168-175`), implemented in
`sql/history_store.go` and `cassandra/history_store.go`. That presumes the log
lives in the execution database. Under an external store it needs to be its own
store type, resolved per namespace or per cluster rather than per shard.

Two consequences of an external store that this design has to answer, and they
are worth being explicit about because they are not improvements:

**There is no transaction across the two systems.** The bytes and the frontier
commit separately. Ordering is therefore load-bearing: write bytes, then commit
the frontier. Crashing in between leaves bytes nobody references, which is
reclaimable garbage. The reverse order leaves a frontier whose bytes never
landed, which is unrecoverable data loss discovered by a reader. Idempotent
writes keyed by offset make the safe order safe to retry.

**Durability has to be real.** Redis has been named as a candidate. Its default
configuration is not durable, and a stream that a customer is told is durable
cannot be backed by a cache. Whatever the store is, it needs durable
acknowledgement before the frontier advances, or the ordering rule above buys
nothing.

## What the framework would have to do

**Replicate.** Component state already replicates: `sync_state_retriever.go:415`
ships `UpdatedChasmNodes` inside `SyncWorkflowStateMutationAttributes`, scoped
by `exclusive_start_versioned_transition`. External bytes do not, so today a
standby holds a frontier and no data, discovered at failover.

Whether the framework has to ship the bytes at all depends on the store, and
this is the one question an external store genuinely improves. If the store is
itself multi-region, replication of the payload is the store's problem and
Temporal ships only the reference, which it already does. If the store is
regional, Temporal ships the payload, and now two systems have to fail over
consistently. The framework should therefore treat "who replicates the bytes"
as a property of the store rather than assuming either answer.

**Reclaim.** When the owning component completes or truncates, something has to
delete the bytes. Inline data gets this free. External data needs the framework
to run a reclamation hook, and to tolerate the store having already lost them.

**Account.** External bytes are invisible to `approximateSize`, which is correct
for the force-terminate check and wrong for quota. A namespace can currently
write unbounded external payload with no accounting anywhere.

## A protocol that works

For the regional-store case, where Temporal does ship the payload. Offered as a
concrete proposal rather than the only option.

Three properties make shipping the payload viable rather than fetching it back
the way history events are fetched through branch tokens. All three follow from
keying a record by the offset it starts at:

- **Applying is idempotent.** Same key, same record. Duplicate and retry freely.
- **Records are independent.** No chain, no previous-record pointer, so
  out-of-order arrival is harmless.
- **A record is self-describing.** It carries the range it covers, so a receiver
  needs no context to place it.

Carry the records in the message that already carries the frontier, and apply
records before state. Both are idempotent, so a failure in between replays
harmlessly. Ordering is then free, which matters because it is the thing most
likely to be got wrong: shipped separately, the frontier can arrive first and
the standby holds offsets whose bytes never landed.

**Which records to ship** is the part with a real choice in it. The state delta
is versioned and the records are not, so the sender cannot tell from what
already exists which records go with it.

*Receiver reports a watermark.* The standby says, per collection, the offset it
holds records through, and the sender ships from there to the frontier. No
stored state anywhere. Needs somewhere to put a per-entity offset on the way
back, since receiver progress travels today as a per-shard task-id
acknowledgement.

*Version the ranges.* Each append writes a small child node keyed by its start
offset holding the range it covered. Those nodes are versioned like any other,
so "nodes updated since transition X" yields exactly the ranges appended since
X. No protocol change, and it rides machinery that already exists. Costs a node
per append in mutable state, which is the thing this whole design is trying to
avoid, and it needs continuation built separately.

**Prefer the watermark, because it makes the size cap free.** A cap is needed
either way: a stream can append a great deal between two syncs, and
`sync_state_retriever` has no byte cap today. With a watermark, a message that
cannot carry the whole range carries a prefix, and the receiver's next watermark
resumes exactly there. The cap and the resume are the same mechanism, with no
continuation token and no resumption state.

## Questions for CHASM owners

1. Is an external-data node kind something you want in the model, or is the
   position that components needing this should keep doing it privately the way
   history does?
2. If it is wanted, does the framework ship the bytes, or is that delegated to
   the store based on a declared property of it?
3. Where should a byte cap on `sync_state_retriever` live: in the
   external-data handling, or in the sync path generally?
4. Does external data need to count against a namespace quota, and is there an
   existing place for that?

## Still unresolved, and not a framework question

A stream written from both sides of a failover. Two clusters appending assign
the same offsets to different bytes, and idempotent-by-offset then means last
writer wins, silently. The right answer is single-writer ownership, the way an
execution already has an owning cluster, with appends elsewhere rejected or
forwarded. That is a design question for the stream component, not for CHASM.

## Status

Designed, not built, and deliberately so. The framework question above has
lead time, and building the private version first would make it harder to ask.
Touching the sync-state path on the strength of my own design note, in a
prototype whose substrate was decided this week, would be the wrong order.
