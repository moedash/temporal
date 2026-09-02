# Replicating a stream log

Status: designed, not built. The substrate change made this much cheaper than it
was, and in a way worth writing down before anyone starts.

## What replicates today, and what does not

The stream component's state replicates. `sync_state_retriever.go` ships
`UpdatedChasmNodes` from the execution's CHASM tree in a
`SYNC_VERSIONED_TRANSITION_TASK`, and a stream's frontier, base offset,
consumers and producer cursors are all component state. They ride along for
free.

The log does not. Its rows live in `stream_log`, outside the CHASM tree, and
nothing ships them. That is the whole of the gap: after a failover the standby
holds a frontier and no bytes, so every read fails and every subscribed
workflow wedges.

This was previously worse than a gap. On the old substrate there was no hook to
build either, because replication reads history through a workflow's version
histories and a stream's buckets appeared in none. There was nowhere to put the
rows even if you wanted to.

## Why the substrate change makes it cheap

Three properties, all of them consequences of keying a row by the offset its
batch starts at.

**Applying a row is idempotent.** The key is `(shard, namespace, collection,
bucket, start_offset)`, so writing the same row twice is a no-op and writing a
retried version of it replaces the first. A replication stream may therefore
duplicate, retry, and redeliver freely. None of the exactly-once machinery
history events need applies here.

**Rows are independent.** There is no chain, no previous-transaction pointer,
nothing that orders one row against another. Out-of-order arrival is harmless.

**A row is self-describing.** It carries the offsets it covers, so a receiver
needs no context to place it.

Together those mean the payload can be shipped rather than referenced. History
events are fetched back by the standby through branch tokens, which is most of
the complexity in that path, and it exists because events are large and shared
across branches. Stream batches are neither.

## The design

Carry the rows in the message that already carries the frontier.

`SyncVersionedTransition` is built from everything that changed since the
receiver's last known version. A stream's appends are part of what changed. Add
the rows for the offset range between the last replicated offset and the
current frontier to that message, alongside `UpdatedChasmNodes`.

The apply side writes the rows first, then applies the state. Both are
idempotent, so a failure between them replays harmlessly.

**Ordering comes free, and it is the thing most likely to be got wrong.** If the
rows travelled separately from the frontier, the frontier could arrive first and
the standby would hold a frontier covering offsets whose bytes have not landed:
exactly the hole this whole design exists to avoid, revealed at failover, which
is the worst possible moment. Putting them in one message removes the question
rather than answering it. Any design that separates them needs a per-collection
replicated-through watermark and reads clamped to `min(frontier, watermark)`,
which is more machinery to get a worse result.

## What has to be decided before building

**Message size.** A stream can append a great deal between two syncs, and
`sync_state_retriever` has no byte cap today. Attaching log rows changes the
size profile of the most important message in the replication path. It needs a
cap and a continuation, and whether that belongs here or in the sync path
generally is not my call to make alone.

**Conflict on a stream written from both sides.** Unresolved, and not made
better by any of the above. Two clusters appending to one stream will assign the
same offsets to different bytes, and idempotent-by-offset then means last writer
wins, silently. The honest options are to make a stream single-writer by
ownership, the way an execution already is, or to accept the loss and say so.
Ownership is the right answer and it is a design question of its own.

**Whether streams replicate at all in the first release.** A stream that does
not replicate is not a correctness bug if it is documented; it is a smaller
feature. Given the option is still competing against one that keeps the payload
outside Temporal entirely, shipping without replication and saying so may be the
right first step.

## Estimate

The mechanism above is perhaps a week: proto, retrieval, apply, and the xdc
tests to prove a failover keeps the bytes. The size cap and the single-writer
question are each larger than the mechanism, and both are decisions rather than
code.

I have not started it. Touching the sync-state path is touching the most
safety-critical shared code in the server, and doing that on the strength of my
own design note, in a prototype whose substrate was itself decided this week,
would be the wrong order. The design is here to be argued with first.
