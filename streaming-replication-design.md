# Replicating a stream log

Status: designed, not built. The substrate change made this much cheaper than it
was, and in a way worth writing down before anyone starts.

## What replicates today, and what does not

The stream component's state replicates. `sync_state_retriever.go` ships
`UpdatedChasmNodes` inside `SyncWorkflowStateMutationAttributes`, which carries
an `exclusive_start_versioned_transition`: everything that changed since the
receiver's last known transition. A stream's frontier, base offset, consumers
and producer cursors are all component state, so they ride along for free.

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

## The mechanism

Carry the rows in the message that already carries the frontier. The apply side
writes the rows first, then applies the state. Both are idempotent, so a failure
between them replays harmlessly.

**Ordering comes free, and it is the thing most likely to be got wrong.** If the
rows travelled separately from the frontier, the frontier could arrive first and
the standby would hold a frontier covering offsets whose bytes have not landed:
exactly the hole this whole design exists to avoid, revealed at failover, which
is the worst possible moment. Putting them in one message removes the question
rather than answering it.

## Knowing which rows to send

The node snapshot is versioned, so the active knows the state delta since a
given transition. The rows are not versioned, so it does not know the offset
delta. This is the one part of the mechanism with a real choice in it.

**Option 1: the receiver reports a watermark.** The standby says, per
collection, the offset it holds rows through. The active ships from there to the
current frontier. No stored state anywhere, and nothing to keep in step.

The cost is a protocol addition. Receiver progress travels today as a per-shard
task-id acknowledgement, not as per-entity state, so this needs somewhere to put
a per-collection offset on the way back.

**Option 2: version the ranges.** Each append writes a small child node keyed by
its start offset, holding the range it covered. Those nodes are versioned like
any other, so "nodes updated since transition X" yields exactly the ranges
appended since X, and the active reads those rows from the table by them. No
protocol change, and it rides machinery that already exists.

The cost is a node per batch until truncation prunes it. For a token stream at
one batch per token that is a great many nodes in component state, which is the
thing this design has been trying to keep small everywhere else.

**Prefer option 1, because it makes the size cap free.** A stream can append a
great deal between two syncs, so a byte cap is needed either way. With a
watermark, a message that cannot carry the whole range carries a prefix, and the
receiver's next watermark resumes exactly where it stopped. There is no
continuation token and no resumption state: the cap and the resume are the same
mechanism. Option 2 needs continuation built separately, because a set of
versioned ranges gives no natural place to stop halfway.

## What has to be decided before building

**Where the size cap lives.** `sync_state_retriever` has no byte cap today.
Attaching log rows changes the size profile of the most important message in the
replication path. Option 1 makes the cap resumable but does not decide whether
capping belongs in the stream-specific code or in the sync path generally, and
that is not my call to make alone.

**Conflict on a stream written from both sides.** Unresolved, and not made
better by any of the above. Two clusters appending to one stream will assign the
same offsets to different bytes, and idempotent-by-offset then means last writer
wins, silently. The honest options are to make a stream single-writer by
ownership, the way an execution already is, with appends elsewhere rejected or
forwarded, or to accept the loss and say so. Ownership is the right answer and
it is a design question of its own.

**Whether streams replicate at all in the first release.** A stream that does
not replicate is not a correctness bug if it is documented; it is a smaller
feature. Given the option is still competing against one that keeps the payload
outside Temporal entirely, shipping without replication and saying so may be the
right first step.

## Estimate

The mechanism above is perhaps a week: proto, retrieval, apply, and the xdc
tests to prove a failover keeps the bytes. The watermark's back-channel is the
largest single piece of it. The size cap and the single-writer question are each
larger than the mechanism, and both are decisions rather than code.

I have not started it. Touching the sync-state path is touching the most
safety-critical shared code in the server, and doing that on the strength of my
own design note, in a prototype whose substrate was itself decided this week,
would be the wrong order. The design is here to be argued with first.
