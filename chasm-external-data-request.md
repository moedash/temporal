# Where a stream's bytes live: asked, and answered

Status: **answered 2026-09-02.** This started as a request to CHASM owners for a
node kind covering data a component owns but does not store inline. The answer
is that it is already planned, so the request is withdrawn and this file is the
record of the answer and what it means for the prototype.

## The answer

Yichao Yang: the underlying event storage in CHASM is planned to be exposed in
**H2, with replication and lifecycle**. Roey Berman confirmed that streaming was
expected to use it. So the general capability is coming, and nothing in this
prototype should be built to substitute for it.

That closes the question this file originally asked. Not by argument, but
because the work is scheduled and owned elsewhere.

## Why the question came up

A stream's messages cannot live in mutable state. All four CHASM node kinds
store their bytes inline (`chasm.proto:27-32`) in
`WorkflowMutableState.chasm_nodes` (`workflow_mutable_state.proto:19`), and:

- `chasmNodeSizes` (`mutable_state_impl.go:170`) feeds `approximateSize`
- checked against `MutableStateSizeLimitError`, 8 MB, warn at 1 MB
  (`dynamicconfig/constants.go:473-481`)
- over the error limit the execution is **force-terminated**
  (`context.go:1381`, `maxMutableStateSizeExceeded`)
- archetype-tagged, so a standalone stream entity is subject to it exactly as a
  workflow is

Measured: a 100k-token stream is 5.84 MB of payload. Inline, that is one long
agent conversation past the warn and approaching the limit, and every append
rewrites the whole record on the way there.

## Prior art, which reached the same ceiling

Dan Davison and Sean Kane built a CHASM streaming component about a year ago,
on `seandan/streaming` in `temporalio/temporal`. It is roughly 1500 lines under
the same `chasm/lib/stream/` path this prototype uses, with `AddToStream` and
`PollStream` on the frontend and one end-to-end test. Its component is:

```go
type Stream struct {
    chasm.UnimplementedComponent
    *streampb.StreamState                          // head, tail
    Messages chasm.Map[int64, *commonpb.Payload]   // one data node per message
}
```

Roey Berman flagged its ceiling at the time: limited to about 5 MB of total
payload because everything is in mutable state. The same note enumerated three
ways out, and they are the same three this prototype arrived at independently:
CHASM nodes in separate cells, payloads written outside mutable state by
repurposing history, or payloads replaced by pointers to a blob store.

Worth being plain about it: this prototype's first substrate was the second of
those, and the request this file used to make was the third. Neither was a new
idea. Finding that out after the fact is the cost of not having read
`#crew-streaming` first.

## The one thing that is not settled

Dan Davison's guidance is to not make the storage realistic: use something based
on `chasm.Map` and assume it has the properties it needs, because the value of
this prototype is exploring user-facing behaviour that other design sessions
might miss. That is a coherent position and it is what the prior art does.

It does not carry the benchmark, which is the other half of AI-198. The point of
measuring client-side streaming against server-side streaming is to find out
what each costs, and a substrate that force-terminates the entity partway
through the workload cannot produce a number anyone should quote. The two goals
want different things from the same prototype, and that is the disagreement to
resolve rather than paper over.

Separately, Paul Nordstrom has asked for a discussion before this goes further,
on the grounds that the data team owns backend storage for the stream affordance
and this is not a place for a one-off, and that the short-term path agreed with
Max was a client-side Redis connection. That is an ownership question, not a
technical one, and it is not mine to settle here.

## What the prototype assumes in the meantime

An external shared store, with `stream_log` as the working implementation. Not
because it should ship, but because the benchmark needs a substrate that does
not fall over inside the workload. When CHASM event storage lands, this is the
piece that gets deleted.

The consequence already recorded: the three persistence methods sit on
`ExecutionStore` (`persistence_interface.go:168-175`), which presumes the log is
shard-local. Under any external store that is the wrong home. Given the answer
above, moving it is probably wasted work, so it stays as it is.

## What replication does, until then

Nothing. A standby holds a frontier and no bytes, so a failover breaks every
reader. That is now a documented limitation of a prototype rather than a gap to
close, because the replication of stream bytes arrives with CHASM event storage
in H2.

The protocol sketch that used to be the point of this file is kept below,
because it is cheap to keep and it is a concrete answer to one question that
CHASM event storage will have to answer too: how a receiver tells a sender which
byte ranges it already holds.

Three properties, all from keying a record by the offset it starts at. Applying
a record is idempotent, so duplicates and retries are free. Records are
independent, so out-of-order arrival is harmless. A record is self-describing,
so a receiver needs no context to place it. Together they make shipping the
payload viable rather than fetching it back the way history events are.

Carry the records in the message that already carries the frontier, and apply
records before state. Ordering is then free, which matters because it is the
thing most likely to be got wrong: shipped separately, the frontier can arrive
first and the standby holds offsets whose bytes never landed.

For which records to ship, have the receiver report the offset it holds through,
per collection, and ship from there to the frontier. It needs a back-channel
that does not exist, since receiver progress travels today as a per-shard
task-id acknowledgement. It pays for itself by making the byte cap free: a
message that cannot carry the whole range carries a prefix, and the receiver's
next report resumes exactly there, with no continuation token and no resumption
state.
