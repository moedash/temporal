# Open question: registering a consumer pin from inside a Workflow Task

Status: unresolved. Blocks Path C across executions on any cluster with more
than one history host.

## What works now

Two of the three cross-execution steps are routed. `SubscribeWorkflow` reaches
the stream through `RegisterStreamConsumer`, routed on the stream id, and the
notify task reaches each consumer through `AdvanceConsumerHead`, routed on the
consumer's workflow id. Both were resolving refs through the local shard
controller, which refuses a shard the host does not own, so both only worked
when everything happened to live on one host.

The third does not fit that shape.

## The step that does not fit

`resolveStagedStreamSubscriptions` runs inside the consuming workflow's task
completion. A `SubscribeStream` command cannot resolve anything itself, because
a command handler runs under the state lock with nowhere to do I/O from, so the
command stages and the flush resolves. The flush registers the pin on the
stream and then writes the cursor onto the workflow, in that order, inside the
transaction that commits the workflow task.

That order is the guarantee. Interrupted between the two there is a pin holding
storage nothing reads, which costs space and is reclaimable. The other order
would leave a cursor with no pin behind it, and truncation would be free to take
a range that cursor still points at, which loses data a consumer was promised.

A synchronous cross-shard call cannot live there. The workflow's transaction is
open, it holds the execution lock, and the far shard may be on another host. So
the pin has to move out of the transaction, and the moment it does, the ordering
that made the guarantee is gone.

## Why the obvious answers do not work

**Emit a transfer task that registers the pin.** This is how signalling an
external workflow works, and it is the shape the rest of Temporal uses. It makes
the pin asynchronous: the workflow task commits with a cursor, and the pin
arrives later. Between the two, truncation can take the range the cursor points
at. That is precisely the failure the current order exists to prevent, now with
a wider window.

**Register the pin before the workflow task commits, from outside.** There is
nothing outside to do it. The subscribe originates in workflow code, and the
first moment the server knows about it is the command.

**Have the command handler do the I/O.** It cannot. That constraint is what
produced the staging design in the first place.

**Make truncation defensive: never truncate below any cursor, pin or not.**
Truncation cannot see cursors it does not have a pin for. The pin is how a
consumer in another execution becomes visible to the stream at all.

## Directions worth costing

1. **A cursor that is not usable until its pin is confirmed.** The workflow task
   commits a cursor in a pending state that delivers nothing. A transfer task
   registers the pin and then marks the cursor live. Truncation ignores pending
   cursors, so it can still take the range, but a pending cursor that finds its
   start offset already truncated fails the subscription cleanly rather than
   silently skipping data. Cost: a state on the cursor, a task, and a visible
   failure mode for a subscription that was too slow to pin.

2. **Pin first, from the frontend, before the command is issued.** Move
   subscribe out of workflow code and make it something a client does, the way
   `SubscribeWorkflow` already is for external callers. The workflow then only
   reads. This removes the problem rather than solving it, at the cost of the
   ergonomics: a workflow can no longer subscribe to a stream by itself.

3. **A reservation with a lease.** The command handler cannot do I/O, but the
   flush can, and the flush is still inside the transaction. A short-lived
   reservation written locally, honoured by truncation for its lease duration,
   converted to a real pin by a transfer task. Cost: truncation has to consult
   something with a clock, which it currently does not.

4. **Let the substrate decide it.** If the log moves to a dedicated table keyed
   by `(shard, collection, offset)`, the pin and the cursor may be able to live
   in one place and the ordering question dissolves. This is the argument for
   settling the substrate before spending anything here.

## Recommendation

Do not build any of these yet. The cheapest correct thing today is to say that
Path C across executions is single-host only, which is now true and visible
rather than true and silent. Direction 1 is the one to cost first if Path C has
to work across hosts before the substrate is settled, and direction 4 is the
reason not to start.
