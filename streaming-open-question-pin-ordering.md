# Registering a consumer pin from inside a Workflow Task

Status: resolved by removing the pin. Kept because the reasoning matters more
than the answer, and because the answer is a weakening of a guarantee.

## What the question was

A `SubscribeStream` command cannot resolve anything itself: a command handler
runs under the state lock with nowhere to do I/O from. So the command staged,
and the flush registered the consumer's pin on the stream and then wrote the
cursor onto the workflow, in that order, inside the transaction committing the
workflow task.

That order was the guarantee. Interrupted between the two there is a pin
holding storage nothing reads, which costs space and is reclaimable. The other
order would leave a cursor with no pin, and truncation would be free to take a
range that cursor still points at.

A synchronous cross-shard call cannot live there. The workflow's transaction is
open, it holds the execution lock, and the stream may be on another host. Move
the pin out and the ordering goes with it.

## Why it is no longer a question

The pin did not do what it claimed. Nothing released it when a consumer
finished, so a stream with a cap kept everything for as long as a consumer had
ever been registered. The floor was not protecting a reader; it was disabling
the cap. An outside review found that, and it is what makes the trade obvious.

So the pin is gone. Registering a consumer says who to wake when the frontier
moves, and nothing more. Truncation applies the cap it was asked for.

A consumer that falls behind the floor is told. On the polling path that was
already true: a read below the base returns an error naming where the stream
now starts. The delivery path now does the same, and used not to, which was the
real hazard: it would have read whatever survived and handed the workflow a
range with a hole in it that nothing could see.

This is what a log with a retention window does everywhere else. A reader that
cannot keep up gets an out-of-range error rather than silent loss, and the
window is the thing the operator asked for.

## What this costs

A workflow consuming a capped stream can now be outrun and fail. Before, it
could not be outrun, because the cap did not work. The failure is explicit, it
names the offsets, and it happens when the workflow next tries to read.

For a stream with no cap and no retention, nothing changes: there is no floor
to fall behind.

## What it buys, beyond the cap working

The cross-execution write disappears from the workflow-task transaction, which
is what made this a design question rather than plumbing. Subscribing from
workflow code no longer has to reach another shard while holding the execution
lock, so the last of the three cross-execution steps stops being a problem
rather than being solved with machinery.

## What is still not settled

Registering a consumer still happens in the flush, and still reaches the
stream's shard. That is now a call whose failure costs a delayed wake-up rather
than a lost pin, so it can be retried or deferred without risking data. Making
it asynchronous is a tidy-up, not a correctness fix, and is not done.
