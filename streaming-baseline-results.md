# Workflow Streams: measured baseline

| | |
|---|---|
| Status | First measurement, single run per cell |
| Ticket | [AI-198](https://temporalio.atlassian.net/browse/AI-198), Stage 0 |
| Harness | `tests/streaming_baseline_test.go` |
| Date | 2026-08-24 |

What today's Workflow Streams pattern costs, measured rather than argued. Native Streams is a proposal to replace it, and a replacement cannot be justified without this.

## Method

The shipped pattern reproduced end to end: a producer generates messages continuously and flushes them into the workflow as batched **Signals**; consumers read them back with a long-polling **Update**; the workflow holds the buffer.

- 40 messages/sec of 20 bytes, 50 seconds, so about 2000 messages per cell.
- Flush intervals 2s (the shipped default) and 100ms (the bar the Native Streams 1-pager sets).
- 1, 5, and 25 concurrent subscribers.
- Dedicated single-node cluster, SQLite, namespace-scoped `persistence_requests`.
- Latency is stamped at **generation**, not at flush. Stamping at flush measures only the server round trip and hides the batching delay, which is the dominant term.
- Persistence counts exclude cluster, namespace, and workflow start, so they reflect streaming steady state.

Reproduce with `TEMPORAL_STREAM_BENCH=1 go test -tags test_dep ./tests/ -run TestStreamingBaseline -v -timeout 40m`. Without the variable a two-cell short version runs, which keeps the harness from rotting.

## Results

| scenario | msgs | delivered | rejected polls | hist events/msg | hist bytes/msg | persist ops/msg | p50 | p99 |
|---|---|---|---|---|---|---|---|---|
| flush 2s, 1 sub | 2000 | 2000 | 2 | 0.11 | 70.85 | 0.08 | 989ms | 1.985s |
| flush 2s, 5 subs | 2000 | 10000 | 10 | 0.22 | 198.15 | 0.10 | 1.008s | 1.986s |
| flush 2s, 25 subs | 1999 | 21830 | 4771 | 0.36 | 385.82 | 2.58 | 1.09s | 40.571s |
| flush 100ms, 1 sub | 1999 | 1999 | 2 | 2.25 | 373.64 | 1.53 | 57ms | 108ms |
| flush 100ms, 5 subs | 1999 | 8000 | 2328 | 3.62 | 699.73 | 2.61 | 62ms | 109ms |
| flush 100ms, 25 subs | 2000 | 15422 | 11621 | 3.13 | 735.60 | 6.91 | 108ms | 38.881s |

"Delivered" counts message receipts across all subscribers, so the expected value is messages times subscribers.

## What it shows

### Latency is bought with cost, at roughly 20x

At one subscriber, tightening the flush from 2s to 100ms improves p50 latency 17x (989ms to 57ms) and costs:

- **20x** more history events per message (0.11 to 2.25)
- **5.3x** more history bytes per message (70.9 to 373.6)
- **19x** more persistence operations per message (0.08 to 1.53)

The p50 and p99 figures are a check on the method as much as a result: p50 lands at about half the flush interval and p99 at about the full interval, which is what a uniform batching delay must produce. If they had not, the measurement would have been wrong.

### There is a hard ceiling at 10 concurrent subscribers

`history.maxInFlightUpdates` defaults to **10** per workflow execution (`common/dynamicconfig/constants.go:2559`). At 25 subscribers the pattern delivers 44% of expected receipts with 4771 rejected polls and a p99 of 40 seconds, even at the cheap 2s flush. This is the "maximum of 10 concurrent subscribers" limit from the 1-pager, now with numbers attached: it does not degrade gracefully, it starves.

### There is a second ceiling nobody has been talking about

`history.maxTotalUpdates` defaults to **2000 per workflow execution** (`constants.go:2569`). Every poll consumes one, whether or not it returns anything.

At 100ms batching a single subscriber burns about 500 polls in 50 seconds. Five subscribers exhaust a workflow's entire lifetime update budget in **under a minute**, which is exactly what the 100ms/5-subscriber cell shows: delivery stops dead at 8000 receipts with 2328 rejections following.

The implication for agent sessions is the sharper form of the cost problem. A workflow is not merely paying per stream item; **it is spending a finite, non-renewable per-execution budget on the act of reading**. A long agent conversation would need continue-as-new every few minutes purely to reset a poll counter, which is a lifecycle event driven by the transport rather than by the application.

This is the strongest argument in the data for moving reads off the workflow entirely. A design where reading costs no state transition does not have this ceiling at all, because there is nothing to exhaust.

## Caveats

- Single run per cell, no repetitions, so treat the numbers as an order of magnitude rather than a precise figure.
- SQLite on a single-node dev cluster. Cassandra behaviour, especially per-partition cost, is not addressed and these numbers must not be read as speaking to it.
- Persistence ops per message in the rejecting cells (2.58 and 6.91) are inflated by retry traffic from rejected polls. The uncontended cells (0.08 and 1.53) are the ones to quote for the cost comparison.
- The harness disables the test logger's failure-on-error behaviour, because a saturated cluster torn down mid-drain always logs shard-status errors. An anomalous result should be re-run with that off before it is trusted.
- Latency is measured from a simulated generation clock, not from a real LLM token stream.

## Next

These become the left-hand column of the comparison once the native path exists. The figures to beat are 1.53 persistence operations and 2.25 history events per message at 100ms, with no subscriber ceiling and no per-execution poll budget.
