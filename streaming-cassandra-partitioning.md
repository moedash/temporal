# Cassandra partitioning for the stream log

Measured 2026-09-02 against Cassandra 5.0.9, single node, on the schema the
dedicated-facet substrate would use. This was named as the single biggest risk
to a Temporal-owned stream and had never been run.

## What was measured

The proposed table, one partition per bucket:

```sql
CREATE TABLE stream_log (
  shard_id      int,
  namespace_id  uuid,
  collection_id text,
  bucket        bigint,
  start_offset  bigint,
  end_offset    bigint,
  data          blob,
  data_encoding text,
  PRIMARY KEY ((shard_id, namespace_id, collection_id, bucket), start_offset)
) WITH CLUSTERING ORDER BY (start_offset ASC);
```

One row is one appended batch, keyed by the offset its first message landed at.

| shape | rows in the bucket | payload per row | partition |
|---|---|---|---|
| unbatched token stream | 100,000 | 20 B | 5.84 MB |
| lightly batched | 50,000 | 100 B | 4.87 MB |
| large batches | 50 | 512 KB | 30.13 MB |

Sizes are `Compacted partition bytes` from `nodetool tablestats`, which is the
logical size. On-disk figures are not quoted: the synthetic payload is a
repeating byte pattern and compresses far better than text would.

## What it says

**Row count binds before size does, for the shape that matters.** An agent
session of 100k unbatched tokens fits in 5.84 MB, comfortably inside the 100 MB
guidance, but it is 100,000 rows in one partition, which is exactly the
rule-of-thumb ceiling. Bucket size therefore has to be chosen against the
unbatched case, because a bucket spans offsets and only the producer decides how
many offsets ride in a row.

A bucket of 10,000 offsets gives about 580 KB and 10,000 rows unbatched, which
leaves room on both axes. That is the number to start from, not the current
100,000.

**The current byte limit permits partitions that are too large.** A batch may
carry up to 1,000 messages, each up to the payload limit. At 2 KB per message
that is a 2 MB row, and a 100,000-offset bucket holds 100 of them: roughly
200 MB, twice the guidance, in cells that are themselves against Cassandra's
advice. The measured 512 KB rows already produce a 30 MB partition from 50 rows.

So a bucket needs a byte budget as well as an offset budget, and must roll on
whichever is reached first. The current design has only the offset budget. This
is a design gap the measurement found, and it applies to the existing substrate
as much as to the proposed one.

**Reads position in one row.** Tracing a read for an offset deep inside the
100,000-row partition:

```
SELECT ... WHERE <partition> AND start_offset <= 73456 ORDER BY start_offset DESC LIMIT 1
  -> Read 1 live rows and 0 tombstone cells
SELECT ... WHERE <partition> AND start_offset >= 73456 LIMIT 100
  -> Read 100 live rows and 0 tombstone cells
```

One row to find the batch containing an arbitrary offset, then exactly the page
asked for. Keying by the offset a batch starts at is what buys that.

The current `history_node` substrate cannot do it. A node id is the first offset
of its batch, so a read starting inside a batch does not know where that batch
began and compensates by starting `MaxMessagesPerBatch` rows earlier
(`chasm/lib/stream/log.go`, `startNode := NodeIDOf(...) - MaxMessagesPerBatch + 1`).
That is up to 1,000 clustering rows read behind the target on every poll, for
every reader.

## Conclusion

Cassandra is not the obstacle it was feared to be, on either substrate, provided
buckets roll on bytes as well as offsets. What the measurement does show is a
read-amplification difference that favours the dedicated facet by three orders
of magnitude per poll, which is a second reason to move beyond the correctness
ones.

## Reproducing

`develop/cassmeasure/` has the generator and the read trace. Start Cassandra with
`docker compose -f develop/docker-compose/docker-compose.yml up -d cassandra`.

## Caveats

Single node, no replication, no compaction pressure, no concurrent writers.
Synthetic compressible payloads. Partition sizes are logical rather than on
disk. None of that changes the row counts or the read shapes, which is what the
conclusions rest on.
