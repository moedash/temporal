"""Generate the CQL for the Option B stream-log partition measurement.

Three shapes, each in its own partition so tablestats can tell them apart:
  A  100k rows of 40 bytes   an unbatched token stream, one bucket of 100k offsets
  B  100 rows of 2 MB        the largest batch the current design permits
  C  50k rows of 200 bytes   a modestly batched stream
"""
import sys

NS = "11111111-1111-1111-1111-111111111111"

def rows(bucket, count, payload_bytes, offset_per_row=1):
    blob = "0x" + ("ab" * payload_bytes)
    out = []
    off = 0
    for _ in range(count):
        out.append(
            f"INSERT INTO stream_log (shard_id, namespace_id, collection_id, bucket, "
            f"start_offset, end_offset, data, data_encoding) VALUES "
            f"(1, {NS}, 'c1', {bucket}, {off}, {off + offset_per_row}, {blob}, 'Proto3');"
        )
        off += offset_per_row
    return out

print("""
CREATE KEYSPACE IF NOT EXISTS streammeasure WITH replication =
  {'class': 'SimpleStrategy', 'replication_factor': 1};
USE streammeasure;
DROP TABLE IF EXISTS stream_log;
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
""")

shape = sys.argv[1]
if shape == "A":
    print("\n".join(rows(bucket=0, count=100000, payload_bytes=20)))
elif shape == "B":
    print("\n".join(rows(bucket=1, count=50, payload_bytes=512 * 1024, offset_per_row=1000)))
elif shape == "C":
    print("\n".join(rows(bucket=2, count=50000, payload_bytes=100)))
