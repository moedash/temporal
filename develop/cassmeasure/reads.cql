USE streammeasure;
TRACING ON;
-- Find the batch containing an arbitrary offset: reverse slice, one row.
SELECT start_offset, end_offset FROM stream_log
  WHERE shard_id=1 AND namespace_id=11111111-1111-1111-1111-111111111111
    AND collection_id='c1' AND bucket=0 AND start_offset <= 73456
  ORDER BY start_offset DESC LIMIT 1;
-- Read forward from there for a page.
SELECT start_offset, end_offset FROM stream_log
  WHERE shard_id=1 AND namespace_id=11111111-1111-1111-1111-111111111111
    AND collection_id='c1' AND bucket=0 AND start_offset >= 73456 LIMIT 100;
