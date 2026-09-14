CREATE TABLE stream_log (
  shard_id          int,
  namespace_id      uuid,
  collection_id     text,
  bucket            bigint,  -- one bucket is one partition, bounded by offsets and by bytes
  start_offset      bigint,  -- first offset in this batch, and the key a retry addresses
  next_offset        bigint,  -- exclusive, so a reader knows the span without decoding
  data              blob,
  data_encoding     text,
  PRIMARY KEY ((shard_id, namespace_id, collection_id, bucket), start_offset)
) WITH CLUSTERING ORDER BY (start_offset ASC)
  AND COMPACTION = {
     'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
};
