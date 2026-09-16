CREATE TABLE stream_log (
  shard_id INTEGER NOT NULL,
  namespace_id BYTEA NOT NULL,
  collection_id VARCHAR(255) NOT NULL,
  bucket BIGINT NOT NULL,
  start_offset BIGINT NOT NULL,
  --
  next_offset BIGINT NOT NULL,
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, collection_id, bucket, start_offset)
);
