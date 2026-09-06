package mysql

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// Upsert: the key is the offset a batch starts at, so a rewrite of that
	// offset is a retry of the same append rather than a competing one.
	insertStreamLogQuery = `INSERT INTO stream_log
		(shard_id, namespace_id, collection_id, bucket, start_offset, next_offset, data, data_encoding)
		VALUES (:shard_id, :namespace_id, :collection_id, :bucket, :start_offset, :next_offset, :data, :data_encoding)
		ON DUPLICATE KEY UPDATE next_offset = VALUES(next_offset),
		                        data = VALUES(data),
		                        data_encoding = VALUES(data_encoding)`

	// The floor subquery finds the batch holding an arbitrary offset: the last
	// row starting at or below it.
	getStreamLogQuery = `SELECT shard_id, namespace_id, collection_id, bucket, start_offset, next_offset, data, data_encoding ` +
		`FROM stream_log ` +
		`WHERE shard_id = ? AND namespace_id = ? AND collection_id = ? AND bucket = ? ` +
		`AND start_offset >= COALESCE((SELECT MAX(start_offset) FROM stream_log ` +
		`WHERE shard_id = ? AND namespace_id = ? AND collection_id = ? AND bucket = ? AND start_offset <= ?), ?0) ` +
		`AND start_offset < ?1 ORDER BY start_offset LIMIT ?2`

	deleteStreamLogQuery = `DELETE FROM stream_log ` +
		`WHERE shard_id = ? AND namespace_id = ? AND collection_id = ? AND bucket = ?`
)

func (mdb *db) InsertIntoStreamLog(
	ctx context.Context,
	row *sqlplugin.StreamLogRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx, insertStreamLogQuery, row)
}

func (mdb *db) RangeSelectFromStreamLog(
	ctx context.Context,
	filter sqlplugin.StreamLogSelectFilter,
) ([]sqlplugin.StreamLogRow, error) {
	var rows []sqlplugin.StreamLogRow
	if err := mdb.SelectContext(ctx, &rows, getStreamLogQuery,
		filter.ShardID, filter.NamespaceID, filter.CollectionID, filter.Bucket,
		filter.ShardID, filter.NamespaceID, filter.CollectionID, filter.Bucket,
		filter.MinOffset,
		filter.MinOffset,
		filter.MaxOffset,
		filter.PageSize,
	); err != nil {
		return nil, err
	}
	return rows, nil
}

func (mdb *db) DeleteFromStreamLog(
	ctx context.Context,
	filter sqlplugin.StreamLogDeleteFilter,
) (sql.Result, error) {
	return mdb.ExecContext(ctx, deleteStreamLogQuery,
		filter.ShardID, filter.NamespaceID, filter.CollectionID, filter.Bucket)
}
