package sqlite

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// Upsert, because the key is the offset a batch starts at and a rewrite of
	// that offset is a retry of the same append rather than a competing one.
	// This is the property the whole substrate exists for: there is nothing to
	// order two writers against, so nothing to get wrong.
	insertStreamLogQuery = `INSERT INTO stream_log
		(shard_id, namespace_id, collection_id, bucket, start_offset, next_offset, data, data_encoding)
		VALUES (:shard_id, :namespace_id, :collection_id, :bucket, :start_offset, :next_offset, :data, :data_encoding)
		ON CONFLICT (shard_id, namespace_id, collection_id, bucket, start_offset)
		DO UPDATE SET next_offset = excluded.next_offset,
		              data = excluded.data,
		              data_encoding = excluded.data_encoding`

	// The floor subquery is what lets a reader ask for an arbitrary offset. A
	// batch covers a range, so the row holding an offset is the last one that
	// starts at or below it, and only the store can find that in one step.
	getStreamLogQuery = `SELECT shard_id, namespace_id, collection_id, bucket, start_offset, next_offset, data, data_encoding
		FROM stream_log
		WHERE shard_id = ? AND namespace_id = ? AND collection_id = ? AND bucket = ?
		AND start_offset >= COALESCE((
			SELECT MAX(start_offset) FROM stream_log
			WHERE shard_id = ? AND namespace_id = ? AND collection_id = ? AND bucket = ?
			AND start_offset <= ?
		), ?)
		AND start_offset < ?
		ORDER BY start_offset LIMIT ?`

	deleteStreamLogQuery = `DELETE FROM stream_log
		WHERE shard_id = ? AND namespace_id = ? AND collection_id = ? AND bucket = ?`
)

func (mdb *db) InsertIntoStreamLog(
	ctx context.Context,
	row *sqlplugin.StreamLogRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx, insertStreamLogQuery, row)
}

func (mdb *db) RangeSelectFromStreamLog(
	ctx context.Context,
	filter sqlplugin.StreamLogSelectFilter,
) ([]sqlplugin.StreamLogRow, error) {
	var rows []sqlplugin.StreamLogRow
	if err := mdb.conn.SelectContext(ctx, &rows, getStreamLogQuery,
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
	return mdb.conn.ExecContext(ctx, deleteStreamLogQuery,
		filter.ShardID, filter.NamespaceID, filter.CollectionID, filter.Bucket)
}
