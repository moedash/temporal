package postgresql

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
		ON CONFLICT (shard_id, namespace_id, collection_id, bucket, start_offset)
		DO UPDATE SET next_offset = excluded.next_offset,
		              data = excluded.data,
		              data_encoding = excluded.data_encoding`

	// The floor subquery finds the batch holding an arbitrary offset: the last
	// row starting at or below it.
	getStreamLogQuery = `SELECT shard_id, namespace_id, collection_id, bucket, start_offset, next_offset, data, data_encoding ` +
		`FROM stream_log ` +
		`WHERE shard_id = $1 AND namespace_id = $2 AND collection_id = $3 AND bucket = $4 ` +
		`AND start_offset >= COALESCE((SELECT MAX(start_offset) FROM stream_log ` +
		`WHERE shard_id = $5 AND namespace_id = $6 AND collection_id = $7 AND bucket = $8 AND start_offset <= $9), $10) ` +
		`AND start_offset < $11 ORDER BY start_offset LIMIT $12`

	deleteStreamLogQuery = `DELETE FROM stream_log ` +
		`WHERE shard_id = $1 AND namespace_id = $2 AND collection_id = $3 AND bucket = $4`
)

func (pdb *db) InsertIntoStreamLog(
	ctx context.Context,
	row *sqlplugin.StreamLogRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx, insertStreamLogQuery, row)
}

func (pdb *db) RangeSelectFromStreamLog(
	ctx context.Context,
	filter sqlplugin.StreamLogSelectFilter,
) ([]sqlplugin.StreamLogRow, error) {
	var rows []sqlplugin.StreamLogRow
	if err := pdb.SelectContext(ctx, &rows, getStreamLogQuery,
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

func (pdb *db) DeleteFromStreamLog(
	ctx context.Context,
	filter sqlplugin.StreamLogDeleteFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx, deleteStreamLogQuery,
		filter.ShardID, filter.NamespaceID, filter.CollectionID, filter.Bucket)
}
