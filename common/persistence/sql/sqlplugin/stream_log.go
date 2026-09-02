package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// StreamLogRow is one appended batch, keyed by the offset its first message
	// landed at.
	//
	// Keying by offset rather than by an opaque node id is what makes a write
	// idempotent: a retry of the same append addresses the same row and
	// replaces it. There is no chain to order two writers against, and no way
	// for an uncommitted write to outrank a later one. The frontier the stream
	// component holds is the only thing that decides what a reader may see.
	StreamLogRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		CollectionID string
		Bucket       int64
		StartOffset  int64
		// One past the last offset this batch covers, so a reader knows whether
		// the row holds the offset it asked for without decoding the blob.
		//
		// Not called end_offset: the schema loader scans for the SQL keyword
		// `END` and a column beginning with it fails to parse.
		NextOffset   int64
		Data         []byte
		DataEncoding string
	}

	// StreamLogSelectFilter reads the batches covering [MinOffset, MaxOffset).
	//
	// The read begins at the batch containing MinOffset, which is the greatest
	// start_offset at or below it, not at MinOffset itself. A store resolves
	// that itself; the caller does not have to guess how far back a batch may
	// have begun.
	StreamLogSelectFilter struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		CollectionID string
		Bucket       int64
		MinOffset    int64
		MaxOffset    int64
		PageSize     int
	}

	// StreamLogDeleteFilter drops a whole bucket, which is the unit of
	// reclamation.
	StreamLogDeleteFilter struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		CollectionID string
		Bucket       int64
	}

	// StreamLog is the SQL persistence interface for stream log batches.
	StreamLog interface {
		InsertIntoStreamLog(ctx context.Context, row *StreamLogRow) (sql.Result, error)
		RangeSelectFromStreamLog(ctx context.Context, filter StreamLogSelectFilter) ([]StreamLogRow, error)
		DeleteFromStreamLog(ctx context.Context, filter StreamLogDeleteFilter) (sql.Result, error)
	}
)
