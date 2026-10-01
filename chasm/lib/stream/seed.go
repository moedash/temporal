package stream

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
)

// Seed copies the records of another stream's window [from, to), read with
// ReadBatches, into this one, which has to begin at from and hold nothing yet.
// The records keep their offsets, so a history that names them stays true. No
// producer entry comes along: the copy is this stream's own past, not an
// append anyone could retry.
//
// Made for a reset. The run reset from keeps its stream, and what it held past
// the inherited cursor is the input the reset-point task had been given; the
// run that re-runs that task needs the same input in a stream of its own.
func (s *Stream) Seed(
	mctx chasm.MutableContext,
	blobs []*commonpb.DataBlob,
	starts []int64,
	from int64,
	to int64,
) error {
	if s.State.BaseOffset != from || s.State.HeadOffset != from || len(s.Batches) != 0 {
		return serviceerror.NewInternalf(
			"a stream holding [%d,%d) cannot be seeded from offset %d",
			s.State.BaseOffset, s.State.HeadOffset, from)
	}
	if s.Batches == nil {
		s.Batches = make(chasm.Map[int64, *commonpb.DataBlob])
	}
	for i, blob := range blobs {
		var batch streamlib.StreamRecordBatch
		if err := proto.Unmarshal(blob.GetData(), &batch); err != nil {
			return err
		}
		// A batch straddling the start is trimmed, so the copy begins exactly
		// at the inherited offset and is keyed there.
		var kept []*streamlib.StreamRecord
		for j, record := range batch.GetRecords() {
			if offset := starts[i] + int64(j); offset >= from && offset < to {
				kept = append(kept, record)
			}
		}
		if len(kept) == 0 {
			continue
		}
		first := max(starts[i], from)
		if first != s.State.HeadOffset {
			return serviceerror.NewInternalf(
				"seeding a stream at %d with a batch starting at %d", s.State.HeadOffset, first)
		}
		// The copy keeps the source batch's age: the records are as old as
		// they were, whichever stream holds them.
		var appendedAt time.Time
		if batch.GetAppendedAt() != nil {
			appendedAt = batch.GetAppendedAt().AsTime()
		}
		encoded, err := marshalBatch(kept, appendedAt)
		if err != nil {
			return err
		}
		s.Batches[first] = chasm.NewDataField(mctx, encoded)
		s.State.HeadOffset = first + int64(len(kept))
		s.State.AppendedBytes += int64(len(encoded.Data))
		s.State.HeldBytes += int64(len(encoded.Data))
	}
	if s.State.HeadOffset != to {
		return serviceerror.NewInternalf(
			"seeding stopped at offset %d, short of %d", s.State.HeadOffset, to)
	}
	return nil
}
