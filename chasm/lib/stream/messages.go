package stream

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
)

// CollectRecords decodes the batches covering a range and trims to the
// requested window. Decoding happens only here and only on the batches a read
// actually touches; the store never interprets them, and user payloads stay
// opaque because the codec runs in the SDK.
func CollectRecords(
	blobs []*commonpb.DataBlob,
	startOffsets []int64,
	from int64,
	head int64,
	maxMessages int,
	topics []string,
) ([]*streamlib.StreamRecord, int64, error) {
	wanted := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		wanted[t] = struct{}{}
	}

	var out []*streamlib.StreamRecord
	next := from
	for i, blob := range blobs {
		var batch streamlib.StreamRecordBatch
		if err := proto.Unmarshal(blob.GetData(), &batch); err != nil {
			return nil, 0, err
		}
		for j, msg := range batch.GetRecords() {
			offset := startOffsets[i] + int64(j)
			if offset < from || offset >= head {
				continue
			}
			if len(out) >= maxMessages {
				return out, next, nil
			}
			next = offset + 1
			if len(wanted) > 0 {
				if _, ok := wanted[msg.GetTopic()]; !ok {
					continue
				}
			}
			// Set here rather than stored: it is decided by where the
			// message sits in the log, not by what the producer wrote.
			msg.Offset = offset
			out = append(out, msg)
		}
	}
	return out, next, nil
}

// ToAPIRecords converts stored records to the shape carried on a Workflow
// Task. Every stored field crosses over except the offset, which the slice
// carries as a range, so a reader in any language sees the record the producer
// wrote, kind and identity included.
func ToAPIRecords(in []*streamlib.StreamRecord) []*streampb.StreamRecord {
	out := make([]*streampb.StreamRecord, 0, len(in))
	for _, m := range in {
		out = append(out, &streampb.StreamRecord{
			Body:       m.GetBody(),
			Metadata:   m.GetMetadata(),
			Topic:      m.GetTopic(),
			Kind:       m.GetKind(),
			ProducerId: m.GetProducerId(),
			Attempt:    m.GetAttempt(),
			Sequence:   m.GetSequence(),
		})
	}
	return out
}

// CapByBytes trims a contiguous run of records to a byte budget and returns
// the offset just past the last one kept.
//
// It always keeps the first record, however large. Dropping it would leave the
// cursor unable to advance, and since an unconsumed range schedules a workflow
// task, a stream holding one oversized record would wake the workflow forever
// without ever delivering anything.
func CapByBytes(
	records []*streamlib.StreamRecord,
	from int64,
	maxBytes int,
) ([]*streamlib.StreamRecord, int64) {
	if len(records) == 0 {
		return records, from
	}

	total := 0
	for i, m := range records {
		total += proto.Size(m)
		if total > maxBytes && i > 0 {
			return records[:i], from + int64(i)
		}
	}
	return records, from + int64(len(records))
}

// checkBatchBytes rejects an append that is too large to store or too large to
// ever hand back.
//
// The per-record bound matters on its own: a record is never split, so one
// that exceeds a consumer's byte budget can never be delivered, and CapByBytes
// would hand it over alone forever rather than reject it. The batch bound is
// the storage side, since a batch is written as a single node.
func checkBatchBytes(records []*streamlib.StreamRecord, limits Limits) error {
	total := 0
	for i, m := range records {
		size := proto.Size(m)
		if size > limits.MaxMessageBytes {
			return serviceerror.NewInvalidArgumentf(
				"record %d is %d bytes, over the %d byte limit", i, size, limits.MaxMessageBytes)
		}
		total += size
	}
	if total > limits.MaxBatchBytes {
		return serviceerror.NewInvalidArgumentf(
			"batch is %d bytes, over the %d byte limit", total, limits.MaxBatchBytes)
	}
	return nil
}
