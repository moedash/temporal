package stream

import (
	commonpb "go.temporal.io/api/common/v1"
	streampb "go.temporal.io/api/stream/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
)

// CollectMessages decodes the batches covering a range and trims to the
// requested window. Decoding happens only here and only on the batches a read
// actually touches; the store never interprets them, and user payloads stay
// opaque because the codec runs in the SDK.
func CollectMessages(
	blobs []*commonpb.DataBlob,
	startOffsets []int64,
	from int64,
	head int64,
	maxMessages int,
	topics []string,
) ([]*streamlib.StreamMessage, int64, error) {
	wanted := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		wanted[t] = struct{}{}
	}

	var out []*streamlib.StreamMessage
	next := from
	for i, blob := range blobs {
		var batch streamlib.StreamMessageBatch
		if err := proto.Unmarshal(blob.GetData(), &batch); err != nil {
			return nil, 0, err
		}
		for j, msg := range batch.GetMessages() {
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
			out = append(out, msg)
		}
	}
	return out, next, nil
}

// ToAPIMessages converts stored messages to the shape carried on a Workflow
// Task. Control messages are dropped: they steer the log itself and mean
// nothing to a consumer.
func ToAPIMessages(in []*streamlib.StreamMessage) []*streampb.StreamMessage {
	out := make([]*streampb.StreamMessage, 0, len(in))
	for _, m := range in {
		if m.GetKind() != streamlib.STREAM_MESSAGE_KIND_DATA {
			continue
		}
		out = append(out, &streampb.StreamMessage{
			Body:          m.GetBody(),
			Metadata:      m.GetMetadata(),
			Topic:         m.GetTopic(),
			TopicSequence: m.GetTopicSequence(),
		})
	}
	return out
}

// CapByBytes trims a contiguous run of messages to a byte budget and returns
// the offset just past the last one kept.
//
// It always keeps the first message, however large. Dropping it would leave the
// cursor unable to advance, and since an unconsumed range now schedules a
// workflow task, a stream holding one oversized message would wake the workflow
// forever without ever delivering anything.
func CapByBytes(
	messages []*streamlib.StreamMessage,
	from int64,
	maxBytes int,
) ([]*streamlib.StreamMessage, int64) {
	if len(messages) == 0 {
		return messages, from
	}

	total := 0
	for i, m := range messages {
		total += proto.Size(m)
		if total > maxBytes && i > 0 {
			return messages[:i], from + int64(i)
		}
	}
	return messages, from + int64(len(messages))
}
