package stream

import (
	commonpb "go.temporal.io/api/common/v1"
	apistreampb "go.temporal.io/api/stream/v1"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
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
) ([]*streampb.StreamMessage, int64, error) {
	wanted := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		wanted[t] = struct{}{}
	}

	var out []*streampb.StreamMessage
	next := from
	for i, blob := range blobs {
		var batch streampb.StreamMessageBatch
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
func ToAPIMessages(in []*streampb.StreamMessage) []*apistreampb.StreamMessage {
	out := make([]*apistreampb.StreamMessage, 0, len(in))
	for _, m := range in {
		if m.GetKind() != streampb.STREAM_MESSAGE_KIND_DATA {
			continue
		}
		out = append(out, &apistreampb.StreamMessage{
			Body:          m.GetBody(),
			Metadata:      m.GetMetadata(),
			Topic:         m.GetTopic(),
			TopicSequence: m.GetTopicSequence(),
		})
	}
	return out
}
