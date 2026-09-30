package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// ContentHashMetadataKey is the record metadata key under which a producer
// declares a record's identity before its payload codec runs. The value is a
// payload with encoding binary/plain whose data is the 64 lowercase hex
// characters of [ContentHashOf] over the converted body. See the package doc.
const ContentHashMetadataKey = "temporal.io/content-hash"

// ContentHashOf is the SHA-256 a producer declares for a body: over the
// deterministic proto serialization of the converted payload, metadata and
// data, before any codec or external storage touches it.
func ContentHashOf(body *commonpb.Payload) ([]byte, error) {
	data, err := marshalDeterministic(body)
	if err != nil {
		return nil, err
	}
	return contentHash(data), nil
}

// batchFingerprint is what the producer table compares a repeat against.
//
// A batch with no declared hash is fingerprinted over its encoded bytes, which
// is exact and costs nothing extra. Once any record declares a hash, the
// fingerprint is built per record instead, so an encoding that changes on
// every call cannot turn a retry into a conflict.
func batchFingerprint(records []*streamlib.StreamRecord, encoded []byte) ([]byte, error) {
	declared := false
	digests := make([]byte, 0, sha256.Size*len(records))
	for _, record := range records {
		digest, ok := declaredContentHash(record)
		if !ok {
			data, err := marshalDeterministic(record)
			if err != nil {
				return nil, err
			}
			digests = append(digests, contentHash(data)...)
			continue
		}
		declared = true
		// The declared hash stands in for the body and the metadata, which are
		// what a codec may rewrite. The plain fields still count, so a repeat
		// that keeps the body but moves it to another topic is not mistaken
		// for the same write.
		plain, err := marshalDeterministic(&streamlib.StreamRecord{
			Topic:      record.GetTopic(),
			Kind:       record.GetKind(),
			Sequence:   record.GetSequence(),
			Attempt:    record.GetAttempt(),
			ProducerId: record.GetProducerId(),
		})
		if err != nil {
			return nil, err
		}
		digests = append(digests, contentHash(append(digest, plain...))...)
	}
	if !declared {
		return contentHash(encoded), nil
	}
	return contentHash(digests), nil
}

// declaredContentHash reads the hash a record declares. A record without the
// key, or whose value is not a hex SHA-256, reports none and is fingerprinted
// by its bytes: a value a codec has encoded is unreadable here, and refusing
// the append for it would make the key worse than no key at all.
func declaredContentHash(record *streamlib.StreamRecord) ([]byte, bool) {
	payload, ok := record.GetMetadata()[ContentHashMetadataKey]
	if !ok {
		return nil, false
	}
	digest, err := hex.DecodeString(strings.TrimSpace(string(payload.GetData())))
	if err != nil || len(digest) != sha256.Size {
		return nil, false
	}
	return digest, true
}

func contentHash(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}
