package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"go.temporal.io/api/serviceerror"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// ContentHashMetadataKey is the record metadata key under which a producer
// declares a record's identity before its payload codec runs: the lowercase
// hex SHA-256 of the converted body. See the package doc.
const ContentHashMetadataKey = "temporal.io/content-hash"

// batchFingerprint is what the producer table compares a repeat against.
//
// A batch with no declared hash is fingerprinted over its encoded bytes, which
// is exact and costs nothing extra. Once any record declares a hash, the
// fingerprint is built per record instead, so an encoding that changes on
// every call cannot turn a retry into a conflict.
func batchFingerprint(records []*streamlib.StreamRecord, encoded []byte) ([]byte, error) {
	declared := false
	digests := make([]byte, 0, sha256.Size*len(records))
	for i, record := range records {
		digest, ok, err := declaredContentHash(record)
		if err != nil {
			return nil, serviceerror.NewInvalidArgumentf("record %d: %v", i, err)
		}
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

// declaredContentHash reads the hash a record declares, reporting whether it
// declared one at all.
func declaredContentHash(record *streamlib.StreamRecord) ([]byte, bool, error) {
	payload, ok := record.GetMetadata()[ContentHashMetadataKey]
	if !ok {
		return nil, false, nil
	}
	text := strings.TrimSpace(string(payload.GetData()))
	digest, err := hex.DecodeString(text)
	if err != nil || len(digest) != sha256.Size {
		return nil, true, serviceerror.NewInvalidArgumentf(
			"metadata %q must be the hex SHA-256 of the record's converted body, got %q",
			ContentHashMetadataKey, text)
	}
	return digest, true, nil
}

func contentHash(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}
