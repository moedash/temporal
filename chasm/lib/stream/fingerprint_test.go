package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// hashedRecord is what an SDK sends when its codec rewrites the body on every
// call: the encoded bytes differ per attempt, the declared hash of the
// plaintext does not.
func hashedRecord(plaintext, encoded, topic string) *streamlib.StreamRecord {
	sum := sha256.Sum256([]byte(plaintext))
	return &streamlib.StreamRecord{
		Body:  &commonpb.Payload{Data: []byte(encoded)},
		Topic: topic,
		Kind:  streampb.STREAM_RECORD_KIND_DATA,
		Metadata: map[string]*commonpb.Payload{
			ContentHashMetadataKey: {Data: []byte(hex.EncodeToString(sum[:]))},
		},
	}
}

func TestDeclaredContentHashMakesAReencodedRetryARetry(t *testing.T) {
	s := newTestStream(t)
	for attempt := range 3 {
		result, err := s.AddMessages(nil, AddMessagesRequest{
			Records: []*streamlib.StreamRecord{
				hashedRecord("hello", fmt.Sprint("nonce-", attempt, "-hello"), "t"),
			},
			ProducerID: "p1", Sequence: 1,
		})
		require.NoError(t, err, "attempt %d", attempt)
		require.Equal(t, attempt > 0, result.Deduplicated)
		require.Equal(t, int64(0), result.FirstOffset)
	}
	require.Equal(t, int64(1), s.State.HeadOffset)
}

func TestDeclaredContentHashStillRefusesADivergentRepeat(t *testing.T) {
	s := newTestStream(t)
	_, err := s.AddMessages(nil, AddMessagesRequest{
		Records:    []*streamlib.StreamRecord{hashedRecord("hello", "enc-1", "t")},
		ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)

	// Same encoded bytes, different plaintext: the declared hash is what
	// counts, so this is a conflict even though the bytes match.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Records:    []*streamlib.StreamRecord{hashedRecord("goodbye", "enc-1", "t")},
		ProducerID: "p1", Sequence: 1,
	})
	require.Equal(t, ReasonProducerConflict, ReasonOf(err.Error()))

	// Same plaintext on another topic is a different write, not a retry.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Records:    []*streamlib.StreamRecord{hashedRecord("hello", "enc-1", "other")},
		ProducerID: "p1", Sequence: 1,
	})
	require.Equal(t, ReasonProducerConflict, ReasonOf(err.Error()))
	require.Equal(t, int64(1), s.State.HeadOffset)
}

func TestDeclaredContentHashCoversEveryRecordOfTheBatch(t *testing.T) {
	s := newTestStream(t)
	batch := func(second string) []*streamlib.StreamRecord {
		return []*streamlib.StreamRecord{
			hashedRecord("one", "enc-a", "t"),
			hashedRecord(second, "enc-b", "t"),
		}
	}
	_, err := s.AddMessages(nil, AddMessagesRequest{
		Records: batch("two"), ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)

	retry, err := s.AddMessages(nil, AddMessagesRequest{
		Records: batch("two"), ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)
	require.True(t, retry.Deduplicated)

	_, err = s.AddMessages(nil, AddMessagesRequest{
		Records: batch("three"), ProducerID: "p1", Sequence: 1,
	})
	require.Equal(t, ReasonProducerConflict, ReasonOf(err.Error()))
}

// A batch may mix records that declare a hash with ones that do not; the
// undeclared ones are identified by their encoded bytes as before.
func TestDeclaredContentHashMixesWithUndeclaredRecords(t *testing.T) {
	s := newTestStream(t)
	mixed := func(encoded, plainBody string) []*streamlib.StreamRecord {
		return []*streamlib.StreamRecord{
			hashedRecord("one", encoded, "t"),
			{Body: &commonpb.Payload{Data: []byte(plainBody)}, Kind: streampb.STREAM_RECORD_KIND_DATA},
		}
	}
	_, err := s.AddMessages(nil, AddMessagesRequest{
		Records: mixed("enc-1", "plain"), ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)

	retry, err := s.AddMessages(nil, AddMessagesRequest{
		Records: mixed("enc-2", "plain"), ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err, "the declared record may be re-encoded")
	require.True(t, retry.Deduplicated)

	_, err = s.AddMessages(nil, AddMessagesRequest{
		Records: mixed("enc-2", "changed"), ProducerID: "p1", Sequence: 1,
	})
	require.Equal(t, ReasonProducerConflict, ReasonOf(err.Error()),
		"the undeclared record is still compared by its bytes")
}

func TestDeclaredContentHashRefusesAMalformedValue(t *testing.T) {
	s := newTestStream(t)
	for name, value := range map[string]string{
		"not hex":   "zz",
		"too short": hex.EncodeToString([]byte("short")),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := s.AddMessages(nil, AddMessagesRequest{
				Records: []*streamlib.StreamRecord{{
					Body: &commonpb.Payload{Data: []byte("x")},
					Metadata: map[string]*commonpb.Payload{
						ContentHashMetadataKey: {Data: []byte(value)},
					},
				}},
				ProducerID: "p1", Sequence: 1,
			})
			var invalid *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalid)
			require.ErrorContains(t, err, ContentHashMetadataKey)
		})
	}
	require.Equal(t, int64(0), s.State.HeadOffset)
}

// Without the key the fingerprint is the encoded batch, exactly as before, so
// producer entries written before the key existed stay comparable.
func TestUndeclaredBatchIsFingerprintedOverItsBytes(t *testing.T) {
	records := msgs("a", "b")
	blob, err := marshalBatch(records)
	require.NoError(t, err)
	got, err := batchFingerprint(records, blob.Data)
	require.NoError(t, err)
	require.Equal(t, contentHash(blob.Data), got)
}

func TestDeclaredHashIsCaseInsensitive(t *testing.T) {
	s := newTestStream(t)
	sum := sha256.Sum256([]byte("hello"))
	record := func(digest string) *streamlib.StreamRecord {
		return &streamlib.StreamRecord{
			Body: &commonpb.Payload{Data: []byte("enc")},
			Kind: streampb.STREAM_RECORD_KIND_DATA,
			Metadata: map[string]*commonpb.Payload{
				ContentHashMetadataKey: {Data: []byte(digest)},
			},
		}
	}
	lower := hex.EncodeToString(sum[:])
	upper := fmt.Sprintf("%X", sum[:])
	_, err := s.AddMessages(nil, AddMessagesRequest{
		Records: []*streamlib.StreamRecord{record(lower)}, ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)
	retry, err := s.AddMessages(nil, AddMessagesRequest{
		Records: []*streamlib.StreamRecord{record(upper)}, ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)
	require.True(t, retry.Deduplicated)
}
