package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	streampb "go.temporal.io/api/stream/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/proto"
)

// plainBody is a converted payload as the SDK sees it before its codec runs.
func plainBody(text string) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     []byte(text),
	}
}

// declaredHash is the metadata value the SDK sends: binary/plain, 64 lowercase
// hex bytes of the SHA-256 over the deterministic serialization of the body.
func declaredHash(t *testing.T, body *commonpb.Payload) *commonpb.Payload {
	t.Helper()
	sum, err := ContentHashOf(body)
	require.NoError(t, err)
	return &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
		Data:     []byte(hex.EncodeToString(sum)),
	}
}

// hashedRecord is what an SDK sends when its codec rewrites the body on every
// call: the encoded bytes differ per attempt, the declared hash of the
// plaintext does not.
func hashedRecord(t *testing.T, plaintext, encoded, topic string) *streamlib.StreamRecord {
	t.Helper()
	return &streamlib.StreamRecord{
		Body: &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte("binary/encrypted")},
			Data:     []byte(encoded),
		},
		Topic: topic,
		Kind:  streampb.STREAM_RECORD_KIND_DATA,
		Metadata: map[string]*commonpb.Payload{
			ContentHashMetadataKey: declaredHash(t, plainBody(plaintext)),
		},
	}
}

func TestContentHashOfIsTheSHA256OfTheDeterministicPayload(t *testing.T) {
	body := plainBody("hello")
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(body)
	require.NoError(t, err)
	want := sha256.Sum256(data)
	got, err := ContentHashOf(body)
	require.NoError(t, err)
	require.Equal(t, want[:], got)
	require.Len(t, hex.EncodeToString(got), 64)
}

func TestDeclaredContentHashMakesAReencodedRetryARetry(t *testing.T) {
	s := newTestStream(t)
	for attempt := range 3 {
		result, err := s.AddMessages(nil, AddMessagesRequest{
			Records: []*streamlib.StreamRecord{
				hashedRecord(t, "hello", fmt.Sprint("nonce-", attempt, "-hello"), "t"),
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
		Records:    []*streamlib.StreamRecord{hashedRecord(t, "hello", "enc-1", "t")},
		ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)

	// Same encoded bytes, different plaintext: the declared hash is what
	// counts, so this is a conflict even though the bytes match.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Records:    []*streamlib.StreamRecord{hashedRecord(t, "goodbye", "enc-1", "t")},
		ProducerID: "p1", Sequence: 1,
	})
	require.Equal(t, ReasonProducerConflict, ReasonOf(err.Error()))

	// Same plaintext on another topic is a different write, not a retry.
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Records:    []*streamlib.StreamRecord{hashedRecord(t, "hello", "enc-1", "other")},
		ProducerID: "p1", Sequence: 1,
	})
	require.Equal(t, ReasonProducerConflict, ReasonOf(err.Error()))
	require.Equal(t, int64(1), s.State.HeadOffset)
}

func TestDeclaredContentHashCoversEveryRecordOfTheBatch(t *testing.T) {
	s := newTestStream(t)
	batch := func(second string) []*streamlib.StreamRecord {
		return []*streamlib.StreamRecord{
			hashedRecord(t, "one", "enc-a", "t"),
			hashedRecord(t, second, "enc-b", "t"),
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

// A batch may mix records that declare a hash with ones that do not, as a
// FINISH record does; the undeclared ones are identified by their bytes.
func TestDeclaredContentHashMixesWithUndeclaredRecords(t *testing.T) {
	s := newTestStream(t)
	mixed := func(encoded, plainBody string) []*streamlib.StreamRecord {
		return []*streamlib.StreamRecord{
			hashedRecord(t, "one", encoded, "t"),
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

// A value that is not a hex SHA-256 is what the key looks like once a codec
// has encoded it. The record falls back to its bytes rather than being
// refused, so a retry with the same bytes is still a retry.
func TestAnUnreadableContentHashFallsBackToTheBytes(t *testing.T) {
	s := newTestStream(t)
	record := func(body, value string) *streamlib.StreamRecord {
		return &streamlib.StreamRecord{
			Body: &commonpb.Payload{Data: []byte(body)},
			Kind: streampb.STREAM_RECORD_KIND_DATA,
			Metadata: map[string]*commonpb.Payload{
				ContentHashMetadataKey: {
					Metadata: map[string][]byte{"encoding": []byte("binary/encrypted")},
					Data:     []byte(value),
				},
			},
		}
	}
	first, err := s.AddMessages(nil, AddMessagesRequest{
		Records:    []*streamlib.StreamRecord{record("x", "ciphertext")},
		ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)
	require.False(t, first.Deduplicated)

	retry, err := s.AddMessages(nil, AddMessagesRequest{
		Records:    []*streamlib.StreamRecord{record("x", "ciphertext")},
		ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)
	require.True(t, retry.Deduplicated)

	_, err = s.AddMessages(nil, AddMessagesRequest{
		Records:    []*streamlib.StreamRecord{record("x", "other-ciphertext")},
		ProducerID: "p1", Sequence: 1,
	})
	require.Equal(t, ReasonProducerConflict, ReasonOf(err.Error()),
		"with the key unreadable, the bytes are the identity again")
}

// A workflow's own publish names no producer, so the key is never read on
// that path and an encoded value there costs nothing.
func TestTheKeyIsIgnoredWithoutAProducer(t *testing.T) {
	s := newTestStream(t)
	for i := range 2 {
		result, err := s.AddMessages(nil, AddMessagesRequest{
			Records: []*streamlib.StreamRecord{{
				Body: &commonpb.Payload{Data: []byte("x")},
				Kind: streampb.STREAM_RECORD_KIND_DATA,
				Metadata: map[string]*commonpb.Payload{
					ContentHashMetadataKey: {Data: []byte("not-hex")},
				},
			}},
		})
		require.NoError(t, err)
		require.False(t, result.Deduplicated)
		require.Equal(t, int64(i), result.FirstOffset)
	}
	require.Empty(t, s.State.Producers)
}

// Without the key the fingerprint is the deterministic serialization of the
// records, so producer entries written before the key existed stay comparable,
// and the stamp the stored batch carries plays no part in it.
func TestUndeclaredBatchIsFingerprintedOverItsRecords(t *testing.T) {
	records := msgs("a", "b")
	unstamped, err := marshalBatch(records, time.Time{})
	require.NoError(t, err)
	got, err := batchFingerprint(records)
	require.NoError(t, err)
	require.Equal(t, contentHash(unstamped.Data), got)

	stamped, err := marshalBatch(records, time.Unix(1_700_000_000, 0))
	require.NoError(t, err)
	require.NotEqual(t, unstamped.Data, stamped.Data, "the stamp is stored")
	again, err := batchFingerprint(records)
	require.NoError(t, err)
	require.Equal(t, got, again, "and ignored by the fingerprint")
}

func TestDeclaredHashIsCaseInsensitive(t *testing.T) {
	s := newTestStream(t)
	sum, err := ContentHashOf(plainBody("hello"))
	require.NoError(t, err)
	record := func(digest string) *streamlib.StreamRecord {
		return &streamlib.StreamRecord{
			Body: &commonpb.Payload{Data: []byte("enc")},
			Kind: streampb.STREAM_RECORD_KIND_DATA,
			Metadata: map[string]*commonpb.Payload{
				ContentHashMetadataKey: {Data: []byte(digest)},
			},
		}
	}
	lower := hex.EncodeToString(sum)
	upper := fmt.Sprintf("%X", sum)
	_, err = s.AddMessages(nil, AddMessagesRequest{
		Records: []*streamlib.StreamRecord{record(lower)}, ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)
	retry, err := s.AddMessages(nil, AddMessagesRequest{
		Records: []*streamlib.StreamRecord{record(upper)}, ProducerID: "p1", Sequence: 1,
	})
	require.NoError(t, err)
	require.True(t, retry.Deduplicated)
}
