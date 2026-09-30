// Package stream holds the CHASM stream component: a durable, offset-addressed
// append-only sequence with a producer table for retry deduplication and a
// consumer table for the floors workflow subscriptions pin.
//
// # Refusal reasons
//
// A refusal a client has to act on, rather than retry, is a FailedPrecondition
// whose message begins with one of the reason tokens below followed by ": ".
// The token is the contract; the rest of the message is for a human. A client
// maps the token to its typed error without matching on prose. The tokens are
// stable until a typed error detail carries them on the wire.
//
//   - STREAM_PRODUCER_CONFLICT: a repeat of a producer's most recent sequence
//     with different content. Nothing was written. The producer has a bug,
//     or two producers share an id.
//   - STREAM_PRODUCER_STALE_SEQUENCE: a sequence below the producer's most
//     recent one. Nothing was written. A producer id carries one append at a
//     time, so a retry of an earlier append after a later one committed
//     cannot be answered.
//   - STREAM_CURSOR_BELOW_FLOOR: a read or a subscription start below the
//     stream's retention floor. The message names the offset the stream now
//     starts at.
//
// [Refusal] builds one and [ReasonOf] reads one back.
//
// # Content hash
//
// The producer table tells a genuine retry from a divergent repeat by
// comparing a fingerprint. By default the fingerprint is taken over the
// encoded batch, so a payload codec that encrypts with a fresh nonce on every
// call makes every retry look divergent. A producer avoids that by declaring
// each record's identity before the codec runs: in the record's metadata map,
// under the key [ContentHashMetadataKey], a payload whose data is the
// lowercase hex SHA-256 of the converted body, computed before any codec or
// offload. The value is read as sent and must not itself be encoded.
//
// With the key present, the fingerprint is derived from the declared hash and
// the record's topic, kind, sequence, attempt and producer id, and never from
// the body or the other metadata payloads. A record without the key keeps the
// default. A malformed value is refused as an invalid argument, since a
// producer that sets the key means to be deduplicated by it.
//
// The metadata map is stored with the record and is separate from the body
// payload. The server applies no codec anywhere, so a codec on the client is
// the only thing that could hide the key, and it must leave the map alone.
package stream
