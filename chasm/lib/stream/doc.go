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
//   - STREAM_CLOSED: an append on a stream that has been sealed. Its records
//     stay readable; nothing more goes in.
//
// [Refusal] builds one and [ReasonOf] reads one back.
//
// # Content hash
//
// The producer table tells a genuine retry from a divergent repeat by
// comparing a fingerprint. By default the fingerprint is taken over the
// encoded batch, so a payload codec that encrypts with a fresh nonce on every
// call makes every retry look divergent. A producer avoids that by declaring
// each record's identity before the codec runs, in the record's metadata map
// under the key [ContentHashMetadataKey]. The value is a payload whose
// metadata "encoding" is "binary/plain" and whose data is 64 lowercase hex
// ASCII bytes: the SHA-256 over the deterministic proto serialization of the
// converted body payload, metadata and data, taken before any codec or
// external storage ([ContentHashOf] computes the same thing here). A FINISH
// record carries none.
//
// With the key present, the fingerprint is derived from the declared hash and
// the record's topic, kind, sequence, attempt and producer id, and never from
// the body or the other metadata payloads. A record without the key keeps the
// default, and so does a record whose value is not a hex SHA-256: that is
// what the key looks like after a codec has encoded it, and refusing the
// append would be worse than deduplicating it by its bytes.
//
// The key is read only on an append that names a producer, which is every
// outside append and never a workflow's own publish. A workflow's publish has
// no producer to repeat under, the task being its boundary, and the worker's
// codec runs over its record metadata payloads too, so the value may arrive
// encoded on that path. The server applies no codec anywhere.
//
// # Replay re-supply
//
// A cold replay is handed every range the consumer's completed tasks
// recorded, re-read from the streams, within one response's budget. A
// re-supply the budget cuts short is refused as a whole rather than marked:
// paging it needs a short flag on the poll response, or on the last
// StreamSlice, that this package does not have on the wire yet.
package stream
