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
//   - STREAM_POLICY_MISMATCH: a create naming a stream that already exists
//     with a different lifecycle. A create that repeats the existing
//     lifecycle is an idempotent retry and answers AlreadyExists instead.
//
// [Refusal] builds one and [ReasonOf] reads one back.
//
// # Capacity
//
// A standalone stream's creator sets its lifecycle: a record cap, a byte cap
// and a retention. The record cap is a rolling window, reclaiming the oldest
// batches as new ones land. The byte cap is a ceiling on held bytes: an
// append that would cross it is refused with the storage-limit
// ResourceExhausted a budgeted stream gives, and room comes back only as the
// record cap, an explicit truncation or the retention age reclaims behind the
// floor. Neither ever reclaims past an active workflow consumer's replay
// floor; the record cap refuses instead, and the byte cap simply stays full.
// DescribeStream reports the lifecycle and the held bytes.
//
// # Retention
//
// The lifecycle's retention is an age. On an open stream, a batch older than
// it is reclaimed behind the floor by a check the first append arms and that
// re-arms itself on stream.retentionRecheckInterval while the stream holds
// records, so a reader from BEGINNING sees the oldest record still young
// enough. The check never moves the floor past an active workflow consumer's
// replay floor; an aged batch a consumer still depends on waits until the
// consumer lets go. On a closed stream the same retention is the time to
// deletion, counted from the close, so a consumer can drain the tail first.
// Batches written before they carried a timestamp never age.
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
//
// # Reset
//
// A reset run keeps its subscriptions. A range recorded before the reset
// point is re-supplied from the run reset from; the range the reset-point task
// had been given is delivered again to the reset run's first task, from a
// stream of its own that begins with a copy of that input (see [Stream.Seed]).
// Outside consumers are not told that a reset happened: reporting it as a
// record needs a record shape on the wire, which this package does not have.
package stream
