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
package stream
