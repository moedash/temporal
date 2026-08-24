package stream

import "time"

// defaultMaxMessagesPerPoll bounds a read page when the caller does not.
const defaultMaxMessagesPerPoll = 1000

// MaxMessagesPerBatch bounds one append. It is not only an admission limit: a
// node ID is the first offset of its batch, so to serve a read starting inside
// a batch the reader has to find the node that contains it. Bounding the batch
// bounds how far back it has to start, which turns an unbounded scan into a
// fixed overread.
const MaxMessagesPerBatch = 1000

// longPollTimeout matches the convention used by the history long polls: on
// expiry the caller gets an empty response and polls again, rather than an
// error it would have to special-case.
const longPollTimeout = 20 * time.Second

// longPollBuffer leaves room to return an empty response before the caller's
// own deadline fires.
const longPollBuffer = 3 * time.Second

// Tail-cache bounds. Sized for many modest streams rather than a few large
// ones, which is the shape this primitive targets.
const (
	tailCacheBytesPerStream = 1 << 20
	tailCacheMaxStreams     = 4096
)
