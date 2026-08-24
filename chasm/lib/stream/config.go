package stream

// defaultMaxMessagesPerPoll bounds a read page when the caller does not.
const defaultMaxMessagesPerPoll = 1000

// MaxMessagesPerBatch bounds one append. It is not only an admission limit: a
// node ID is the first offset of its batch, so to serve a read starting inside
// a batch the reader has to find the node that contains it. Bounding the batch
// bounds how far back it has to start, which turns an unbounded scan into a
// fixed overread.
const MaxMessagesPerBatch = 1000
