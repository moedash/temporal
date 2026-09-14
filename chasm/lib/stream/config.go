package stream

import "time"

// DefaultMaxMessagesPerPoll bounds a read page when the caller does not.
const DefaultMaxMessagesPerPoll = 1000

// MaxMessagesPerBatch bounds one append. It is not only an admission limit: a
// node ID is the first offset of its batch, so to serve a read starting inside
// a batch the reader has to find the node that contains it. Bounding the batch
// bounds how far back it has to start, which turns an unbounded scan into a
// fixed overread.
const MaxMessagesPerBatch = 1000

// LongPollTimeout matches the convention used by the history long polls: on
// expiry the caller gets an empty response and polls again, rather than an
// error it would have to special-case.
const LongPollTimeout = 20 * time.Second

// LongPollBuffer leaves room to return an empty response before the caller's
// own deadline fires.
const LongPollBuffer = 3 * time.Second

// MaxConsumeItemsPerTask bounds one Workflow Task's slice. A byte cap alone is
// not enough: a burst of tiny messages stays under it while still making one
// task's drain arbitrarily long. Whichever bound binds first, the rest is
// delivered on the following task.
const MaxConsumeItemsPerTask = 1000

// MaxConsumeBytesPerTask bounds one Workflow Task's slice by size. Paired with
// MaxConsumeItemsPerTask because neither bound alone is enough: a burst of tiny
// messages slips under the byte budget, and a few large ones slip under the
// item count.
const MaxConsumeBytesPerTask = 2 << 20

// MaxProducersPerStream bounds the per-producer dedup table. The table is part
// of the component state written on every append, so a caller that sends a
// fresh producer id per request would grow the state until the mutable-state
// size limit rejects every further append, leaving the stream unwritable for
// good. The bound turns that into a clear error on the offending call.
const MaxProducersPerStream = 1000

// MaxConsumersPerStream bounds the registered consumer table for the same
// reason. Each consumer also holds a truncation floor, so an unbounded table
// would pin storage as well as grow state.
const MaxConsumersPerStream = 1000

// MaxListPageSize bounds a visibility page when the caller does not.
const MaxListPageSize = 1000

// MaxMessageBytes bounds one message. A message is never split, so this is also
// the smallest unit a reader can be asked to materialise.
const MaxMessageBytes = 1 << 20

// MaxBatchBytes bounds one append. It is deliberately equal to
// MaxConsumeBytesPerTask: a batch is written as one node and read back whole,
// so a batch larger than a task's byte budget could never be delivered.
const MaxBatchBytes = MaxConsumeBytesPerTask

// MaxOwnedStreamsPerWorkflow bounds how many named streams one execution can
// carry. Each is a component in the workflow's mutable state, so an unbounded
// count grows that state until the size limit terminates the execution. The
// name comes from the caller, and any caller in the namespace can pick a new
// one, which is what makes this reachable from outside.
const MaxOwnedStreamsPerWorkflow = 100

// MaxStreamNameLength bounds a name before it becomes a map key in mutable
// state, for the same reason.
const MaxStreamNameLength = 255
