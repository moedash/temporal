package stream

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

// DefaultMaxMessagesPerPoll bounds a read page when the caller does not.
const DefaultMaxMessagesPerPoll = 1000

// MaxMessagesPerBatch bounds one append. It is not only an admission limit: a
// batch is keyed by its first offset, so to serve a read starting inside a
// batch the reader has to find the batch that contains it. Bounding the batch
// bounds how far back it has to look, which turns an unbounded scan into a
// fixed overread.
const MaxMessagesPerBatch = 1000

// LongPollTimeout matches the convention used by the history long polls: on
// expiry the caller gets an empty response and polls again, rather than an
// error it would have to special-case.
const LongPollTimeout = 20 * time.Second

// LongPollBuffer leaves room to return an empty response before the caller's
// own deadline fires.
const LongPollBuffer = 3 * time.Second

// RoutedCallTimeout bounds a call to another shard made while a workflow's lock
// is held. The request's own deadline can be far longer, and a slow stream
// shard would otherwise stretch the lock hold to match it.
const RoutedCallTimeout = 5 * time.Second

// MaxListPageSize bounds a visibility page when the caller does not.
const MaxListPageSize = 1000

// MaxStreamNameLength bounds a name before it becomes a map key in mutable
// state. The name comes from the caller, and any caller in the namespace can
// pick a new one.
const MaxStreamNameLength = 255

// The limits below bound resource use and are namespace-scoped dynamic config,
// with these values as the defaults. They stay as constants too so component
// code driven without a config, as the unit tests do, has something to fall
// back on.
const (
	// MaxConsumeItemsPerTask bounds one Workflow Task's slice. A byte cap alone
	// is not enough: a burst of tiny messages stays under it while still making
	// one task's drain arbitrarily long. Whichever bound binds first, the rest
	// is delivered on the following task.
	MaxConsumeItemsPerTask = 1000

	// MaxConsumeBytesPerTask bounds one Workflow Task's slice by size. Paired
	// with MaxConsumeItemsPerTask because neither bound alone is enough: a
	// burst of tiny messages slips under the byte budget, and a few large ones
	// slip under the item count.
	MaxConsumeBytesPerTask = 2 << 20

	// MaxProducersPerStream bounds the per-producer dedup table. The table is
	// part of the component state written on every append, so a caller that
	// sends a fresh producer id per request would grow the state until the
	// mutable-state size limit rejects every further append, leaving the
	// stream unwritable for good. The bound turns that into a clear error on
	// the offending call.
	MaxProducersPerStream = 1000

	// MaxConsumersPerStream bounds the registered consumer table for the same
	// reason. Each consumer also holds a truncation floor, so an unbounded
	// table would pin storage as well as grow state.
	MaxConsumersPerStream = 1000

	// MaxMessageBytes bounds one message. A message is never split, so this is
	// also the smallest unit a reader can be asked to materialise.
	MaxMessageBytes = 1 << 20

	// MaxBatchBytes bounds one append. It is deliberately equal to
	// MaxConsumeBytesPerTask: a batch is written as one node and read back
	// whole, so a batch larger than a task's byte budget could never be
	// delivered.
	MaxBatchBytes = MaxConsumeBytesPerTask

	// MaxOwnedStreamsPerWorkflow bounds how many named streams one execution
	// can carry. Each is a component in the workflow's mutable state, so an
	// unbounded count grows that state until the size limit terminates the
	// execution, and any caller in the namespace can name a new one.
	MaxOwnedStreamsPerWorkflow = 100

	// OwnedStreamMaxBytes is the byte budget of a stream a workflow owns. Its
	// batches are the owning execution's mutable state, and the execution size
	// limit terminates the workflow rather than refusing an append, so the
	// stream refuses first. The sum over a workflow's streams is still bounded
	// by that limit.
	OwnedStreamMaxBytes = 2 << 20

	// OwnedStreamMaxItems is the item budget of a stream a workflow owns, for
	// the same reason: each batch is a node, and many small ones cost state
	// that the byte budget alone does not see.
	OwnedStreamMaxItems = 10_000
)

var (
	MaxConsumeItemsPerTaskSetting = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxConsumeItemsPerTask",
		MaxConsumeItemsPerTask,
		`Most stream messages one workflow task carries per subscription.`,
	)
	MaxConsumeBytesPerTaskSetting = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxConsumeBytesPerTask",
		MaxConsumeBytesPerTask,
		`Most stream message bytes one workflow task carries per subscription.`,
	)
	MaxProducersPerStreamSetting = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxProducersPerStream",
		MaxProducersPerStream,
		`Most producer ids a stream tracks for deduplication.`,
	)
	MaxConsumersPerStreamSetting = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxConsumersPerStream",
		MaxConsumersPerStream,
		`Most workflow consumers a stream registers.`,
	)
	MaxMessageBytesSetting = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxMessageBytes",
		MaxMessageBytes,
		`Largest single stream message accepted.`,
	)
	MaxBatchBytesSetting = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxBatchBytes",
		MaxBatchBytes,
		`Largest stream append accepted, summed over its messages.`,
	)
	MaxOwnedStreamsPerWorkflowSetting = dynamicconfig.NewNamespaceIntSetting(
		"stream.maxOwnedStreamsPerWorkflow",
		MaxOwnedStreamsPerWorkflow,
		`Most named streams one workflow execution can own.`,
	)
	OwnedStreamMaxBytesSetting = dynamicconfig.NewNamespaceIntSetting(
		"stream.ownedStreamMaxBytes",
		OwnedStreamMaxBytes,
		`Byte budget of a stream a workflow owns. Appends past it are refused. Keep it well
under limit.mutableStateSize.error, which would otherwise terminate the workflow.`,
	)
	OwnedStreamMaxItemsSetting = dynamicconfig.NewNamespaceIntSetting(
		"stream.ownedStreamMaxItems",
		OwnedStreamMaxItems,
		`Message budget of a stream a workflow owns. Appends past it are refused.`,
	)
	RetentionRecheckIntervalSetting = dynamicconfig.NewGlobalDurationSetting(
		"stream.retentionRecheckInterval",
		time.Minute,
		`How long a closed stream past its retention waits before asking again whether the
consumers holding it are still running.`,
	)
)

// Config holds the settings as live property functions.
type Config struct {
	// The id length limit shared with workflow ids. A stream id becomes an
	// execution's business id, and a stream name a key in mutable state.
	MaxIDLength                dynamicconfig.IntPropertyFn
	RetentionRecheckInterval   dynamicconfig.DurationPropertyFn
	MaxConsumeItemsPerTask     dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxConsumeBytesPerTask     dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxProducersPerStream      dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxConsumersPerStream      dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxMessageBytes            dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxBatchBytes              dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOwnedStreamsPerWorkflow dynamicconfig.IntPropertyFnWithNamespaceFilter
	OwnedStreamMaxBytes        dynamicconfig.IntPropertyFnWithNamespaceFilter
	OwnedStreamMaxItems        dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func NewConfig(dc *dynamicconfig.Collection) *Config {
	return &Config{
		MaxIDLength:                dynamicconfig.MaxIDLengthLimit.Get(dc),
		RetentionRecheckInterval:   RetentionRecheckIntervalSetting.Get(dc),
		MaxConsumeItemsPerTask:     MaxConsumeItemsPerTaskSetting.Get(dc),
		MaxConsumeBytesPerTask:     MaxConsumeBytesPerTaskSetting.Get(dc),
		MaxProducersPerStream:      MaxProducersPerStreamSetting.Get(dc),
		MaxConsumersPerStream:      MaxConsumersPerStreamSetting.Get(dc),
		MaxMessageBytes:            MaxMessageBytesSetting.Get(dc),
		MaxBatchBytes:              MaxBatchBytesSetting.Get(dc),
		MaxOwnedStreamsPerWorkflow: MaxOwnedStreamsPerWorkflowSetting.Get(dc),
		OwnedStreamMaxBytes:        OwnedStreamMaxBytesSetting.Get(dc),
		OwnedStreamMaxItems:        OwnedStreamMaxItemsSetting.Get(dc),
	}
}

// Limits is one namespace's resolved limits, read once per request so a
// transition sees one consistent set.
type Limits struct {
	MaxConsumeItemsPerTask     int
	MaxConsumeBytesPerTask     int
	MaxProducersPerStream      int
	MaxConsumersPerStream      int
	MaxMessageBytes            int
	MaxBatchBytes              int
	MaxOwnedStreamsPerWorkflow int
	OwnedStreamMaxBytes        int
	OwnedStreamMaxItems        int
}

// LimitsFor resolves the limits for a namespace. A nil Config, which is what
// code driven without a service gets, resolves to the defaults.
func (c *Config) LimitsFor(namespaceName string) Limits {
	if c == nil {
		return DefaultLimits()
	}
	return Limits{
		MaxConsumeItemsPerTask:     c.MaxConsumeItemsPerTask(namespaceName),
		MaxConsumeBytesPerTask:     c.MaxConsumeBytesPerTask(namespaceName),
		MaxProducersPerStream:      c.MaxProducersPerStream(namespaceName),
		MaxConsumersPerStream:      c.MaxConsumersPerStream(namespaceName),
		MaxMessageBytes:            c.MaxMessageBytes(namespaceName),
		MaxBatchBytes:              c.MaxBatchBytes(namespaceName),
		MaxOwnedStreamsPerWorkflow: c.MaxOwnedStreamsPerWorkflow(namespaceName),
		OwnedStreamMaxBytes:        c.OwnedStreamMaxBytes(namespaceName),
		OwnedStreamMaxItems:        c.OwnedStreamMaxItems(namespaceName),
	}.withDefaults()
}

// DefaultLimits is the constant set above.
func DefaultLimits() Limits {
	return Limits{}.withDefaults()
}

// withDefaults fills any limit left at zero, so a zero Limits value means the
// defaults rather than a stream that accepts nothing.
func (l Limits) withDefaults() Limits {
	fill := func(v *int, def int) {
		if *v <= 0 {
			*v = def
		}
	}
	fill(&l.MaxConsumeItemsPerTask, MaxConsumeItemsPerTask)
	fill(&l.MaxConsumeBytesPerTask, MaxConsumeBytesPerTask)
	fill(&l.MaxProducersPerStream, MaxProducersPerStream)
	fill(&l.MaxConsumersPerStream, MaxConsumersPerStream)
	fill(&l.MaxMessageBytes, MaxMessageBytes)
	fill(&l.MaxBatchBytes, MaxBatchBytes)
	fill(&l.MaxOwnedStreamsPerWorkflow, MaxOwnedStreamsPerWorkflow)
	fill(&l.OwnedStreamMaxBytes, OwnedStreamMaxBytes)
	fill(&l.OwnedStreamMaxItems, OwnedStreamMaxItems)
	return l
}
