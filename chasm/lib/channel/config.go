package channel

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

// MaxPositionBytes bounds the position a notification carries. It is a cursor
// for the listener to read from rather than data, and the channel retains it.
const MaxPositionBytes = 1024

// LongPollTimeout matches the other history long polls: on expiry the caller
// gets an empty response and polls again.
const LongPollTimeout = 20 * time.Second

// LongPollBuffer leaves room to answer before the caller's own deadline.
const LongPollBuffer = 3 * time.Second

// RoutedCallTimeout bounds one call to another shard, so a slow listener does
// not take the whole budget of the task that is telling the others.
const RoutedCallTimeout = 5 * time.Second

// The defaults below are also what component code falls back on when it is
// driven without a config, as the unit tests do.
const (
	DefaultMaxListeners                 = 1000
	DefaultRetainedNotifications        = 1000
	DefaultMaxMetadataBytes             = 2048
	DefaultRetention                    = time.Hour
	DefaultNotifyPerSecond              = 10000
	DefaultMaxSubscriptionsPerWorkflow  = 32
	DefaultLinkedRetainedNotifications  = 100
	DefaultMaxLinkedChannelsPerWorkflow = 32
)

var (
	MaxListenersSetting = dynamicconfig.NewNamespaceIntSetting(
		"channel.maxListeners",
		DefaultMaxListeners,
		`Most listeners, workflow and callback together, one notification channel holds. A
registration past it is refused.`,
	)
	RetainedNotificationsSetting = dynamicconfig.NewNamespaceIntSetting(
		"channel.retainedNotifications",
		DefaultRetainedNotifications,
		`Most notifications a channel keeps for pollers. The oldest goes when a new one
arrives past it.`,
	)
	MaxMetadataBytesSetting = dynamicconfig.NewNamespaceIntSetting(
		"channel.maxMetadataBytes",
		DefaultMaxMetadataBytes,
		`Largest notification metadata accepted, summed over its payloads.`,
	)
	RetentionSetting = dynamicconfig.NewNamespaceDurationSetting(
		"channel.retention",
		DefaultRetention,
		`How long a channel with no listeners stays after its last notify, registration or
poll before it is deleted with what it retained.`,
	)
	NotifyPerSecondSetting = dynamicconfig.NewNamespaceIntSetting(
		"channel.notifyPerSecond",
		DefaultNotifyPerSecond,
		`Most NotifyChannel calls a namespace may make per second on one history host. Zero
means no limit.`,
	)
	MaxSubscriptionsPerWorkflowSetting = dynamicconfig.NewNamespaceIntSetting(
		"channel.maxSubscriptionsPerWorkflow",
		DefaultMaxSubscriptionsPerWorkflow,
		`Most notification channels one workflow run subscribes to. A subscribe command past
it fails the workflow task.`,
	)
	LinkedRetainedNotificationsSetting = dynamicconfig.NewNamespaceIntSetting(
		"channel.linkedRetainedNotifications",
		DefaultLinkedRetainedNotifications,
		`Most notifications a channel linked to a workflow keeps for pollers. Held in the
workflow's own state, so smaller than an independent channel's ring.`,
	)
	MaxLinkedChannelsPerWorkflowSetting = dynamicconfig.NewNamespaceIntSetting(
		"channel.maxLinkedChannelsPerWorkflow",
		DefaultMaxLinkedChannelsPerWorkflow,
		`Most channels linked to one execution, a workflow run or a standalone activity. A
notify or a registration that would create one past it is refused with
ResourceExhausted.`,
	)
	// LinkedKindEnabledSetting exists so the independent kind can be exercised
	// live on a server that has the linked kind: a client that sends
	// execution is routed to the independent channel of that name, as before
	// the linked kind existed, and a probe lands on INDEPENDENT.
	LinkedKindEnabledSetting = dynamicconfig.NewNamespaceBoolSetting(
		"channel.linkedKindEnabled",
		true,
		`Whether the public channel calls honour execution and reach the channel linked to
that workflow or activity. Off, they ignore it and reach the independent channel of
the name, so DescribeChannel on an untouched linked name answers NotFound.`,
	)
)

// Limits are the per-namespace bounds a channel transition applies, resolved
// by the caller so the component needs no config of its own.
type Limits struct {
	MaxListeners          int
	RetainedNotifications int
	MaxMetadataBytes      int
	Retention             time.Duration
	// The linked kind's ring and how many linked channels one run may hold.
	LinkedRetainedNotifications  int
	MaxLinkedChannelsPerWorkflow int
}

// DefaultLimits is what applies when nothing is configured.
func DefaultLimits() Limits {
	return Limits{
		MaxListeners:                 DefaultMaxListeners,
		RetainedNotifications:        DefaultRetainedNotifications,
		MaxMetadataBytes:             DefaultMaxMetadataBytes,
		Retention:                    DefaultRetention,
		LinkedRetainedNotifications:  DefaultLinkedRetainedNotifications,
		MaxLinkedChannelsPerWorkflow: DefaultMaxLinkedChannelsPerWorkflow,
	}
}

// withDefaults fills what a caller left at zero.
func (l Limits) withDefaults() Limits {
	d := DefaultLimits()
	if l.MaxListeners <= 0 {
		l.MaxListeners = d.MaxListeners
	}
	if l.RetainedNotifications <= 0 {
		l.RetainedNotifications = d.RetainedNotifications
	}
	if l.MaxMetadataBytes <= 0 {
		l.MaxMetadataBytes = d.MaxMetadataBytes
	}
	if l.Retention <= 0 {
		l.Retention = d.Retention
	}
	if l.LinkedRetainedNotifications <= 0 {
		l.LinkedRetainedNotifications = d.LinkedRetainedNotifications
	}
	if l.MaxLinkedChannelsPerWorkflow <= 0 {
		l.MaxLinkedChannelsPerWorkflow = d.MaxLinkedChannelsPerWorkflow
	}
	return l
}

type Config struct {
	MaxListeners                 dynamicconfig.IntPropertyFnWithNamespaceFilter
	RetainedNotifications        dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxMetadataBytes             dynamicconfig.IntPropertyFnWithNamespaceFilter
	Retention                    dynamicconfig.DurationPropertyFnWithNamespaceFilter
	NotifyPerSecond              dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxSubscriptionsPerWorkflow  dynamicconfig.IntPropertyFnWithNamespaceFilter
	LinkedRetainedNotifications  dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxLinkedChannelsPerWorkflow dynamicconfig.IntPropertyFnWithNamespaceFilter
	LinkedKindEnabled            dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

func NewConfig(dc *dynamicconfig.Collection) *Config {
	return &Config{
		LinkedKindEnabled:            LinkedKindEnabledSetting.Get(dc),
		MaxListeners:                 MaxListenersSetting.Get(dc),
		RetainedNotifications:        RetainedNotificationsSetting.Get(dc),
		MaxMetadataBytes:             MaxMetadataBytesSetting.Get(dc),
		Retention:                    RetentionSetting.Get(dc),
		NotifyPerSecond:              NotifyPerSecondSetting.Get(dc),
		MaxSubscriptionsPerWorkflow:  MaxSubscriptionsPerWorkflowSetting.Get(dc),
		LinkedRetainedNotifications:  LinkedRetainedNotificationsSetting.Get(dc),
		MaxLinkedChannelsPerWorkflow: MaxLinkedChannelsPerWorkflowSetting.Get(dc),
	}
}

// LimitsFor resolves the namespace's limits.
func (c *Config) LimitsFor(namespaceName string) Limits {
	return Limits{
		MaxListeners:                 c.MaxListeners(namespaceName),
		RetainedNotifications:        c.RetainedNotifications(namespaceName),
		MaxMetadataBytes:             c.MaxMetadataBytes(namespaceName),
		Retention:                    c.Retention(namespaceName),
		LinkedRetainedNotifications:  c.LinkedRetainedNotifications(namespaceName),
		MaxLinkedChannelsPerWorkflow: c.MaxLinkedChannelsPerWorkflow(namespaceName),
	}
}
