package channel

import (
	"maps"
	"net/url"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	channelpb "go.temporal.io/server/chasm/lib/channel/gen/channelpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Channel is a named pub/sub point in a namespace, held as an execution of its
// own keyed by the channel name. A writer notifies the channel and never
// learns who listens. The channel keeps the listener table and wakes every
// listener: a workflow run through its next scheduled Workflow Task, a
// callback through an HTTP post. A bounded ring of recent notifications serves
// clients that poll.
type Channel struct {
	chasm.UnimplementedComponent

	State *channelpb.ChannelState

	// Keyed by sequence, oldest first. Nodes of their own, so a notify writes
	// one of them rather than rewriting the whole ring.
	Retained chasm.Map[int64, *channelpb.Notification]

	// Keyed by listener id. A workflow listens under its workflow id: one
	// workflow id has one open run, so a later run that subscribes takes the
	// entry of the run before it. A callback listens under its request id.
	// One key space, so a callback whose request id is the id of a workflow
	// listening here is refused.
	Listeners chasm.Map[string, *channelpb.Listener]
}

// readOnly hides the mutable half of a context. Reaching a data node through a
// mutable context marks it for persistence, and a notify must not rewrite
// every listener it only looked at.
func readOnly(ctx chasm.Context) chasm.Context {
	return struct{ chasm.Context }{ctx}
}

// NewChannel makes an empty channel. The transition that creates it is always
// a notify or a registration, which stamps the first activity.
func NewChannel(_ chasm.MutableContext) *Channel {
	return &Channel{
		State:     &channelpb.ChannelState{},
		Retained:  make(chasm.Map[int64, *channelpb.Notification]),
		Listeners: make(chasm.Map[string, *channelpb.Listener]),
	}
}

// ContextMetadata satisfies chasm.RootComponent. A channel has nothing to
// propagate to the request context.
func (c *Channel) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

// Terminate drops the listeners, so nothing is woken by a channel an operator
// shut down. What it retained stays readable until it is deleted.
func (c *Channel) Terminate(
	mctx chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	for id := range c.Listeners {
		delete(c.Listeners, id)
	}
	c.scheduleIdleCheck(mctx, DefaultLimits())
	return chasm.TerminateComponentResponse{}, nil
}

// LifecycleState reports a channel as running until the idle task closes it
// on the way to deleting it.
func (c *Channel) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	if c.State.GetClosed() {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

// ListenerCount is how many listeners the channel holds, of both kinds.
func (c *Channel) ListenerCount() int {
	return len(c.Listeners)
}

// The listener_kind metrics tag, derived from the callback variant.
const (
	ListenerKindWorkflow = "workflow"
	ListenerKindCallback = "callback"
)

// ListenerKind names what a callback reaches: a workflow run for an internal
// callback, an HTTP endpoint for a Nexus one.
func ListenerKind(cb *commonpb.Callback) string {
	if cb.GetInternal() != nil {
		return ListenerKindWorkflow
	}
	return ListenerKindCallback
}

// IsWorkflowListener reports whether the listener is a workflow run, reached
// through the routed delivery rather than an HTTP post.
func IsWorkflowListener(l *channelpb.Listener) bool {
	return l.GetCallback().GetInternal() != nil
}

// WorkflowTargetOf decodes the run an internal callback reaches. It reports
// false for an HTTP callback, and for data that does not parse, which the
// delivery then has no way to reach.
func WorkflowTargetOf(l *channelpb.Listener) (*channelpb.WorkflowTarget, bool) {
	internal := l.GetCallback().GetInternal()
	if internal == nil {
		return nil, false
	}
	target := &channelpb.WorkflowTarget{}
	if err := proto.Unmarshal(internal.GetData(), target); err != nil {
		return nil, false
	}
	return target, true
}

// setWorkflowTarget stores the run under the listener's internal callback.
func setWorkflowTarget(l *channelpb.Listener, target *channelpb.WorkflowTarget) error {
	data, err := proto.Marshal(target)
	if err != nil {
		return err
	}
	l.Callback = &commonpb.Callback{Variant: &commonpb.Callback_Internal_{
		Internal: &commonpb.Callback_Internal{Data: data},
	}}
	return nil
}

// CheckNotification refuses a notification the channel cannot accept. The
// frontend checks the same things first; this is what the component itself
// guarantees.
func CheckNotification(n *channelpb.Notification, maxMetadataBytes int) error {
	if n == nil {
		return serviceerror.NewInvalidArgument("notification is required")
	}
	if n.GetChannel() == "" {
		return serviceerror.NewInvalidArgument("channel is required")
	}
	if n.GetCounter() <= 0 {
		return serviceerror.NewInvalidArgument("notification counter must be greater than zero")
	}
	if len(n.GetPosition()) > MaxPositionBytes {
		return serviceerror.NewInvalidArgumentf(
			"notification position exceeds %d bytes", MaxPositionBytes)
	}
	if size := MetadataSize(n.GetMetadata()); maxMetadataBytes > 0 && size > maxMetadataBytes {
		return serviceerror.NewInvalidArgumentf(
			"notification metadata is %d bytes, more than the limit of %d", size, maxMetadataBytes)
	}
	return nil
}

// MetadataSize is the metered size of a notification's metadata: the keys and
// the payloads as the writer sent them.
func MetadataSize(metadata map[string]*commonpb.Payload) int {
	size := 0
	for key, p := range metadata {
		size += len(key) + p.Size()
	}
	return size
}

// NotifyResult is what a notify reports back to the writer.
type NotifyResult struct {
	ListenerCount int
	// The notification raised the channel's latest counter and was accepted.
	// One that did not changed nothing.
	Advanced bool
}

// Notify accepts a notification: it joins the retained ring for pollers and
// is fanned out to the listeners. One whose counter is not above the latest
// changes nothing, since every listener and poller already has a newer one;
// the caller checks that first so such a notify writes nothing at all.
//
// The fan-out is one task at a time. A notify that lands while one is
// outstanding schedules nothing: the task reads the latest when it runs, so a
// burst reaches each listener as its newest notification rather than as one
// delivery per notify.
func (c *Channel) Notify(
	mctx chasm.MutableContext,
	n *channelpb.Notification,
	limits Limits,
) (NotifyResult, error) {
	limits = limits.withDefaults()
	if err := CheckNotification(n, limits.MaxMetadataBytes); err != nil {
		return NotifyResult{}, err
	}
	if !c.Advances(n.GetCounter()) {
		return NotifyResult{ListenerCount: c.ListenerCount()}, nil
	}
	n = common.CloneProto(n)

	c.retain(mctx, n, limits.RetainedNotifications)
	c.State.AcceptedCount++
	c.touch(mctx)
	c.State.Latest = n
	c.scheduleFanOut(mctx)
	c.scheduleIdleCheck(mctx, limits)
	return NotifyResult{ListenerCount: c.ListenerCount(), Advanced: true}, nil
}

// retain appends to the ring and drops the oldest past the bound.
func (c *Channel) retain(mctx chasm.MutableContext, n *channelpb.Notification, bound int) {
	if c.Retained == nil {
		c.Retained = make(chasm.Map[int64, *channelpb.Notification])
	}
	seq := c.State.RetainedNext
	c.Retained[seq] = chasm.NewDataField(mctx, n)
	c.State.RetainedNext = seq + 1
	for c.State.RetainedNext-c.State.RetainedFirst > int64(bound) {
		delete(c.Retained, c.State.RetainedFirst)
		c.State.RetainedFirst++
	}
}

// Advances reports whether a notification at this counter would be news to
// the channel.
func (c *Channel) Advances(counter int64) bool {
	return counter > c.State.GetLatest().GetCounter()
}

// RetainedCount is how many notifications the ring holds.
func (c *Channel) RetainedCount() int64 {
	return c.State.GetRetainedNext() - c.State.GetRetainedFirst()
}

func (c *Channel) scheduleFanOut(mctx chasm.MutableContext) {
	if c.State.FanOutPending || c.ListenerCount() == 0 {
		return
	}
	c.State.FanOutPending = true
	mctx.AddTask(c, chasm.TaskAttributes{ScheduledTime: mctx.Now(c)}, &channelpb.ChannelFanOutTask{})
}

func (c *Channel) touch(mctx chasm.MutableContext) {
	c.State.LastActivityTime = timestamppb.New(mctx.Now(c))
}

// FanOut is what the fan-out task delivers and to whom.
type FanOut struct {
	Latest *channelpb.Notification
	// The workflow listeners, for the task to reach on their own shards.
	Workflows []*channelpb.WorkflowTarget
	// Callback listeners that already held the notification, in flight or
	// pending, or whose pending one it replaced.
	CallbackFolded int
	// Callback listeners a delivery was started for.
	CallbackStarted int
}

// TakeFanOut is the fan-out task's transition. It lowers the coalescing flag in
// the same transition that reads the latest, so a notify that commits after it
// schedules the next task and nothing is missed.
//
// Callback listeners are handed the latest here, since handing it over is a
// write on this execution: a listener with nothing in flight starts a delivery,
// and one that is busy keeps the latest as its pending notification, replacing
// any older one. The workflow listeners come back for the task to reach on
// their own shards.
func (c *Channel) TakeFanOut(mctx chasm.MutableContext, _ struct{}) (FanOut, error) {
	c.State.FanOutPending = false
	latest := c.State.GetLatest()
	out := FanOut{Latest: common.CloneProto(latest)}
	if latest == nil {
		return out, nil
	}

	started, folded, err := c.HandToCallbacks(mctx, latest)
	if err != nil {
		return FanOut{}, err
	}
	out.CallbackStarted, out.CallbackFolded = started, folded
	out.Workflows = c.WorkflowTargets(mctx)
	return out, nil
}

// callbackHolds reports whether a callback listener already holds a
// notification at this counter or above, in flight or pending. Only what it
// holds counts: once a post is done, the same counter is news again.
func callbackHolds(listener *channelpb.Listener, counter int64) bool {
	return (listener.GetInFlight() != nil && listener.GetInFlight().GetCounter() >= counter) ||
		(listener.GetPending() != nil && listener.GetPending().GetCounter() >= counter)
}

// CallbacksNeed reports whether any callback listener does not hold the
// notification, which is when handing it over writes the channel.
func (c *Channel) CallbacksNeed(ctx chasm.Context, counter int64) bool {
	view := readOnly(ctx)
	for _, field := range c.Listeners {
		listener := field.Get(view)
		if !IsWorkflowListener(listener) && !callbackHolds(listener, counter) {
			return true
		}
	}
	return false
}

// HandToCallbacks gives the notification to every callback listener that
// does not hold it: one with nothing in flight starts a post, and one that is
// busy keeps it as its pending notification, replacing an older one. A
// listener that holds it already is left untouched. It reports how many posts
// it started and how many listeners folded it.
func (c *Channel) HandToCallbacks(
	mctx chasm.MutableContext,
	n *channelpb.Notification,
) (started int, folded int, err error) {
	view := readOnly(mctx)
	for _, id := range slices.Sorted(maps.Keys(c.Listeners)) {
		current := c.Listeners[id].Get(view)
		if IsWorkflowListener(current) {
			continue
		}
		if callbackHolds(current, n.GetCounter()) {
			folded++
			continue
		}
		listener := c.Listeners[id].Get(mctx)
		listener.HandedCounter = max(listener.GetHandedCounter(), n.GetCounter())
		if listener.GetInFlight() != nil {
			if listener.GetPending() != nil {
				folded++
			}
			listener.Pending = common.CloneProto(n)
			continue
		}
		listener.InFlight = common.CloneProto(n)
		listener.Attempt = 0
		if err = c.scheduleCallback(mctx, listener); err != nil {
			return 0, 0, err
		}
		started++
	}
	return started, folded, nil
}

// WorkflowTargets returns the workflow listeners in workflow id order.
func (c *Channel) WorkflowTargets(ctx chasm.Context) []*channelpb.WorkflowTarget {
	view := readOnly(ctx)
	var out []*channelpb.WorkflowTarget
	for _, id := range slices.Sorted(maps.Keys(c.Listeners)) {
		if target, ok := WorkflowTargetOf(c.Listeners[id].Get(view)); ok {
			out = append(out, target)
		}
	}
	return out
}

// CallbackDestination keys a callback's deliveries on the outbound queue,
// where rate limits and the circuit breaker are per destination.
func CallbackDestination(cb *commonpb.Callback) (string, error) {
	nexus := cb.GetNexus()
	if nexus == nil {
		return "", serviceerror.NewInvalidArgument("a channel listener callback needs a URL")
	}
	u, err := url.Parse(nexus.GetUrl())
	if err != nil {
		return "", serviceerror.NewInvalidArgumentf("callback URL does not parse: %v", err)
	}
	return u.Scheme + "://" + u.Host, nil
}

func (c *Channel) scheduleCallback(
	mctx chasm.MutableContext,
	listener *channelpb.Listener,
) error {
	destination, err := CallbackDestination(listener.GetCallback())
	if err != nil {
		return err
	}
	listener.TaskSequence++
	mctx.AddTask(c, chasm.TaskAttributes{Destination: destination}, &channelpb.ChannelCallbackTask{
		ListenerId: listener.GetListenerId(),
		Sequence:   listener.GetTaskSequence(),
	})
	return nil
}

// WorkflowRegistration describes a workflow run that subscribed.
type WorkflowRegistration struct {
	NamespaceID         string
	WorkflowID          string
	RunID               string
	FirstExecutionRunID string
	Limits              Limits
}

// RegisterWorkflowListener records a run as a listener. A run of the same
// workflow already on record is replaced: one workflow id has one open run, so
// the entry belongs to a run that has closed.
//
// A run new to the channel gets the latest notification back, for it to take
// as pending. A reader that checked its source, found nothing and subscribed
// can otherwise miss a write that landed in between, since the notify that
// followed found nobody to tell.
func (c *Channel) RegisterWorkflowListener(
	mctx chasm.MutableContext,
	reg WorkflowRegistration,
) (*channelpb.Notification, error) {
	if reg.WorkflowID == "" || reg.RunID == "" {
		return nil, serviceerror.NewInvalidArgument("workflow id and run id are required")
	}
	limits := reg.Limits.withDefaults()
	c.touch(mctx)
	latest := common.CloneProto(c.State.GetLatest())
	if field, ok := c.Listeners[reg.WorkflowID]; ok {
		target, isWorkflow := WorkflowTargetOf(field.Get(readOnly(mctx)))
		if !isWorkflow {
			return nil, serviceerror.NewInvalidArgumentf(
				"listener id %q belongs to a callback on this channel", reg.WorkflowID)
		}
		if target.GetRunId() == reg.RunID {
			return nil, nil
		}
		target.RunId = reg.RunID
		target.FirstExecutionRunId = reg.FirstExecutionRunID
		listener := field.Get(mctx)
		if err := setWorkflowTarget(listener, target); err != nil {
			return nil, err
		}
		listener.RegisteredTime = timestamppb.New(mctx.Now(c))
		return latest, nil
	}
	if err := c.checkListenerRoom(limits.MaxListeners); err != nil {
		return nil, err
	}
	listener := &channelpb.Listener{
		ListenerId:     reg.WorkflowID,
		RegisteredTime: timestamppb.New(mctx.Now(c)),
	}
	err := setWorkflowTarget(listener, &channelpb.WorkflowTarget{
		NamespaceId:         reg.NamespaceID,
		WorkflowId:          reg.WorkflowID,
		RunId:               reg.RunID,
		FirstExecutionRunId: reg.FirstExecutionRunID,
	})
	if err != nil {
		return nil, err
	}
	if c.Listeners == nil {
		c.Listeners = make(chasm.Map[string, *channelpb.Listener])
	}
	c.Listeners[reg.WorkflowID] = chasm.NewDataField(mctx, listener)
	return latest, nil
}

func (c *Channel) checkListenerRoom(maxListeners int) error {
	if c.ListenerCount() < maxListeners {
		return nil
	}
	return serviceerror.NewResourceExhaustedf(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
		"channel already has %d listeners, which is the limit", maxListeners)
}

// RekeyWorkflowListener moves a listener to the run that continued the one on
// record. Left alone when the entry no longer names that run, because the
// successor may have registered itself in the meantime.
func (c *Channel) RekeyWorkflowListener(
	mctx chasm.MutableContext,
	workflowID, fromRunID, toRunID string,
) error {
	field, ok := c.Listeners[workflowID]
	if !ok {
		return nil
	}
	target, isWorkflow := WorkflowTargetOf(field.Get(readOnly(mctx)))
	if !isWorkflow || target.GetRunId() != fromRunID {
		return nil
	}
	target.RunId = toRunID
	return setWorkflowTarget(field.Get(mctx), target)
}

// ForgetWorkflowListener drops a listener no run of its workflow answers for
// any more, unless a newer run has registered under the same workflow id.
func (c *Channel) ForgetWorkflowListener(
	mctx chasm.MutableContext,
	workflowID, runID string,
	limits Limits,
) {
	field, ok := c.Listeners[workflowID]
	if !ok {
		return
	}
	target, isWorkflow := WorkflowTargetOf(field.Get(readOnly(mctx)))
	if !isWorkflow || target.GetRunId() != runID {
		return
	}
	delete(c.Listeners, workflowID)
	c.scheduleIdleCheck(mctx, limits.withDefaults())
}

// CallbackRegistration describes a callback being registered.
type CallbackRegistration struct {
	RequestID string
	Callback  *commonpb.Callback
	Limits    Limits
}

// RegisterCallbackListener adds a callback listener and returns its id, which
// is the request id. A registration whose request id the channel has seen
// returns the listener that request made, so a retry adds nothing.
//
// A new listener is posted the latest notification the channel holds, once,
// for the same reason a new workflow listener is handed it.
func (c *Channel) RegisterCallbackListener(
	mctx chasm.MutableContext,
	reg CallbackRegistration,
) (string, error) {
	if reg.RequestID == "" {
		return "", serviceerror.NewInvalidArgument("request id is required")
	}
	if _, err := CallbackDestination(reg.Callback); err != nil {
		return "", err
	}
	limits := reg.Limits.withDefaults()
	c.touch(mctx)

	id := reg.RequestID
	if field, ok := c.Listeners[id]; ok {
		if IsWorkflowListener(field.Get(readOnly(mctx))) {
			return "", serviceerror.NewInvalidArgumentf(
				"request id %q is the id of a workflow listening on this channel", id)
		}
		return id, nil
	}
	if err := c.checkListenerRoom(limits.MaxListeners); err != nil {
		return "", err
	}
	if c.Listeners == nil {
		c.Listeners = make(chasm.Map[string, *channelpb.Listener])
	}
	listener := &channelpb.Listener{
		ListenerId:     id,
		Callback:       common.CloneProto(reg.Callback),
		RequestId:      reg.RequestID,
		RegisteredTime: timestamppb.New(mctx.Now(c)),
	}
	if latest := c.State.GetLatest(); latest != nil {
		listener.InFlight = common.CloneProto(latest)
		listener.HandedCounter = latest.GetCounter()
		if err := c.scheduleCallback(mctx, listener); err != nil {
			return "", err
		}
	}
	c.Listeners[id] = chasm.NewDataField(mctx, listener)
	return id, nil
}

// UnregisterListener drops a callback listener. An id the channel does not
// hold is not an error, so a retried unregister succeeds. Workflow listeners
// are not dropped this way: the run believes it is subscribed, and only the
// run ending ends that.
func (c *Channel) UnregisterListener(
	mctx chasm.MutableContext,
	listenerID string,
	limits Limits,
) error {
	if listenerID == "" {
		return serviceerror.NewInvalidArgument("listener id is required")
	}
	if field, ok := c.Listeners[listenerID]; ok && IsWorkflowListener(field.Get(readOnly(mctx))) {
		return serviceerror.NewInvalidArgumentf(
			"listener %q is a workflow, which stops listening when its run ends", listenerID)
	}
	c.touch(mctx)
	delete(c.Listeners, listenerID)
	c.scheduleIdleCheck(mctx, limits.withDefaults())
	return nil
}

// CallbackDelivery is one attempt at posting a callback listener's in-flight
// notification.
type CallbackDelivery struct {
	Callback     *commonpb.Callback
	Notification *channelpb.Notification
	Attempt      int32
}

// CallbackDeliveryFor reads what the delivery task should post. It reports
// false when the listener is gone or the attempt the task was made for is
// over, so a stale task does nothing.
func (c *Channel) CallbackDeliveryFor(
	ctx chasm.Context,
	task *channelpb.ChannelCallbackTask,
) (CallbackDelivery, bool) {
	field, ok := c.Listeners[task.GetListenerId()]
	if !ok {
		return CallbackDelivery{}, false
	}
	listener := field.Get(readOnly(ctx))
	if IsWorkflowListener(listener) || listener.GetInFlight() == nil ||
		listener.GetTaskSequence() != task.GetSequence() || listener.GetNextAttemptTime() != nil {
		return CallbackDelivery{}, false
	}
	return CallbackDelivery{
		Callback:     common.CloneProto(listener.GetCallback()),
		Notification: common.CloneProto(listener.GetInFlight()),
		Attempt:      listener.GetAttempt(),
	}, true
}

// CallbackOutcome is how one delivery attempt ended.
type CallbackOutcome struct {
	ListenerID string
	// The task sequence the attempt ran under.
	Sequence int64
	// Nil on success.
	Err       error
	Retryable bool
	// Applied to a retryable failure.
	RetryPolicy backoff.RetryPolicy
}

// CompleteCallbackDelivery records an attempt. A success or a failure that
// will not succeed on retry ends the in-flight delivery, and the pending
// notification, if one arrived meanwhile, goes out next. A retryable failure
// backs off and tries the same notification again.
//
// It reports whether the attempt delivered.
func (c *Channel) CompleteCallbackDelivery(
	mctx chasm.MutableContext,
	outcome CallbackOutcome,
) (bool, error) {
	field, ok := c.Listeners[outcome.ListenerID]
	if !ok {
		return false, nil
	}
	view := field.Get(readOnly(mctx))
	if view.GetInFlight() == nil || view.GetTaskSequence() != outcome.Sequence ||
		view.GetNextAttemptTime() != nil {
		return false, nil
	}
	listener := field.Get(mctx)
	now := mctx.Now(c)

	if outcome.Err != nil && outcome.Retryable {
		listener.Attempt++
		delay := outcome.RetryPolicy.ComputeNextDelay(0, int(listener.Attempt), outcome.Err)
		next := now.Add(delay)
		listener.NextAttemptTime = timestamppb.New(next)
		listener.LastAttemptFailure = attemptFailure(outcome.Err, false)
		mctx.AddTask(c, chasm.TaskAttributes{ScheduledTime: next}, &channelpb.ChannelCallbackBackoffTask{
			ListenerId: listener.GetListenerId(),
			Sequence:   listener.GetTaskSequence(),
		})
		return false, nil
	}

	if outcome.Err != nil {
		listener.LastAttemptFailure = attemptFailure(outcome.Err, true)
	} else {
		listener.LastAttemptFailure = nil
	}
	listener.InFlight = listener.GetPending()
	listener.Pending = nil
	listener.Attempt = 0
	listener.NextAttemptTime = nil
	if listener.GetInFlight() != nil {
		if err := c.scheduleCallback(mctx, listener); err != nil {
			return false, err
		}
	}
	return outcome.Err == nil, nil
}

func attemptFailure(err error, nonRetryable bool) *failurepb.Failure {
	return &failurepb.Failure{
		Message: err.Error(),
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: nonRetryable},
		},
	}
}

// BackoffDone reports whether a backoff task still has an attempt to resume.
func (c *Channel) BackoffDone(ctx chasm.Context, task *channelpb.ChannelCallbackBackoffTask) bool {
	field, ok := c.Listeners[task.GetListenerId()]
	if !ok {
		return false
	}
	listener := field.Get(readOnly(ctx))
	return listener.GetInFlight() != nil && listener.GetTaskSequence() == task.GetSequence() &&
		listener.GetNextAttemptTime() != nil
}

// ResumeCallback schedules the next attempt once a backoff has run out.
func (c *Channel) ResumeCallback(
	mctx chasm.MutableContext,
	task *channelpb.ChannelCallbackBackoffTask,
) error {
	if !c.BackoffDone(mctx, task) {
		return nil
	}
	listener := c.Listeners[task.GetListenerId()].Get(mctx)
	listener.NextAttemptTime = nil
	return c.scheduleCallback(mctx, listener)
}

// PollRequest asks for the retained notifications above a counter.
type PollRequest struct {
	AfterCounter int64
	Max          int
}

// Poll returns the retained notifications with a counter above the request's,
// oldest first, at most Max of them.
func (c *Channel) Poll(ctx chasm.Context, req PollRequest) ([]*channelpb.Notification, error) {
	limit := req.Max
	if limit <= 0 || limit > DefaultRetainedNotifications {
		limit = DefaultRetainedNotifications
	}
	view := readOnly(ctx)
	var out []*channelpb.Notification
	for seq := c.State.GetRetainedFirst(); seq < c.State.GetRetainedNext(); seq++ {
		field, ok := c.Retained[seq]
		if !ok {
			continue
		}
		n := field.Get(view)
		if n.GetCounter() <= req.AfterCounter {
			continue
		}
		out = append(out, common.CloneProto(n))
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// LatestCounter is the highest counter the channel has accepted. It only
// grows, which is what makes it a condition a long poll can wait on.
func (c *Channel) LatestCounter() int64 {
	return c.State.GetLatest().GetCounter()
}

// Snapshot is a read of the channel for describe and for the poll path.
type Snapshot struct {
	Listeners        []*channelpb.ChannelListenerInfo
	Latest           *channelpb.Notification
	RetainedCount    int64
	LastActivityTime time.Time
}

// Describe lists the listeners, workflow listeners first, each kind in id
// order, and reports the latest notification and how many are retained.
func (c *Channel) Describe(ctx chasm.Context, _ struct{}) (Snapshot, error) {
	view := readOnly(ctx)
	out := Snapshot{
		Latest:           common.CloneProto(c.State.GetLatest()),
		RetainedCount:    c.RetainedCount(),
		LastActivityTime: c.State.GetLastActivityTime().AsTime(),
	}
	var workflows, callbacks []*channelpb.ChannelListenerInfo
	for _, id := range slices.Sorted(maps.Keys(c.Listeners)) {
		listener := c.Listeners[id].Get(view)
		info := &channelpb.ChannelListenerInfo{
			ListenerId:     id,
			RegisteredTime: common.CloneProto(listener.GetRegisteredTime()),
		}
		if target, ok := WorkflowTargetOf(listener); ok {
			info.Variant = &channelpb.ChannelListenerInfo_Workflow_{
				Workflow: &channelpb.ChannelListenerInfo_Workflow{
					WorkflowId: target.GetWorkflowId(),
					RunId:      target.GetRunId(),
				},
			}
			workflows = append(workflows, info)
			continue
		}
		info.Variant = &channelpb.ChannelListenerInfo_Callback{
			Callback: common.CloneProto(listener.GetCallback()),
		}
		callbacks = append(callbacks, info)
	}
	out.Listeners = append(workflows, callbacks...)
	return out, nil
}

// TouchForPoll records a poll as activity, so a channel only pollers use is
// not deleted under them. Skipped while the last activity is recent, since a
// poll is otherwise a read and this keeps it one on all but a few calls.
func (c *Channel) TouchForPoll(mctx chasm.MutableContext, limits Limits) (struct{}, error) {
	limits = limits.withDefaults()
	if mctx.Now(c).Sub(c.State.GetLastActivityTime().AsTime()) < limits.Retention/2 {
		return struct{}{}, nil
	}
	c.touch(mctx)
	return struct{}{}, nil
}

// PollNeedsTouch reports whether a poll should record itself as activity.
func (c *Channel) PollNeedsTouch(now time.Time, limits Limits) bool {
	limits = limits.withDefaults()
	return now.Sub(c.State.GetLastActivityTime().AsTime()) >= limits.Retention/2
}

// scheduleIdleCheck arms the deletion check for a channel with no listeners.
// One is outstanding at a time; it re-arms itself while the channel is in use.
func (c *Channel) scheduleIdleCheck(mctx chasm.MutableContext, limits Limits) {
	if c.State.IdleCheckPending || c.ListenerCount() > 0 {
		return
	}
	c.State.IdleCheckPending = true
	at := c.State.GetLastActivityTime().AsTime().Add(limits.Retention)
	mctx.AddTask(c, chasm.TaskAttributes{ScheduledTime: at}, &channelpb.ChannelIdleTask{})
}

// RunIdleCheck is the idle task's transition. It reports whether the channel
// has gone unused for a full retention with no listeners, which is when the
// task deletes it. Otherwise it re-arms for when that could next be true, or
// stands down while there are listeners, whose departure arms it again.
func (c *Channel) RunIdleCheck(mctx chasm.MutableContext, limits Limits) (bool, error) {
	limits = limits.withDefaults()
	c.State.IdleCheckPending = false
	if c.ListenerCount() > 0 {
		return false, nil
	}
	deadline := c.State.GetLastActivityTime().AsTime().Add(limits.Retention)
	if !mctx.Now(c).Before(deadline) {
		c.State.Closed = true
		return true, nil
	}
	c.scheduleIdleCheck(mctx, limits)
	return false, nil
}

// CheckChannelName refuses a channel name that cannot key an execution.
func CheckChannelName(name string, maxLength int) error {
	if name == "" {
		return serviceerror.NewInvalidArgument("channel is required")
	}
	if maxLength > 0 && len(name) > maxLength {
		return serviceerror.NewInvalidArgumentf(
			"channel name is %d bytes, more than the limit of %d", len(name), maxLength)
	}
	return nil
}
