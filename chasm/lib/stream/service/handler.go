package service

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"google.golang.org/protobuf/proto"
)

type handler struct {
	streamlib.UnimplementedStreamServiceServer

	shardController   shard.Controller
	namespaceRegistry namespace.Registry
	logger            log.Logger
	metricsHandler    metrics.Handler
	timeSource        clock.TimeSource
	config            *stream.Config
	limiters          *namespaceLimiters

	// Routes a call to the host that owns a shard. A step spanning two
	// executions cannot resolve both through the local controller, which
	// refuses a shard this host does not own, so the far half goes back out
	// through the service and lands wherever it belongs.
	routed streamlib.StreamServiceClient
}

func newHandler(
	shardController shard.Controller,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
	config *stream.Config,
	routed streamlib.StreamServiceClient,
) *handler {
	return &handler{
		shardController:   shardController,
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
		metricsHandler:    metricsHandler,
		timeSource:        timeSource,
		config:            config,
		limiters:          newNamespaceLimiters(config),
		routed:            routed,
	}
}

// namespaceName resolves an id for the limits, the rate limiters and the
// metrics tag. An id the registry cannot name resolves to the empty string;
// the interceptors have already refused requests for namespaces that do not
// exist.
func (h *handler) namespaceName(namespaceID string) string {
	name, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		return ""
	}
	return name.String()
}

// limitsFor resolves the namespace's limits. An unnamed namespace falls back
// to the defaults.
func (h *handler) limitsFor(namespaceID string) stream.Limits {
	name := h.namespaceName(namespaceID)
	if name == "" {
		return stream.DefaultLimits()
	}
	return h.config.LimitsFor(name)
}

// admitAppend applies the namespace's append rate to a batch before the
// transition that would store it.
func (h *handler) admitAppend(
	ns string, records []*streamlib.StreamRecord, limits stream.Limits,
) error {
	return h.limiters.allowAppend(
		ns, h.timeSource.Now(), len(records), recordsSize(records), limits)
}

// admitPoll applies the namespace's poll rate and counts the poll once it is
// through. A refused poll cost nothing and is not counted.
func (h *handler) admitPoll(ns string) error {
	if err := h.limiters.allowPoll(ns, h.timeSource.Now()); err != nil {
		return err
	}
	metrics.StreamPolls.With(h.metricsHandler).Record(1, metrics.NamespaceTag(ns))
	return nil
}

// meterAppend counts what an append stored. A deduplicated retry stored
// nothing, so the meter reads records kept rather than requests made.
func (h *handler) meterAppend(
	ns string, records []*streamlib.StreamRecord, result stream.AddMessagesResult,
) {
	if result.Deduplicated {
		return
	}
	tag := metrics.NamespaceTag(ns)
	metrics.StreamRecordsAppended.With(h.metricsHandler).Record(result.Count, tag)
	metrics.StreamBytesAppended.With(h.metricsHandler).Record(int64(recordsSize(records)), tag)
}

// meterDelivery counts what a poll handed back.
func (h *handler) meterDelivery(ns string, out *streamlib.PollMessagesOutput) {
	records := out.GetRecords()
	if len(records) == 0 {
		return
	}
	tag := metrics.NamespaceTag(ns)
	metrics.StreamRecordsDelivered.With(h.metricsHandler).Record(int64(len(records)), tag)
	metrics.StreamBytesDelivered.With(h.metricsHandler).Record(int64(recordsSize(records)), tag)
}

// recordsSize is the metered size of a batch: the records as the producer
// sent them, which is also what the component measures against its caps.
func recordsSize(records []*streamlib.StreamRecord) int {
	total := 0
	for _, record := range records {
		total += proto.Size(record)
	}
	return total
}

// withCallerInfo tags the context so the stream's direct persistence calls are
// attributed to the namespace that caused them. Without it they carry no caller
// name, which means they escape namespace rate limiting and priority as well as
// going uncounted in per-namespace metrics. The RPC path sets this via
// interceptors; calls made outside a request handler have to set it themselves.
func (h *handler) withCallerInfo(ctx context.Context, namespaceID string) context.Context {
	name, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		return ctx
	}
	return headers.SetCallerInfo(ctx, headers.NewCallerInfo(
		name.String(), headers.CallerTypeAPI, ""))
}

// refFor builds a reference to a stream. A supplied run ID lets the engine skip
// resolving the current run, which is otherwise a persistence lookup on every
// call and dominates the cost of an otherwise cheap read.
func refFor(namespaceID, streamID string) chasm.ComponentRef {
	return refForRun(namespaceID, streamID, "")
}

func refForRun(namespaceID, streamID, runID string) chasm.ComponentRef {
	return chasm.NewComponentRef[*stream.Stream](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  streamID,
		RunID:       runID,
	})
}

// workflowRef builds a reference to the execution that owns an attached
// stream. An attached stream is a subcomponent, so it has no id of its own and
// everything about it is reached through its owner.
//
// An empty runID means the current run. A caller that supplies one is pinning:
// an owned stream does not carry across continue-as-new, so the successor's
// stream of the same name is a different, empty one, and a caller that meant
// the predecessor would otherwise be redirected to it without being told.
func workflowRef(namespaceID, workflowID, runID string) chasm.ComponentRef {
	return chasm.NewComponentRef[*chasmworkflow.Workflow](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  workflowID,
		RunID:       runID,
	})
}

// activityRef builds a reference to a standalone activity, which is an
// execution of its own and so owns its streams the way a workflow does.
func activityRef(namespaceID, activityID, runID string) chasm.ComponentRef {
	return chasm.NewComponentRef[*activity.Activity](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  activityID,
		RunID:       runID,
	})
}

// ownedStreamName resolves a name the caller left empty the same way a publish
// command does, so a reader addresses the default stream by omission just as a
// writer creates it by omission.
func ownedStreamName(name string) string {
	if name == "" {
		return stream.DefaultStreamName
	}
	return name
}

// ownedTarget is an owned stream resolved from the caller's reference: the
// execution that holds it and the key it is held under there.
type ownedTarget struct {
	ref chasm.ComponentRef
	key string
}

// resolveOwned turns an owner and a stream name into where the stream lives.
//
// A workflow's activity is not an execution of its own, so its streams are held
// in the workflow's map under a reserved key and reached through the workflow.
// A workflow may not name that part of its map itself, or it could write into
// a stream its activity owns.
func resolveOwned(
	namespaceID string,
	owner *streamlib.StreamOwner,
	name string,
) (ownedTarget, error) {
	if err := checkOwner(owner); err != nil {
		return ownedTarget{}, err
	}
	name = ownedStreamName(name)
	switch owner.GetKind() {
	case streamlib.STREAM_OWNER_KIND_WORKFLOW:
		if err := chasmworkflow.CheckWorkflowStreamName(name); err != nil {
			return ownedTarget{}, err
		}
		return ownedTarget{
			ref: workflowRef(namespaceID, owner.GetId(), owner.GetRunId()),
			key: name,
		}, nil
	case streamlib.STREAM_OWNER_KIND_ACTIVITY:
		if err := stream.CheckStreamName(name); err != nil {
			return ownedTarget{}, err
		}
		return ownedTarget{
			ref: activityRef(namespaceID, owner.GetId(), owner.GetRunId()),
			key: name,
		}, nil
	case streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY:
		if err := stream.CheckStreamName(name); err != nil {
			return ownedTarget{}, err
		}
		return ownedTarget{
			ref: workflowRef(namespaceID, owner.GetId(), owner.GetRunId()),
			key: chasmworkflow.ActivityStreamKey(owner.GetActivityId(), name),
		}, nil
	default:
		return ownedTarget{}, serviceerror.NewInvalidArgument("owner kind is required")
	}
}

// checkOwner refuses an owner reference that names no execution, or names one
// in a way its kind does not allow.
func checkOwner(owner *streamlib.StreamOwner) error {
	if owner.GetId() == "" {
		return serviceerror.NewInvalidArgument("owner id is required")
	}
	switch owner.GetKind() {
	case streamlib.STREAM_OWNER_KIND_WORKFLOW, streamlib.STREAM_OWNER_KIND_ACTIVITY:
		if owner.GetActivityId() != "" {
			return serviceerror.NewInvalidArgumentf(
				"owner activity id is only for a workflow's activity, not for kind %v",
				owner.GetKind())
		}
	case streamlib.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY:
		if owner.GetActivityId() == "" {
			return serviceerror.NewInvalidArgument(
				"owner activity id is required for a workflow's activity")
		}
	default:
		return serviceerror.NewInvalidArgument("owner kind is required")
	}
	return nil
}

func (h *handler) CreateStream(
	ctx context.Context,
	req *streamlib.CreateStreamRequest,
) (*streamlib.CreateStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	if in.GetStreamId() == "" {
		return nil, serviceerror.NewInvalidArgument("stream id is required")
	}

	result, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: in.GetStreamId()},
		func(mctx chasm.MutableContext, input *streamlib.CreateStreamInput) (*stream.Stream, error) {
			return stream.NewStream(mctx, stream.NewStreamRequest{Lifecycle: input.GetLifecycle()})
		},
		in,
	)
	if err != nil {
		if started, ok := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); ok {
			return nil, h.existingStreamRefusal(ctx, req.GetNamespaceId(), in, started.CurrentRunID)
		}
		return nil, err
	}
	return &streamlib.CreateStreamResponse{
		FrontendResponse: &streamlib.CreateStreamOutput{RunId: result.ExecutionKey.RunID},
	}, nil
}

// existingStreamRefusal answers a create that names a stream already there.
// The engine's own error is not a service error and reaches the caller as
// Unknown, so it is read here and answered in two ways a client can tell
// apart without a describe: a repeat of the existing lifecycle is an
// idempotent retry and says AlreadyExists; a different lifecycle is a change
// the caller has to make on purpose, under a new id, and is refused with its
// own reason.
func (h *handler) existingStreamRefusal(
	ctx context.Context,
	namespaceID string,
	in *streamlib.CreateStreamInput,
	runID string,
) error {
	state, err := chasm.ReadComponent(ctx,
		refForRun(namespaceID, in.GetStreamId(), runID), (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return err
	}
	existing := state.GetLifecycle()
	if proto.Equal(existing, in.GetLifecycle()) {
		return serviceerror.NewAlreadyExistsf(
			"stream %q already exists with this lifecycle", in.GetStreamId())
	}
	return stream.Refusal(stream.ReasonPolicyMismatch,
		"stream %q already exists with a different lifecycle: retention %v, max items %d, "+
			"max bytes %d",
		in.GetStreamId(), existing.GetRetention().AsDuration(), existing.GetMaxItems(),
		existing.GetMaxBytes())
}

func (h *handler) AddMessages(
	ctx context.Context,
	req *streamlib.AddMessagesRequest,
) (*streamlib.AddMessagesResponse, error) {
	in := req.GetFrontendRequest()
	if len(in.GetRecords()) == 0 {
		return nil, serviceerror.NewInvalidArgument("no records to append")
	}
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	ns := h.namespaceName(req.GetNamespaceId())
	limits := h.limitsFor(req.GetNamespaceId())
	if err := h.admitAppend(ns, in.GetRecords(), limits); err != nil {
		return nil, err
	}

	// The batch and the frontier commit in one transition, and the execution
	// serializes transitions, so a producer that names no expected offset takes
	// whatever offset it lands at. A retried sequence is answered by the
	// producer table, not by a pin on the head.
	addReq := stream.AddMessagesRequest{
		Records:    in.GetRecords(),
		ProducerID: in.GetProducerId(),
		Sequence:   in.GetSequence(),
		Limits:     limits,
	}
	if in.GetUseExpectedOffset() {
		expected := in.GetExpectedOffset()
		addReq.ExpectedOffset = &expected
	}

	result, _, err := chasm.UpdateComponent(ctx,
		refForRun(req.GetNamespaceId(), in.GetStreamId(), in.GetRunId()),
		func(
			s *stream.Stream, mctx chasm.MutableContext, r stream.AddMessagesRequest,
		) (stream.AddMessagesResult, error) {
			out, err := s.AddMessages(mctx, r)
			if err == nil && !out.Deduplicated {
				s.ScheduleChannelNotify(mctx,
					stream.ChannelName(in.GetStreamId()), in.GetStreamId(), limits)
			}
			return out, err
		}, addReq)
	if err != nil {
		return nil, err
	}
	h.meterAppend(ns, in.GetRecords(), result)

	return &streamlib.AddMessagesResponse{
		FrontendResponse: &streamlib.AddMessagesOutput{
			FirstOffset:  result.FirstOffset,
			NextOffset:   result.NextOffset,
			Count:        result.Count,
			Deduplicated: result.Deduplicated,
		},
	}, nil
}

// AddWorkflowMessages appends to a stream an execution owns, from outside that
// execution. The owner is a workflow, a standalone activity, or an activity a
// workflow scheduled.
//
// The workflow's own publishes ride its Workflow Task and cost no transition of
// their own. This producer is off-shard, so it pays one transition on the
// owning execution per batch, and batching is what keeps that cheap. It is the
// path a model activity streaming tokens takes, where the workflow is only
// bracketing what the activity produces.
//
// One transition does everything: the stream is created on first write, and
// the workflow's own publishes are serialized against this append by the
// execution, so neither producer needs to pin the head against the other.
func (h *handler) AddWorkflowMessages(
	ctx context.Context,
	req *streamlib.AddWorkflowMessagesRequest,
) (*streamlib.AddWorkflowMessagesResponse, error) {
	in := req.GetFrontendRequest()
	if len(in.GetRecords()) == 0 {
		return nil, serviceerror.NewInvalidArgument("no records to append")
	}
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	target, err := resolveOwned(req.GetNamespaceId(), in.GetOwner(), in.GetStreamName())
	if err != nil {
		return nil, err
	}
	ns := h.namespaceName(req.GetNamespaceId())
	limits := h.limitsFor(req.GetNamespaceId())
	if err := h.admitAppend(ns, in.GetRecords(), limits); err != nil {
		return nil, err
	}

	// The records go in as sent, producer identity included. Who is writing is
	// the caller's claim to make; the store only answers whether it fits.
	addReq := stream.AddMessagesRequest{
		Records:    in.GetRecords(),
		ProducerID: in.GetProducerId(),
		Sequence:   in.GetSequence(),
		Limits:     limits,
	}
	result, _, err := chasm.UpdateComponent(ctx, target.ref,
		func(
			owner stream.Owner, mctx chasm.MutableContext, r stream.AddMessagesRequest,
		) (stream.AddMessagesResult, error) {
			return owner.AppendToOwnedStream(mctx, target.key, r)
		}, addReq)
	if err != nil {
		return nil, err
	}
	h.meterAppend(ns, in.GetRecords(), result)

	return &streamlib.AddWorkflowMessagesResponse{
		FrontendResponse: &streamlib.AddMessagesOutput{
			FirstOffset:  result.FirstOffset,
			NextOffset:   result.NextOffset,
			Count:        result.Count,
			Deduplicated: result.Deduplicated,
		},
	}, nil
}

func (h *handler) FinishWriting(
	ctx context.Context,
	req *streamlib.FinishWritingRequest,
) (*streamlib.FinishWritingResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	_, _, err := chasm.UpdateComponent(
		ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()),
		func(s *stream.Stream, mctx chasm.MutableContext, producerID string) (struct{}, error) {
			return struct{}{}, s.FinishWriting(mctx, producerID)
		},
		in.GetProducerId(),
	)
	if err != nil {
		return nil, err
	}
	return &streamlib.FinishWritingResponse{FrontendResponse: &streamlib.FinishWritingOutput{}}, nil
}

// SubscribeWorkflow registers a workflow as a consumer of a stream it owns.
//
// The cursor is written into the workflow's own state, not the stream's, which
// is what lets every later advance commit with the event that records it. Only
// a stream the workflow owns can be subscribed here: reaching one in another
// execution needs that stream's frontier, and reading it from inside the
// consuming workflow's transaction is a separate problem.
func (h *handler) SubscribeWorkflow(
	ctx context.Context,
	req *streamlib.SubscribeWorkflowRequest,
) (*streamlib.SubscribeWorkflowResponse, error) {
	in := req.GetFrontendRequest()
	start, err := stream.RequestedStart(in.GetStartPosition(), "start_offset", in.GetStartOffset())
	if err != nil {
		return nil, err
	}

	if in.GetStreamId() != "" {
		if in.GetStreamName() != "" {
			return nil, serviceerror.NewInvalidArgument(
				"set either stream id, for a standalone stream, or stream name, for one the " +
					"workflow owns, not both")
		}
		return h.subscribeToExternalStream(ctx, req.GetNamespaceId(), in, start)
	}

	limits := h.limitsFor(req.GetNamespaceId())
	startOffset, _, err := chasm.UpdateComponent(
		ctx,
		workflowRef(req.GetNamespaceId(), in.GetWorkflowId(), in.GetOwnerRunId()),
		func(
			wf *chasmworkflow.Workflow, mctx chasm.MutableContext, input *streamlib.SubscribeWorkflowInput,
		) (int64, error) {
			return wf.SubscribeToOwnedStream(mctx, ownedStreamName(input.GetStreamName()), start, limits)
		},
		in,
	)
	if err != nil {
		return nil, err
	}

	return &streamlib.SubscribeWorkflowResponse{
		FrontendResponse: &streamlib.SubscribeWorkflowOutput{StartOffset: startOffset},
	}, nil
}

// subscribeToExternalStream registers a cursor against a stream in another
// execution.
//
// The pin goes on the stream before the cursor goes on the workflow, and the
// order is the guarantee: interrupted after the first write there is a pin
// holding storage nothing reads, which costs space. Interrupted after the
// other order there would be a cursor with no pin, and truncation would be free
// to take a range that cursor still points at.
func (h *handler) subscribeToExternalStream(
	ctx context.Context,
	namespaceID string,
	in *streamlib.SubscribeWorkflowInput,
	start *streampb.StreamStartPosition,
) (*streamlib.SubscribeWorkflowResponse, error) {
	// The pin is keyed by the consuming run, so the run is resolved first. It
	// also pins the cursor write below to that run, so a run that ends between
	// the two steps cannot leave the pin on one run and the cursor on another.
	consumerRunID, err := chasm.ReadComponent(ctx,
		workflowRef(namespaceID, in.GetWorkflowId(), in.GetOwnerRunId()),
		func(_ *chasmworkflow.Workflow, cctx chasm.Context, _ struct{}) (string, error) {
			return cctx.ExecutionKey().RunID, nil
		}, struct{}{})
	if err != nil {
		return nil, err
	}

	// The stream half goes out and comes back on the shard that owns it. This
	// handler was routed to the consuming workflow, so the stream may well be
	// somewhere else, and resolving it here would fail on any cluster with more
	// than one history host.
	//
	// The pin still lands before the cursor, which is the guarantee: interrupted
	// between them there is a pin holding storage nothing reads, which costs
	// space, where the other order would leave a cursor with no pin and let
	// truncation take a range it still points at.
	registered, err := h.routed.RegisterStreamConsumer(ctx, &streamlib.RegisterStreamConsumerRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &streamlib.RegisterStreamConsumerInput{
			Namespace:          in.GetNamespace(),
			StreamId:           in.GetStreamId(),
			ConsumerWorkflowId: in.GetWorkflowId(),
			ConsumerRunId:      consumerRunID,
			StartPosition:      start,
		},
	})
	if err != nil {
		return nil, err
	}
	pin := registered.GetFrontendResponse()
	if pin.GetStreamAbsent() {
		// The caller named a standalone stream id, so there is nothing to fall
		// back to: an id that names nothing is the caller's mistake here.
		return nil, serviceerror.NewNotFoundf(
			"no stream with id %q in this namespace", in.GetStreamId())
	}

	startOffset, _, err := chasm.UpdateComponent(
		ctx,
		workflowRef(namespaceID, in.GetWorkflowId(), consumerRunID),
		func(wf *chasmworkflow.Workflow, mctx chasm.MutableContext, offset int64) (int64, error) {
			return wf.SubscribeToExternalStream(mctx, chasmworkflow.ExternalStreamSubscription{
				StreamID:    in.GetStreamId(),
				StartOffset: offset,
				KnownHead:   pin.GetKnownHead(),
			})
		},
		pin.GetStartOffset(),
	)
	if err != nil {
		return nil, err
	}

	return &streamlib.SubscribeWorkflowResponse{
		FrontendResponse: &streamlib.SubscribeWorkflowOutput{StartOffset: startOffset},
	}, nil
}

// RegisterStreamConsumer takes the pin, on the shard that owns the stream.
//
// Internal. Called by SubscribeWorkflow and by the workflow task completion
// path, both of which run on the consumer's shard and so cannot reach the
// stream themselves. The start offset is resolved inside the transition that
// records it, against the same frontier it hands back, so the consumer records
// facts rather than readings.
func (h *handler) RegisterStreamConsumer(
	ctx context.Context,
	req *streamlib.RegisterStreamConsumerRequest,
) (*streamlib.RegisterStreamConsumerResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	start, err := stream.RequestedStart(in.GetStartPosition(), "start_offset", in.GetStartOffset())
	if err != nil {
		return nil, err
	}

	limits := h.limitsFor(req.GetNamespaceId())
	pin, _, err := chasm.UpdateComponent(
		ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()),
		func(
			s *stream.Stream, mctx chasm.MutableContext, start *streampb.StreamStartPosition,
		) (*streamlib.RegisterStreamConsumerOutput, error) {
			startOffset, err := s.RegisterConsumer(mctx, stream.ConsumerRegistration{
				ConsumerID:   externalConsumerID(in.GetConsumerWorkflowId(), in.GetConsumerRunId()),
				WorkflowID:   in.GetConsumerWorkflowId(),
				RunID:        in.GetConsumerRunId(),
				Start:        start,
				External:     true,
				MaxConsumers: limits.MaxConsumersPerStream,
			})
			if err != nil {
				return nil, err
			}
			return &streamlib.RegisterStreamConsumerOutput{
				StartOffset: startOffset,
				KnownHead:   s.State.GetHeadOffset(),
			}, nil
		},
		start,
	)
	if err != nil {
		// Only the absence of the execution itself is turned into an answer.
		// The caller treats that as "the id names no standalone stream" and
		// binds to a stream of its own instead, which would be the wrong thing
		// to do about a registry miss or a shard that has moved.
		if executionAbsent(err) {
			return &streamlib.RegisterStreamConsumerResponse{
				FrontendResponse: &streamlib.RegisterStreamConsumerOutput{StreamAbsent: true},
			}, nil
		}
		return nil, err
	}
	return &streamlib.RegisterStreamConsumerResponse{FrontendResponse: pin}, nil
}

// executionAbsent reports whether an error means no execution holds the stream.
//
// This runs on the shard that owns the stream, after routing, so a NotFound
// here is the engine's answer about the execution. A namespace the registry
// cannot name and a shard that has moved have error types of their own and do
// not reach it.
func executionAbsent(err error) bool {
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}

// externalConsumerID names a workflow run's pin on a stream in another
// execution. Keyed by run, so a later run of the same workflow id registers
// fresh instead of inheriting a closed run's floor.
func externalConsumerID(workflowID, runID string) string {
	return "workflow:" + workflowID + "/" + runID
}

// consumerProbe is what one run says about a subscription: whether the run is
// still open, whether it holds a cursor for the stream, and where that cursor
// began, which is the floor a re-keyed pin has to hold.
type consumerProbe struct {
	runID       string
	closed      bool
	consumes    bool
	startOffset int64
}

// probeConsumer reads a run without touching it. A run that is gone reads as
// closed, since the notify task only needs to know whether pushing at it can
// achieve anything.
func (h *handler) probeConsumer(
	ctx context.Context,
	namespaceID, workflowID, runID, streamID string,
) (consumerProbe, error) {
	probe, err := chasm.ReadComponent(ctx, workflowRef(namespaceID, workflowID, runID),
		func(wf *chasmworkflow.Workflow, cctx chasm.Context, _ struct{}) (consumerProbe, error) {
			p := consumerProbe{
				runID:  cctx.ExecutionKey().RunID,
				closed: !cctx.ExecutionInfo().CloseTime.IsZero(),
			}
			// Only an external cursor counts. A cursor of the same key on a
			// stream the workflow owns is a different stream that happens to
			// share the name, and answering for it would keep a pin alive on
			// a stream nothing reads.
			if field, ok := wf.StreamCursors[streamID]; ok && field.Get(cctx).IsExternal() {
				p.consumes = true
				p.startOffset = field.Get(cctx).StartOffset()
			}
			return p, nil
		}, struct{}{})
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return consumerProbe{runID: runID, closed: true}, nil
	}
	return probe, err
}

func (h *handler) pushHead(
	ctx context.Context,
	namespaceID, workflowID, runID, streamID string,
	head int64,
) error {
	_, _, err := chasm.UpdateComponent(
		ctx,
		workflowRef(namespaceID, workflowID, runID),
		func(wf *chasmworkflow.Workflow, mctx chasm.MutableContext, at int64) (struct{}, error) {
			return struct{}{}, wf.AdvanceKnownHead(mctx, streamID, at)
		},
		head,
	)
	return err
}

// AdvanceConsumerHead tells one consumer that the frontier moved, on the shard
// that owns that consumer.
//
// Internal. Called by the notify task, which runs on the stream's shard and so
// cannot reach a consumer living anywhere else. The task pins a run, and a run
// ends: the answer then says whether a successor carries the subscription, so
// the stream re-keys its pin, or nothing does, so the stream releases it.
func (h *handler) AdvanceConsumerHead(
	ctx context.Context,
	req *streamlib.AdvanceConsumerHeadRequest,
) (*streamlib.AdvanceConsumerHeadResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	namespaceID, workflowID, streamID := req.GetNamespaceId(), in.GetWorkflowId(), in.GetStreamId()

	pinned, err := h.probeConsumer(ctx, namespaceID, workflowID, in.GetOwnerRunId(), streamID)
	if err != nil {
		return nil, err
	}
	out := &streamlib.AdvanceConsumerHeadOutput{}
	if !pinned.closed {
		if !pinned.consumes {
			out.ConsumerClosed = true
			return &streamlib.AdvanceConsumerHeadResponse{FrontendResponse: out}, nil
		}
		err := h.pushHead(ctx, namespaceID, workflowID, pinned.runID, streamID, in.GetHeadOffset())
		if err != nil {
			return nil, err
		}
		return &streamlib.AdvanceConsumerHeadResponse{FrontendResponse: out}, nil
	}

	// The pinned run is over. A continue-as-new carries the subscription to the
	// current run, and that is the only run worth pushing at.
	current, err := h.probeConsumer(ctx, namespaceID, workflowID, "", streamID)
	if err != nil {
		return nil, err
	}
	if current.closed || current.runID == pinned.runID || !current.consumes {
		out.ConsumerClosed = true
		return &streamlib.AdvanceConsumerHeadResponse{FrontendResponse: out}, nil
	}
	err = h.pushHead(ctx, namespaceID, workflowID, current.runID, streamID, in.GetHeadOffset())
	if err != nil {
		return nil, err
	}
	out.SuccessorRunId = current.runID
	out.SuccessorStartOffset = current.startOffset
	return &streamlib.AdvanceConsumerHeadResponse{FrontendResponse: out}, nil
}

func (h *handler) PollMessages(
	ctx context.Context,
	req *streamlib.PollMessagesRequest,
) (*streamlib.PollMessagesResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	ns := h.namespaceName(req.GetNamespaceId())
	if err := h.admitPoll(ns); err != nil {
		return nil, err
	}

	ref := refForRun(req.GetNamespaceId(), in.GetStreamId(), in.GetRunId())
	from, start := in.GetFromOffset(), in.GetStartPosition()

	// Only the blocking path needs the frontier before the read. On the
	// ordinary path the window carries it, and this is the hottest call in the
	// feature: the state clone walks the producer and consumer tables, which
	// hold up to a thousand entries each.
	if in.GetWaitNewMessages() {
		// One budget for the whole call, however it is spent: on the stream
		// coming into existence, and then on it moving.
		pollCtx, cancel := pollBudget(ctx)
		defer cancel()

		state, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).Snapshot, struct{}{})
		if executionAbsent(err) && in.GetRunId() == "" {
			state, err = h.waitForStream(pollCtx, ctx, ref, err)
		}
		if err != nil {
			return nil, err
		}
		waitFrom, err := waitOffset(from, start, state)
		if err != nil {
			return nil, err
		}
		// Blocking is only worth it once the reader is genuinely caught up.
		if waitFrom == state.GetHeadOffset() && !state.GetClosed() {
			// The window is re-read below, so only the blocking matters here.
			if _, err := h.waitForMessages(pollCtx, ctx, ref, waitFrom, state); err != nil {
				return nil, err
			}
			from, start = waitFrom, nil
		}
	}

	wreq := stream.WindowRequest{
		From: from, Start: start, MaxMessages: in.GetMaxMessages(), Topics: in.GetTopics(),
	}
	w, err := chasm.ReadComponent(ctx, ref, (*stream.Stream).ReadWindow, wreq)
	if err != nil {
		return nil, err
	}
	out, err := formatWindow(w, wreq)
	if err != nil {
		return nil, err
	}
	h.meterDelivery(ns, out)
	return &streamlib.PollMessagesResponse{FrontendResponse: out}, nil
}

// PollWorkflowMessages reads a stream an execution owns.
//
// An attached stream is reached through its owner, so this call routes on the
// owner and both the frontier and the batches come out of the owner's
// component.
func (h *handler) PollWorkflowMessages(
	ctx context.Context,
	req *streamlib.PollWorkflowMessagesRequest,
) (*streamlib.PollWorkflowMessagesResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	target, err := resolveOwned(req.GetNamespaceId(), in.GetOwner(), in.GetStreamName())
	if err != nil {
		return nil, err
	}
	ns := h.namespaceName(req.GetNamespaceId())
	if err := h.admitPoll(ns); err != nil {
		return nil, err
	}
	from, start := in.GetFromOffset(), in.GetStartPosition()

	state, err := h.ownedStreamState(ctx, target)
	if err != nil {
		return nil, err
	}

	if in.GetWaitNewMessages() {
		waitFrom, err := waitOffset(from, start, state)
		if err != nil {
			return nil, err
		}
		if waitFrom == state.GetHeadOffset() && !state.GetClosed() {
			pollCtx, cancel := pollBudget(ctx)
			defer cancel()
			if _, err := h.waitForOwnedMessages(pollCtx, ctx, target, waitFrom, state); err != nil {
				return nil, err
			}
			from, start = waitFrom, nil
		}
	}

	wreq := stream.WindowRequest{
		From: from, Start: start, MaxMessages: in.GetMaxMessages(), Topics: in.GetTopics(),
	}
	w, err := chasm.ReadComponent(ctx, target.ref, readOwnedWindow,
		ownedWindowRequest{Key: target.key, Window: wreq})
	if err != nil {
		return nil, err
	}
	out, err := formatWindow(w, wreq)
	if err != nil {
		return nil, err
	}
	h.meterDelivery(ns, out)
	return &streamlib.PollWorkflowMessagesResponse{FrontendResponse: out}, nil
}

// formatWindow turns a component read into the wire response. The read happens
// in the component, so the frontier and the bytes it was served with cannot
// disagree.
//
// The reader is advanced only over offsets that were examined. CollectRecords
// steps past every message it filtered out, so a filtered page that matched
// nothing still moves the reader; a window whose batches stop short of its end
// must not be reported as read to the end.
func formatWindow(w stream.Window, req stream.WindowRequest) (*streamlib.PollMessagesOutput, error) {
	out := &streamlib.PollMessagesOutput{
		NextOffset:  w.From,
		HeadOffset:  w.State.GetHeadOffset(),
		Closed:      w.State.GetClosed(),
		CloseReason: w.State.GetCloseReason(),
		RunId:       w.RunID,
	}
	if w.From == w.State.GetHeadOffset() {
		return out, nil
	}

	records, next, err := stream.CollectRecords(
		w.Blobs, w.Starts, w.From, w.To, w.Limit, req.Topics)
	if err != nil {
		return nil, err
	}
	out.Records = records
	out.NextOffset = next
	return out, nil
}

// ownedWindowRequest names which attached stream to read and what to read.
type ownedWindowRequest struct {
	Key    string
	Window stream.WindowRequest
}

// readOwnedWindow reads a stream attached to an owner.
//
// An owner that has ended can take no more records, so its stream is finished
// whether or not a producer said so. Without that a reader tailing a workflow
// or an activity that ended stays parked forever.
func readOwnedWindow(
	owner stream.Owner,
	cctx chasm.Context,
	req ownedWindowRequest,
) (stream.Window, error) {
	ended := owner.OwnedStreamEnded(cctx, req.Key)
	s := owner.OwnedStream(cctx, req.Key)
	if s == nil {
		// Nothing published yet, which reads as an empty stream so a reader can
		// attach before the first append.
		state := &streamlib.StreamState{Closed: ended}
		from := req.Window.From
		if req.Window.Start != nil {
			var err error
			if from, err = stream.ResolveStart(req.Window.Start, state); err != nil {
				return stream.Window{}, err
			}
		}
		return stream.Window{State: state, From: from, To: from}, nil
	}
	w, err := s.ReadWindow(cctx, req.Window)
	if err != nil {
		return stream.Window{}, err
	}
	if ended {
		w.State.Closed = true
	}
	return w, nil
}

// ownedStreamState snapshots an attached stream through the component that
// owns it. A stream nothing has published to yet reads as an empty one, so a
// reader may attach before the first record.
func (h *handler) ownedStreamState(
	ctx context.Context,
	target ownedTarget,
) (*streamlib.StreamState, error) {
	return chasm.ReadComponent(ctx, target.ref, readOwnedStream, target.key)
}

// readOwnedStream snapshots an attached stream and reports whether anything
// can still be added to it, which is not the case once its owner has ended.
func readOwnedStream(
	owner stream.Owner,
	cctx chasm.Context,
	key string,
) (*streamlib.StreamState, error) {
	state := &streamlib.StreamState{}
	if s := owner.OwnedStream(cctx, key); s != nil {
		var err error
		if state, err = s.Snapshot(cctx, struct{}{}); err != nil {
			return nil, err
		}
	}
	if owner.OwnedStreamEnded(cctx, key) {
		state.Closed = true
	}
	return state, nil
}

// pollBudget is how long one blocking poll may hold its caller, whatever it is
// waiting for. It ends before the caller's own deadline, so expiry can be
// answered with an empty response rather than an error.
func pollBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	return contextutil.WithDeadlineBuffer(ctx, stream.LongPollTimeout, stream.LongPollBuffer)
}

// waitForStream parks a blocking poll on a standalone stream id that names
// nothing yet, asking again on an interval until the stream exists or the
// poll's budget runs out. A reader can then attach before the producer's first
// write, as it can on an owned stream, whose owner is there to wait on. On
// expiry the caller gets the refusal it would have got at once.
//
// Only an unpinned reference waits. A run id names one execution, and a stream
// created later is another run, so that wait could never be answered.
func (h *handler) waitForStream(
	pollCtx, callerCtx context.Context,
	ref chasm.ComponentRef,
	absent error,
) (*streamlib.StreamState, error) {
	ticker := time.NewTicker(h.config.CreateWaitRecheckInterval())
	defer ticker.Stop()
	for {
		select {
		case <-pollCtx.Done():
			if callerCtx.Err() != nil {
				return nil, callerCtx.Err()
			}
			return nil, absent
		case <-ticker.C:
		}
		state, err := chasm.ReadComponent(pollCtx, ref, (*stream.Stream).Snapshot, struct{}{})
		switch {
		case err == nil:
			return state, nil
		case executionAbsent(err), pollCtx.Err() != nil:
			continue
		default:
			return nil, err
		}
	}
}

// waitForMessages blocks until the head passes the reader's offset or the
// stream closes. On the server's long-poll timeout it returns the state it last
// saw, so the caller gets an empty response and polls again rather than an
// error it would have to distinguish from a real failure.
func (h *handler) waitForMessages(
	pollCtx, callerCtx context.Context,
	ref chasm.ComponentRef,
	from int64,
	current *streamlib.StreamState,
) (*streamlib.StreamState, error) {
	state, _, err := chasm.PollComponent(pollCtx, ref,
		func(s *stream.Stream, _ chasm.Context, offset int64) (*streamlib.StreamState, bool, error) {
			// Monotonic, as PollComponent requires: the head only advances and
			// closed never clears.
			if !pollSatisfied(s.State, offset) {
				return nil, false, nil
			}
			return common.CloneProto(s.State), true, nil
		}, from)
	return pollOutcome(pollCtx, callerCtx, state, err, current)
}

// waitForOwnedMessages is waitForMessages against a stream reached through its
// owner. The predicate has to re-resolve the stream on every evaluation,
// because what the poll observes is the owning execution.
func (h *handler) waitForOwnedMessages(
	pollCtx, callerCtx context.Context,
	target ownedTarget,
	from int64,
	current *streamlib.StreamState,
) (*streamlib.StreamState, error) {
	state, _, err := chasm.PollComponent(pollCtx, target.ref,
		func(
			owner stream.Owner, cctx chasm.Context, offset int64,
		) (*streamlib.StreamState, bool, error) {
			owned, err := readOwnedStream(owner, cctx, target.key)
			if err != nil {
				return nil, false, err
			}
			if !pollSatisfied(owned, offset) {
				return nil, false, nil
			}
			return owned, true, nil
		}, from)
	return pollOutcome(pollCtx, callerCtx, state, err, current)
}

// waitOffset is the offset a blocking poll waits past. A first poll that names
// a start position resolves it against the frontier the wait begins from, and
// the read after the wait uses that offset: resolving the position again would
// put a tail at the new head and read nothing.
func waitOffset(
	from int64, start *streampb.StreamStartPosition, state *streamlib.StreamState,
) (int64, error) {
	if start == nil {
		return from, nil
	}
	return stream.ResolveStart(start, state)
}

// pollSatisfied is the monotonic condition PollComponent requires: the head
// only advances and closed never clears.
func pollSatisfied(state *streamlib.StreamState, from int64) bool {
	return state.GetHeadOffset() > from || state.GetClosed()
}

// pollOutcome turns a long-poll result into the state the reader should be
// served.
func pollOutcome(
	pollCtx, callerCtx context.Context,
	state *streamlib.StreamState,
	err error,
	current *streamlib.StreamState,
) (*streamlib.StreamState, error) {
	if err != nil {
		if pollCtx.Err() != nil && callerCtx.Err() == nil {
			// Our long-poll budget expired, not the caller's. Hand back the
			// state we already had so the reader gets an empty response and
			// polls again, rather than an error it has to tell apart from a
			// real failure.
			return current, nil
		}
		return nil, err
	}
	if state == nil {
		return current, nil
	}
	return state, nil
}

func (h *handler) DescribeStream(
	ctx context.Context,
	req *streamlib.DescribeStreamRequest,
) (*streamlib.DescribeStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	state, err := chasm.ReadComponent(ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()), (*stream.Stream).Snapshot, struct{}{})
	if err != nil {
		return nil, err
	}
	return &streamlib.DescribeStreamResponse{
		FrontendResponse: &streamlib.DescribeStreamOutput{State: state},
	}, nil
}

// DescribeWorkflowStream reports the frontier of a stream an execution owns. A
// reader needs it to start at the tail rather than at the beginning, which an
// attached stream offers no other way to find.
func (h *handler) DescribeWorkflowStream(
	ctx context.Context,
	req *streamlib.DescribeWorkflowStreamRequest,
) (*streamlib.DescribeWorkflowStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())

	target, err := resolveOwned(req.GetNamespaceId(), in.GetOwner(), in.GetStreamName())
	if err != nil {
		return nil, err
	}
	state, err := h.ownedStreamState(ctx, target)
	if err != nil {
		return nil, err
	}
	return &streamlib.DescribeWorkflowStreamResponse{
		FrontendResponse: &streamlib.DescribeStreamOutput{State: state},
	}, nil
}

func (h *handler) CloseStream(
	ctx context.Context,
	req *streamlib.CloseStreamRequest,
) (*streamlib.CloseStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	limits := h.limitsFor(req.GetNamespaceId())
	_, _, err := chasm.UpdateComponent(
		ctx,
		refFor(req.GetNamespaceId(), in.GetStreamId()),
		func(s *stream.Stream, mctx chasm.MutableContext, reason *commonpb.Payload) (struct{}, error) {
			if s.State.GetClosed() {
				return struct{}{}, nil
			}
			if err := s.CloseAndSchedule(mctx, reason); err != nil {
				return struct{}{}, err
			}
			s.ScheduleChannelNotify(mctx, stream.ChannelName(in.GetStreamId()), in.GetStreamId(), limits)
			return struct{}{}, nil
		},
		in.GetReason(),
	)
	if err != nil {
		return nil, err
	}
	return &streamlib.CloseStreamResponse{FrontendResponse: &streamlib.CloseStreamOutput{}}, nil
}

// TruncateStream advances the readable floor.
//
// A refusal means an active consumer's pin is below the requested base. Some
// of those pins belong to nobody: a subscription that took its pin and then
// failed to write its cursor leaves one behind, and the only thing that
// notices is the notify probe, which runs on append. A stream that has gone
// quiet never gets one. So a refusal probes the pins it was refused for and
// tries once more, and what survives that is a consumer that really is there.
func (h *handler) TruncateStream(
	ctx context.Context,
	req *streamlib.TruncateStreamRequest,
) (*streamlib.TruncateStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	ref := refFor(req.GetNamespaceId(), in.GetStreamId())

	err := h.truncate(ctx, ref, in.GetNewBaseOffset())
	if _, ok := errors.AsType[*serviceerror.FailedPrecondition](err); ok {
		state, readErr := chasm.ReadComponent(ctx, ref, (*stream.Stream).Snapshot, struct{}{})
		if readErr != nil {
			return nil, err
		}
		notifier := consumerNotifier{logger: h.logger, routed: h.routed}
		if probeErr := notifier.notify(
			ctx, ref, in.GetNamespace(), state, true); probeErr != nil {
			return nil, err
		}
		err = h.truncate(ctx, ref, in.GetNewBaseOffset())
	}
	if err != nil {
		return nil, err
	}
	return &streamlib.TruncateStreamResponse{FrontendResponse: &streamlib.TruncateStreamOutput{}}, nil
}

func (h *handler) truncate(ctx context.Context, ref chasm.ComponentRef, newBase int64) error {
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(s *stream.Stream, mctx chasm.MutableContext, base int64) (struct{}, error) {
			return struct{}{}, s.Truncate(mctx, base)
		},
		newBase,
	)
	return err
}

// ListStreams is intentionally not implemented here. It queries visibility, so
// it has no business ID to route on and the frontend answers it directly.

func (h *handler) DeleteStream(
	ctx context.Context,
	req *streamlib.DeleteStreamRequest,
) (*streamlib.DeleteStreamResponse, error) {
	in := req.GetFrontendRequest()
	ctx = h.withCallerInfo(ctx, req.GetNamespaceId())
	key := chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: in.GetStreamId()}

	// A workflow consuming the stream recorded ranges its replay will ask for.
	// Deleting under it succeeds now and fails that workflow later, so the
	// caller has to say it means it. Checked and then deleted rather than in
	// one transition, which leaves a window for a subscription that lands in
	// between; that consumer finds out at its next task, with a cause.
	if !in.GetForce() {
		state, err := chasm.ReadComponent(ctx, refFor(key.NamespaceID, key.BusinessID),
			(*stream.Stream).Snapshot, struct{}{})
		if err != nil {
			return nil, err
		}
		for id, consumer := range state.GetConsumers() {
			if consumer.GetActive() {
				return nil, serviceerror.NewFailedPreconditionf(
					"stream %q is consumed by workflow %q (%s); set force to delete it anyway",
					key.BusinessID, consumer.GetWorkflowId(), id)
			}
		}
	}

	// The payload is component state, so deleting the execution takes it too.
	err := chasm.DeleteExecution[*stream.Stream](ctx, key, chasm.DeleteExecutionRequest{})
	if err != nil {
		return nil, err
	}
	return &streamlib.DeleteStreamResponse{FrontendResponse: &streamlib.DeleteStreamOutput{}}, nil
}
