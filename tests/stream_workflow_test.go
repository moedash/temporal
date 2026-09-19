package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streampb "go.temporal.io/api/stream/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	chasmstream "go.temporal.io/server/chasm/lib/stream"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Path A: a workflow publishing to a stream it owns. The stream is co-located
// with the workflow, so the frontier advances in the workflow task's own
// commit, and History gets one fixed-size event naming the offset range rather
// than anything that was published.
//
// Driven through the raw task poller rather than an SDK, because emitting a new
// command type does not need one.
func TestStreamWorkflowPublishesWithARangeEvent(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	id := "stream-wf-publish-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	we, err := env.FrontendClient().StartWorkflowExecution(
		s.ctx(),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.ns,
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: "stream-publisher"},
			TaskQueue:           tq,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            "tester",
		},
	)
	require.NoError(t, err)

	published := false
	// The newer taskpoller cannot emit an arbitrary command type, which is
	// the whole point here.
	//nolint:staticcheck // SA1019: deprecated poller is the only one that can.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			if published {
				return completeWorkflowCommand(), nil
			}
			published = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
				Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
					AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
						Messages: []*streampb.StreamMessage{
							{Body: &commonpb.Payload{Data: []byte("planning")}, Topic: "progress"},
							{Body: &commonpb.Payload{Data: []byte("calling tool")}, Topic: "progress"},
						},
					},
				},
			}}, nil
		},
		Logger: env.Logger,
		T:      t,
	}

	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	// One event for the batch, holding the range and none of the payload. Two
	// messages were published, so it has to name both of them and stop there.
	events := env.GetHistory(s.ns, &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.GetRunId()})
	var added []*historypb.WorkflowStreamMessagesAddedEventAttributes
	for _, e := range events {
		if a := e.GetWorkflowStreamMessagesAddedEventAttributes(); a != nil {
			added = append(added, a)
		}
	}
	require.Len(t, added, 1, "one publish command writes one event")
	require.Equal(t, int64(0), added[0].GetFirstOffset())
	require.Equal(t, int64(2), added[0].GetMessageCount())
	require.Equal(t, chasmworkflow.DefaultStreamName, added[0].GetStreamId(),
		"an unnamed stream resolves to the default before it is recorded")

	// The bodies stay out of History. Asserted on the serialized event rather
	// than on its fields, because a field this test forgot to check would still
	// be carrying them.
	raw, err := added[0].Marshal()
	require.NoError(t, err)
	require.NotContains(t, string(raw), "calling tool",
		"the event must name the range, never carry the payload")

	// The bodies are readable from outside the workflow, which is the point of
	// publishing them: an attached stream has no id, so it is addressed by its
	// owner and its name.
	poll, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: id, FromOffset: 0,
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"planning", "calling tool"},
		bodies(poll.GetFrontendResponse().GetMessages()))
	require.Equal(t, []int64{0, 1}, offsets(poll.GetFrontendResponse().GetMessages()),
		"each message carries where it sits in the log, so a reader can resume between them")
	require.Equal(t, int64(2), poll.GetFrontendResponse().GetNextOffset())
	require.Equal(t, int64(2), poll.GetFrontendResponse().GetHeadOffset())

	// The topic filter applies to an attached stream the same as to any other,
	// and offsets stay assigned over the unfiltered stream.
	filtered, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: id, FromOffset: 0, Topics: []string{"nothing-here"},
		},
	})
	require.NoError(t, err)
	require.Empty(t, filtered.GetFrontendResponse().GetMessages())
	require.Equal(t, int64(2), filtered.GetFrontendResponse().GetNextOffset(),
		"a filtered page still advances the reader")

	// A stream the workflow has not published to reads as an empty one, which
	// is what lets a reader attach before the first event.
	unwritten, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: id, StreamName: "not-published-to-yet", FromOffset: 0,
		},
	})
	require.NoError(t, err)
	require.Empty(t, unwritten.GetFrontendResponse().GetMessages())
	require.Equal(t, int64(0), unwritten.GetFrontendResponse().GetHeadOffset())
	require.False(t, unwritten.GetFrontendResponse().GetClosed())

	// A workflow that does not exist is still an error, so a mistyped owner is
	// not mistaken for a stream with nothing in it.
	_, err = s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: id + "-does-not-exist", FromOffset: 0,
		},
	})
	require.ErrorContains(t, err, "not found")

	// An attached stream still has no standalone id, so the id-addressed read
	// must not reach it.
	streamID := we.GetRunId() + "/" + chasmworkflow.DefaultStreamName
	_, err = s.client.PollMessages(s.ctx(), &streamlib.PollMessagesRequest{
		FrontendRequest: &streamlib.PollMessagesInput{
			Namespace: s.ns, StreamId: streamID, FromOffset: 0,
		},
	})
	require.ErrorContains(t, err, "stream not found",
		"an attached stream is only reachable through its workflow")
}

// A reader attached to a workflow's stream before the workflow published
// anything. This is the ordinary case for a UI that opens on a session and
// waits for the agent's first token, so the poll has to park on a stream that
// does not exist yet and wake when the workflow creates it.
func TestStreamWorkflowLongPollWakesOnPublish(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	id := "stream-wf-longpoll-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	_, err := env.FrontendClient().StartWorkflowExecution(
		s.ctx(),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.ns,
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: "stream-publisher"},
			TaskQueue:           tq,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            "tester",
		},
	)
	require.NoError(t, err)

	type result struct {
		out *streamlib.PollMessagesOutput
		err error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
			FrontendRequest: &streamlib.PollWorkflowMessagesInput{
				Namespace: s.ns, WorkflowId: id, FromOffset: 0, WaitNewMessages: true,
			},
		})
		done <- result{resp.GetFrontendResponse(), err}
	}()

	//nolint:staticcheck // SA1019: deprecated poller is the only one that can emit the command.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
				Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
					AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
						Messages: []*streampb.StreamMessage{
							{Body: &commonpb.Payload{Data: []byte("first token")}},
						},
					},
				},
			}}, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, []string{"first token"}, bodies(r.out.GetMessages()))
		require.Equal(t, int64(1), r.out.GetNextOffset())
	case <-time.After(25 * time.Second):
		t.Fatal("the parked reader did not wake when the workflow published")
	}
}

// Two producers on one stream: the workflow, whose publishes ride its Workflow
// Task, and something outside it. This is the shape of an agent session, where
// a model activity produces the tokens and the workflow only brackets them.
func TestStreamWorkflowTakesAppendsFromOutsideToo(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	id := "stream-wf-mixed-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	_, err := env.FrontendClient().StartWorkflowExecution(
		s.ctx(),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.ns,
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: "stream-publisher"},
			TaskQueue:           tq,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            "tester",
		},
	)
	require.NoError(t, err)

	appendOutside := func(body string) *streamlib.AddMessagesOutput {
		t.Helper()
		resp, err := s.client.AddWorkflowMessages(s.ctx(), &streamlib.AddWorkflowMessagesRequest{
			FrontendRequest: &streamlib.AddWorkflowMessagesInput{
				Namespace: s.ns, WorkflowId: id,
				Messages: []*streamlib.StreamMessage{{
					Body: &commonpb.Payload{Data: []byte(body)},
					Kind: streamlib.STREAM_MESSAGE_KIND_DATA,
				}},
			},
		})
		require.NoError(t, err)
		return resp.GetFrontendResponse()
	}

	// The first writer creates the stream, and here that is not the workflow.
	first := appendOutside("token one")
	require.Equal(t, int64(0), first.GetFirstOffset())

	//nolint:staticcheck // SA1019: deprecated poller is the only one that can emit the command.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
				Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
					AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
						Messages: []*streampb.StreamMessage{
							{Body: &commonpb.Payload{Data: []byte("turn ended")}},
						},
					},
				},
			}}, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	// The workflow appended after the outside writer, so it has to continue the
	// same log rather than start its own.
	require.Equal(t, int64(2), appendOutside("token two").GetFirstOffset())

	poll, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: id, FromOffset: 0,
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"token one", "turn ended", "token two"},
		bodies(poll.GetFrontendResponse().GetMessages()),
		"both producers write one ordered log")

	// The event names the offset the workflow's own publish landed at, which
	// only holds if the workflow saw the outside writer's message first.
	events := env.GetHistory(s.ns, &commonpb.WorkflowExecution{WorkflowId: id})
	var added []*historypb.WorkflowStreamMessagesAddedEventAttributes
	for _, e := range events {
		if a := e.GetWorkflowStreamMessagesAddedEventAttributes(); a != nil {
			added = append(added, a)
		}
	}
	require.Len(t, added, 1)
	require.Equal(t, int64(1), added[0].GetFirstOffset())
}

// A reader tailing a workflow that ends has to be released. Nothing can be
// added to a stream inside a closed execution, so a reader parked on it would
// otherwise wait for a message that can never come.
func TestStreamWorkflowStreamClosesWithItsWorkflow(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)

	id := "stream-wf-close-" + uuid.NewString()
	tq := &taskqueuepb.TaskQueue{Name: id + "-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	_, err := env.FrontendClient().StartWorkflowExecution(
		s.ctx(),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.ns,
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: "stream-publisher"},
			TaskQueue:           tq,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            "tester",
		},
	)
	require.NoError(t, err)

	describe := func() *streamlib.StreamState {
		t.Helper()
		resp, err := s.client.DescribeWorkflowStream(s.ctx(), &streamlib.DescribeWorkflowStreamRequest{
			FrontendRequest: &streamlib.DescribeWorkflowStreamInput{
				Namespace: s.ns, WorkflowId: id,
			},
		})
		require.NoError(t, err)
		return resp.GetFrontendResponse().GetState()
	}

	//nolint:staticcheck // SA1019: deprecated poller is the only one that can emit the command.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
					Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
						AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
							Messages: []*streampb.StreamMessage{
								{Body: &commonpb.Payload{Data: []byte("last word")}},
							},
						},
					},
				},
				completeWorkflowCommand()[0],
			}, nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err = poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	require.True(t, describe().GetClosed(), "the stream of a completed workflow reads as closed")

	// Closed does not mean gone. Everything published before the workflow
	// ended is still there to be read.
	poll, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: id, FromOffset: 0, WaitNewMessages: true,
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"last word"}, bodies(poll.GetFrontendResponse().GetMessages()))
	require.True(t, poll.GetFrontendResponse().GetClosed())
}

// An outside producer and the workflow's own publishes land in one log, with
// neither pinning the head against the other. Run concurrently, every append
// has to succeed and every message has to appear once, in a contiguous range.
func TestOutsideAppendsRaceTheWorkflowPublishWithoutFailing(t *testing.T) {
	env := testcore.NewEnv(t)
	s := newStreamTestEnvFrom(t, env)
	execution, tq := startConsumer(t, s, "stream-wf-race-")

	const producers = 8
	const perProducer = 10
	const workflowTasks = 5

	//nolint:staticcheck // SA1019: deprecated poller is the only one that can emit the command.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			return publishCommand("from-workflow"), nil
		},
		Logger: env.Logger,
		T:      t,
	}

	errs := make(chan error, producers*perProducer)
	var wg sync.WaitGroup
	for p := range producers {
		wg.Go(func() {
			for i := range perProducer {
				_, err := s.client.AddWorkflowMessages(s.ctx(), &streamlib.AddWorkflowMessagesRequest{
					FrontendRequest: &streamlib.AddWorkflowMessagesInput{
						Namespace: s.ns, WorkflowId: execution.GetWorkflowId(),
						Messages: []*streamlib.StreamMessage{{
							Body: &commonpb.Payload{Data: []byte(fmt.Sprintf("outside-%d-%d", p, i))},
						}},
					},
				})
				errs <- err
			}
		})
	}
	for range workflowTasks {
		_, err := poller.PollAndProcessWorkflowTask()
		require.NoError(t, err)
		signalWorkflow(t, s, execution.GetWorkflowId(), execution.GetRunId())
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "an outside append must not fail because the workflow published")
	}

	total := producers*perProducer + workflowTasks
	poll, err := s.client.PollWorkflowMessages(s.ctx(), &streamlib.PollWorkflowMessagesRequest{
		FrontendRequest: &streamlib.PollWorkflowMessagesInput{
			Namespace: s.ns, WorkflowId: execution.GetWorkflowId(), FromOffset: 0,
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(total), poll.GetFrontendResponse().GetHeadOffset())
	seen := make(map[string]int, total)
	for _, body := range bodies(poll.GetFrontendResponse().GetMessages()) {
		seen[body]++
	}
	require.Len(t, seen, producers*perProducer+1, "every outside message appears exactly once")
	require.Equal(t, workflowTasks, seen["from-workflow"])
}

// A stream a workflow owns has a budget, because its batches are the workflow's
// own mutable state. Past it the workflow's publish fails its task with a cause
// and an outside producer is told the stream is full, instead of the execution
// size limit terminating the workflow later.
func TestOwnedStreamRefusesAppendsPastItsBudget(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(chasmstream.OwnedStreamMaxItemsSetting, 3))
	s := newStreamTestEnvFrom(t, env)
	execution, tq := startConsumer(t, s, "stream-wf-budget-")

	task := 0
	//nolint:staticcheck // SA1019: deprecated poller is the only one that can emit the command.
	poller := &testcore.TaskPoller{
		Client:    env.FrontendClient(),
		Namespace: s.ns,
		TaskQueue: tq,
		Identity:  "tester",
		WorkflowTaskHandler: func(
			*workflowservice.PollWorkflowTaskQueueResponse,
		) ([]*commandpb.Command, error) {
			task++
			if task == 1 {
				return publishCommand("one", "two"), nil
			}
			return publishCommand("three", "four"), nil
		},
		Logger: env.Logger,
		T:      t,
	}
	_, err := poller.PollAndProcessWorkflowTask()
	require.NoError(t, err)

	signalWorkflow(t, s, execution.GetWorkflowId(), execution.GetRunId())
	_, err = poller.PollAndProcessWorkflowTask()
	require.Error(t, err, "two more do not fit in a budget of three")
	failed := workflowTaskFailedWith(env.GetHistory(s.ns, execution),
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_ADD_STREAM_MESSAGES_ATTRIBUTES)
	require.NotNil(t, failed)
	require.Contains(t, failed.GetFailure().GetMessage(), "budget")

	appendOutside := func(bodies ...string) error {
		messages := make([]*streamlib.StreamMessage, len(bodies))
		for i, b := range bodies {
			messages[i] = &streamlib.StreamMessage{Body: &commonpb.Payload{Data: []byte(b)}}
		}
		_, err := s.client.AddWorkflowMessages(s.ctx(), &streamlib.AddWorkflowMessagesRequest{
			FrontendRequest: &streamlib.AddWorkflowMessagesInput{
				Namespace: s.ns, WorkflowId: execution.GetWorkflowId(), Messages: messages,
			},
		})
		return err
	}
	// The test client is a bare gRPC connection, so the code is what it sees.
	require.Equal(t, codes.ResourceExhausted, status.Code(appendOutside("three", "four")))
	require.NoError(t, appendOutside("three"), "the last slot is still there for one that fits")
	require.Equal(t, codes.ResourceExhausted, status.Code(appendOutside("four")))
}
