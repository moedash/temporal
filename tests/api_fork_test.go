package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	streampb "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
)

// The api fork has to round-trip the new shapes over the wire, not merely
// compile. A field added without its descriptor would compile and silently
// drop on marshal.
func TestApiForkCarriesStreamShapes(t *testing.T) {
	require.Equal(t, enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES, enumspb.CommandType(19))

	cmd := &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_ADD_STREAM_MESSAGES,
		Attributes: &commandpb.Command_AddStreamMessagesCommandAttributes{
			AddStreamMessagesCommandAttributes: &commandpb.AddStreamMessagesCommandAttributes{
				StreamId: "s1",
				Messages: []*streampb.StreamMessage{{Topic: "tokens"}},
			},
		},
	}
	b, err := proto.Marshal(cmd)
	require.NoError(t, err)
	var back commandpb.Command
	require.NoError(t, proto.Unmarshal(b, &back))
	require.Equal(t, "s1", back.GetAddStreamMessagesCommandAttributes().GetStreamId())
	require.Equal(t, "tokens", back.GetAddStreamMessagesCommandAttributes().GetMessages()[0].GetTopic())

	resp := &workflowservice.PollWorkflowTaskQueueResponse{
		StreamSlices: []*streampb.StreamSlice{{StreamId: "s1", FromOffset: 4, ToOffset: 7}},
	}
	rb, err := proto.Marshal(resp)
	require.NoError(t, err)
	var rback workflowservice.PollWorkflowTaskQueueResponse
	require.NoError(t, proto.Unmarshal(rb, &rback))
	require.Equal(t, int64(7), rback.GetStreamSlices()[0].GetToOffset())

	attrs := &historypb.WorkflowTaskCompletedEventAttributes{
		StreamCursors: []*streampb.StreamCursor{{StreamId: "s1", FromOffset: 4, ToOffset: 4}},
	}
	ab, err := proto.Marshal(attrs)
	require.NoError(t, err)
	var aback historypb.WorkflowTaskCompletedEventAttributes
	require.NoError(t, proto.Unmarshal(ab, &aback))
	// An empty range has to survive the round trip: it is the fact that a
	// subscription observed nothing, which replay must reproduce.
	require.Len(t, aback.GetStreamCursors(), 1)
	require.Equal(t, aback.GetStreamCursors()[0].GetFromOffset(), aback.GetStreamCursors()[0].GetToOffset())
}
