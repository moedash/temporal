package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/stream"
	"go.temporal.io/server/common/payload"
)

// The channel a held stream notifies is named by the stream alone for one
// the workflow owns, and by the activity and the stream for one of its
// activities owns, with the activity id as the caller wrote it.
func TestStreamChannelName(t *testing.T) {
	require.Equal(t, "stream/output", streamChannelName("output"))
	require.Equal(t, "stream/model/call/out",
		streamChannelName(ActivityStreamKey("model/call", "out")))
	require.Equal(t, "stream/act/output", streamChannelName(ActivityStreamKey("act", "output")))
}

// An append to a stream the run holds notifies the linked channel of the
// stream's name without waking the run, the activity's end closes its streams
// with a last change, and the switch leaves no channel behind.
func TestStreamAppendNotifiesLinkedChannel(t *testing.T) {
	ctx := newOwnerContext()
	w := &Workflow{}
	_, err := w.AppendToOwnedStream(ctx, "output", stream.AddMessagesRequest{
		Records: budgetTestRecords(1),
	})
	require.NoError(t, err)
	c, ok := w.LinkedChannel(ctx, "stream/output")
	require.True(t, ok, "the first change creates the channel")
	require.Equal(t, int64(1), c.LatestCounter())
	require.Equal(t, int64(1), c.RetainedCount())
	require.Equal(t, "run-1:1", string(c.State.GetLatest().GetPosition()))
	require.False(t, c.HasOwnerPending(), "the run is not woken by its own stream")
	require.False(t, w.HasPendingChannelNotifications(ctx))

	key := ActivityStreamKey("act", "output")
	_, err = w.AppendToOwnedStream(ctx, key, stream.AddMessagesRequest{Records: budgetTestRecords(1)})
	require.NoError(t, err)
	require.NoError(t, w.CloseActivityStreams(ctx, "act", stream.Limits{}))
	c, ok = w.LinkedChannel(ctx, "stream/act/output")
	require.True(t, ok)
	require.Equal(t, int64(2), c.LatestCounter())
	var closed bool
	require.NoError(t, payload.Decode(
		c.State.GetLatest().GetMetadata()[stream.ClosedMetadataKey], &closed))
	require.True(t, closed)
	require.False(t, w.HasPendingChannelNotifications(ctx))

	off := &Workflow{}
	_, err = off.AppendToOwnedStream(ctx, "output", stream.AddMessagesRequest{
		Records: budgetTestRecords(1), Limits: stream.Limits{ChannelNotifyOff: true},
	})
	require.NoError(t, err)
	require.Zero(t, off.LinkedChannelCount())
}
