package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
)

var _ stream.Owner = (*Activity)(nil)

func (a *Activity) ownedStreams() stream.OwnedStreams {
	return stream.OwnedStreams{Streams: &a.Streams, Kind: "activity"}
}

// OwnedStream returns the stream under key, or nil when nothing has created it.
func (a *Activity) OwnedStream(ctx chasm.Context, key string) *stream.Stream {
	return a.ownedStreams().Get(ctx, key)
}

// OwnedStreamEnded reports that the activity reached a terminal status. A
// failed attempt that will be retried is not the end: the next attempt writes
// to the same stream.
func (a *Activity) OwnedStreamEnded(_ chasm.Context, _ string) bool {
	return a.isTerminal()
}

// AppendToOwnedStream appends on behalf of a writer outside the execution,
// which for an activity is every writer: an activity publishes over the
// stream service, never through commands.
//
// Refused once the activity is terminal. A worker still running an attempt
// the server already timed out would otherwise keep adding to a stream its
// readers were told had ended.
func (a *Activity) AppendToOwnedStream(
	mctx chasm.MutableContext,
	key string,
	req stream.AddMessagesRequest,
) (stream.AddMessagesResult, error) {
	if a.isTerminal() {
		return stream.AddMessagesResult{}, stream.Refusal(stream.ReasonStreamClosed,
			"activity execution closed with status %v, so its streams take no more records",
			InternalStatusToAPIStatus(a.GetStatus()))
	}
	return a.ownedStreams().Append(mctx, key, req)
}
