package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

func newTestCursor(offset int64) *Cursor {
	return &Cursor{
		State: &streampb.WorkflowStreamCursor{
			StreamId:     "s-1",
			CollectionId: "col-1",
			BucketSize:   DefaultBucketSize,
			Offset:       offset,
		},
	}
}

func TestNewCursorRejectsIncompleteRequests(t *testing.T) {
	cases := map[string]NewCursorRequest{
		"no stream id":     {CollectionID: "col-1", BucketSize: 10},
		"no collection id": {StreamID: "s-1", BucketSize: 10},
		"zero bucket size": {StreamID: "s-1", CollectionID: "col-1"},
		"negative start":   {StreamID: "s-1", CollectionID: "col-1", BucketSize: 10, StartOffset: -1},
	}

	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewCursor(nil, req)
			require.Error(t, err)
		})
	}
}

func TestCursorCommitAdvancesAndClears(t *testing.T) {
	c := newTestCursor(4)

	require.NoError(t, c.StagePending(nil, 4, 7))

	from, to, ok := c.Pending()
	require.True(t, ok)
	require.Equal(t, int64(4), from)
	require.Equal(t, int64(7), to)

	from, to, ok = c.Commit(nil)
	require.True(t, ok)
	require.Equal(t, int64(4), from)
	require.Equal(t, int64(7), to)
	require.Equal(t, int64(7), c.Offset())

	_, _, ok = c.Pending()
	require.False(t, ok, "a committed range must not be staged twice")

	_, _, ok = c.Commit(nil)
	require.False(t, ok, "committing again must not re-record the range")
}

// The distinction this pins is the one §8.2 of the design turns on: a task that
// observed nothing still has to be recorded, so an empty range is a pending
// range, not the absence of one.
func TestCursorTreatsAnEmptyRangeAsAFact(t *testing.T) {
	c := newTestCursor(9)

	_, _, ok := c.Pending()
	require.False(t, ok, "no task in flight yet")

	require.NoError(t, c.StagePending(nil, 9, 9))

	from, to, ok := c.Pending()
	require.True(t, ok, "a task given nothing is still a task that must be recorded")
	require.Equal(t, from, to)

	from, to, ok = c.Commit(nil)
	require.True(t, ok)
	require.Equal(t, int64(9), from)
	require.Equal(t, int64(9), to)
	require.Equal(t, int64(9), c.Offset(), "an empty range must not move the cursor")
}

func TestCursorRejectsARangeBehindItself(t *testing.T) {
	c := newTestCursor(12)

	err := c.StagePending(nil, 11, 14)
	require.ErrorContains(t, err, "cursor is already at 12")

	err = c.StagePending(nil, 12, 11)
	require.ErrorContains(t, err, "precedes range start")
}

// A task that failed recorded nothing, so the range it was given never became
// history and the replacement is free to differ.
func TestCursorRedeliveryReplacesTheStagedRange(t *testing.T) {
	c := newTestCursor(2)

	require.NoError(t, c.StagePending(nil, 2, 5))
	require.NoError(t, c.StagePending(nil, 2, 9))

	from, to, ok := c.Pending()
	require.True(t, ok)
	require.Equal(t, int64(2), from)
	require.Equal(t, int64(9), to)
	require.Equal(t, int64(2), c.Offset(), "staging alone must never advance the cursor")
}

func TestCursorAbandonLeavesTheOffsetAlone(t *testing.T) {
	c := newTestCursor(3)

	require.NoError(t, c.StagePending(nil, 3, 8))
	c.Abandon(nil)

	_, _, ok := c.Pending()
	require.False(t, ok)
	require.Equal(t, int64(3), c.Offset())

	require.NoError(t, c.StagePending(nil, 3, 6), "the next delivery re-reads from the unchanged cursor")
}
