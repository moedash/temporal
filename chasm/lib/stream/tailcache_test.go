package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func blob(s string) *commonpb.DataBlob {
	return &commonpb.DataBlob{Data: []byte(s)}
}

func TestTailCacheServesAContiguousRange(t *testing.T) {
	c := newTailCache(1024, 8)
	c.put("s", 0, 3, blob("a"))
	c.put("s", 3, 5, blob("b"))

	blobs, starts, ok := c.get("s", 0, 5)
	require.True(t, ok)
	require.Len(t, blobs, 2)
	require.Equal(t, []int64{0, 3}, starts)

	// A read starting inside a batch still needs the batch that contains it.
	blobs, starts, ok = c.get("s", 1, 5)
	require.True(t, ok)
	require.Len(t, blobs, 2)
	require.Equal(t, []int64{0, 3}, starts)
}

func TestTailCacheMissesRatherThanReturningAPrefix(t *testing.T) {
	c := newTailCache(1024, 8)
	c.put("s", 3, 5, blob("b"))

	// Offsets 0..2 were never cached. Returning just the tail would look like a
	// short read to the caller, which is the shape of a silent data loss.
	_, _, ok := c.get("s", 0, 5)
	require.False(t, ok)

	_, _, ok = c.get("s", 3, 5)
	require.True(t, ok)
}

func TestTailCacheMissesPastTheCachedTail(t *testing.T) {
	c := newTailCache(1024, 8)
	c.put("s", 0, 2, blob("a"))

	_, _, ok := c.get("s", 0, 5)
	require.False(t, ok, "the cache must not claim a range it only partly holds")
}

func TestTailCacheEvictsByBytes(t *testing.T) {
	// Room for roughly two entries.
	c := newTailCache(4, 8)
	c.put("s", 0, 1, blob("aa"))
	c.put("s", 1, 2, blob("bb"))
	c.put("s", 2, 3, blob("cc"))

	_, _, ok := c.get("s", 0, 3)
	require.False(t, ok, "the oldest entry should have been evicted")

	_, _, ok = c.get("s", 1, 3)
	require.True(t, ok)
}

func TestTailCacheEvictsWholeStreams(t *testing.T) {
	c := newTailCache(1024, 2)
	c.put("a", 0, 1, blob("x"))
	c.put("b", 0, 1, blob("y"))
	c.put("c", 0, 1, blob("z"))

	_, _, ok := c.get("a", 0, 1)
	require.False(t, ok)
	_, _, ok = c.get("c", 0, 1)
	require.True(t, ok)
}

func TestTailCacheUnknownStreamMisses(t *testing.T) {
	c := newTailCache(1024, 8)
	_, _, ok := c.get("nope", 0, 1)
	require.False(t, ok)

	hits, misses := c.stats()
	require.Zero(t, hits)
	require.Equal(t, int64(1), misses)
}
