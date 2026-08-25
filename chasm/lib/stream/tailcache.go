package stream

import (
	"sync"

	commonpb "go.temporal.io/api/common/v1"
)

// TailCache keeps the most recently appended batches in memory so a reader at
// the tail is served without touching the database. That is what makes fan-out
// cheap: N readers at the tail cost N copies rather than N range scans, which
// is the difference between a subscriber ceiling and no meaningful limit.
//
// Only the bytes are cached. The frontier always comes from the component, so
// the cache can never widen what a reader is allowed to see. Entries are safe
// to hold indefinitely because an offset's content is immutable once its append
// commits, and nothing is cached before the commit that made it visible.
type TailCache struct {
	mu sync.Mutex

	bytesPerStream int
	maxStreams     int
	streams        map[string]*tailRing
	// Insertion order of stream keys, used to evict whole rings when the cache
	// is tracking more streams than it is allowed to.
	order []string

	hits   int64
	misses int64
}

type tailEntry struct {
	startOffset int64
	nextOffset  int64
	blob        *commonpb.DataBlob
}

type tailRing struct {
	entries []tailEntry
	bytes   int
}

func NewTailCache(bytesPerStream, maxStreams int) *TailCache {
	return &TailCache{
		bytesPerStream: bytesPerStream,
		maxStreams:     maxStreams,
		streams:        make(map[string]*tailRing),
	}
}

func (c *TailCache) Put(key string, startOffset, nextOffset int64, blob *commonpb.DataBlob) {
	if c == nil || blob == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	ring, ok := c.streams[key]
	if !ok {
		ring = &tailRing{}
		c.streams[key] = ring
		c.order = append(c.order, key)
		c.evictStreamsLocked()
	}

	ring.entries = append(ring.entries, tailEntry{
		startOffset: startOffset,
		nextOffset:  nextOffset,
		blob:        blob,
	})
	ring.bytes += len(blob.GetData())

	for len(ring.entries) > 1 && ring.bytes > c.bytesPerStream {
		ring.bytes -= len(ring.entries[0].blob.GetData())
		ring.entries = ring.entries[1:]
	}
}

func (c *TailCache) evictStreamsLocked() {
	for len(c.order) > c.maxStreams {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.streams, oldest)
	}
}

// Get returns the batches covering [from, to) when the cache holds all of them,
// and reports false otherwise. A partial hit is treated as a miss: stitching
// cached and stored batches together would be a second read path to get wrong,
// for a case the database already handles.
func (c *TailCache) Get(key string, from, to int64) ([]*commonpb.DataBlob, []int64, bool) {
	if c == nil || from >= to {
		return nil, nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	ring, ok := c.streams[key]
	if !ok || len(ring.entries) == 0 {
		c.misses++
		return nil, nil, false
	}

	var blobs []*commonpb.DataBlob
	var starts []int64
	cursor := from
	for _, e := range ring.entries {
		if e.nextOffset <= cursor {
			continue
		}
		if e.startOffset > cursor {
			// A gap before the range we need, so the cache does not hold it.
			c.misses++
			return nil, nil, false
		}
		blobs = append(blobs, e.blob)
		starts = append(starts, e.startOffset)
		cursor = e.nextOffset
		if cursor >= to {
			break
		}
	}
	if cursor < to {
		c.misses++
		return nil, nil, false
	}
	c.hits++
	return blobs, starts, true
}

func (c *TailCache) Stats() (hits, misses int64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses
}
