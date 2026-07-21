package xet

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// chunkCache is the on-disk xorb chunk cache. Keys are
// (xorbHash, chunkStart, chunkEnd) — the same key shape hf_xet uses
// (xet_client/src/chunk_cache/mod.rs ChunkCache::get/put). Where hf_xet
// stores *deserialized* chunk bytes, this cache stores the fetched
// serialized ranges verbatim (contract), which keeps cached content valid
// for any later sub-range decode of the same authorized range.
//
// Layout: <dir>/<xorbHash>/<start>-<end> holding the raw serialized range.
// LRU metadata lives in memory, rebuilt by scanning the dir at open; the
// monotonic access counter is seeded from file mtimes so cold-start
// eviction order approximates real recency. A single mutex guards metadata
// and file IO: cache traffic is block-sized and infrequent relative to
// decode work, and the lock makes eviction-vs-read races impossible.
//
// The cache is best-effort: any IO inconsistency degrades to a miss (or a
// dropped put), never to a failed download.

type cacheKey struct {
	xorb       string
	start, end uint32
}

type cacheEntry struct {
	size  int64
	atime int64 // LRU clock value of last access
}

type chunkCache struct {
	dir      string
	maxBytes int64

	mu      sync.Mutex
	entries map[cacheKey]*cacheEntry
	total   int64
	clock   int64
}

func openChunkCache(dir string, maxBytes int64) (*chunkCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("xet: create chunk cache dir: %w", err)
	}
	c := &chunkCache{
		dir:      dir,
		maxBytes: maxBytes,
		entries:  make(map[cacheKey]*cacheEntry),
	}
	// Rebuild the index. Unknown files are ignored (never deleted): the dir
	// may be shared with hf_xet itself.
	type scanned struct {
		key   cacheKey
		size  int64
		mtime time.Time
	}
	var found []scanned
	xorbs, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("xet: scan chunk cache: %w", err)
	}
	for _, xe := range xorbs {
		if !xe.IsDir() || !isHexHash(xe.Name()) {
			continue
		}
		files, err := os.ReadDir(filepath.Join(dir, xe.Name()))
		if err != nil {
			continue
		}
		for _, fe := range files {
			if fe.IsDir() {
				continue
			}
			start, end, ok := parseRangeFileName(fe.Name())
			if !ok {
				continue
			}
			info, err := fe.Info()
			if err != nil {
				continue
			}
			found = append(found, scanned{
				key:   cacheKey{xorb: xe.Name(), start: start, end: end},
				size:  info.Size(),
				mtime: info.ModTime(),
			})
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].mtime.Before(found[j].mtime) })
	for _, f := range found {
		c.clock++
		c.entries[f.key] = &cacheEntry{size: f.size, atime: c.clock}
		c.total += f.size
	}
	// A previous run may have exceeded the cap (or the cap shrank).
	c.evictLocked()
	return c, nil
}

func isHexHash(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

func parseRangeFileName(name string) (start, end uint32, ok bool) {
	a, b, found := strings.Cut(name, "-")
	if !found {
		return 0, 0, false
	}
	s, err1 := strconv.ParseUint(a, 10, 32)
	e, err2 := strconv.ParseUint(b, 10, 32)
	if err1 != nil || err2 != nil || s >= e {
		return 0, 0, false
	}
	return uint32(s), uint32(e), true
}

func (c *chunkCache) path(key cacheKey) string {
	return filepath.Join(c.dir, key.xorb, fmt.Sprintf("%d-%d", key.start, key.end))
}

// Get returns the serialized range for key, refreshing its LRU position.
// Any read error is a miss; the broken entry is dropped.
func (c *chunkCache) Get(key cacheKey) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	b, err := os.ReadFile(c.path(key))
	if err != nil || int64(len(b)) != e.size {
		delete(c.entries, key)
		c.total -= e.size
		_ = os.Remove(c.path(key))
		return nil, false
	}
	c.clock++
	e.atime = c.clock
	return b, true
}

// Put stores data under key (atomic temp+rename) and evicts LRU entries
// until the cache is back under its cap. Put failures are swallowed:
// caching is opportunistic.
func (c *chunkCache) Put(key cacheKey, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return
	}
	if err := os.Rename(tmpName, p); err != nil {
		_ = os.Remove(tmpName)
		return
	}
	if old, ok := c.entries[key]; ok {
		c.total -= old.size
	}
	c.clock++
	c.entries[key] = &cacheEntry{size: int64(len(data)), atime: c.clock}
	c.total += int64(len(data))
	c.evictLocked()
}

// evictLocked drops least-recently-used entries until total <= maxBytes.
func (c *chunkCache) evictLocked() {
	for c.total > c.maxBytes && len(c.entries) > 0 {
		var oldest cacheKey
		var oldestAtime int64 = -1
		for k, e := range c.entries {
			if oldestAtime < 0 || e.atime < oldestAtime {
				oldest, oldestAtime = k, e.atime
			}
		}
		e := c.entries[oldest]
		if err := os.Remove(c.path(oldest)); err == nil || os.IsNotExist(err) {
			delete(c.entries, oldest)
			c.total -= e.size
		} else {
			// Un-removable file: forget the entry anyway or we spin forever.
			delete(c.entries, oldest)
			c.total -= e.size
		}
	}
}

// stats reports (entries, totalBytes); used by tests.
func (c *chunkCache) stats() (int, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries), c.total
}
