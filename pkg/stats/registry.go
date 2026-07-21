package stats

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// emaAlpha is the EMA weight for fresh upstream rate samples.
	emaAlpha = 0.2
	// penalizeFactor scales an upstream's EMA down on PenalizeUpstream.
	penalizeFactor = 0.5
)

// fileEntry holds the live counters for one in-flight file. Removed from the
// registry by RemoveFile once the file finishes, so Snapshot only ever shows
// active files.
type fileEntry struct {
	done  atomic.Int64
	total atomic.Int64
	conns atomic.Int64
	ring  rateRing

	// live counts open connections per upstream on this file; an upstream
	// stays listed until its last connection on the file ends.
	liveMu sync.Mutex
	live   map[string]int64
}

// upstreamEntry holds the live counters for one upstream endpoint.
type upstreamEntry struct {
	bytes atomic.Int64
	ema   atomic.Uint64 // math.Float64bits of the EMA rate estimate
	conns atomic.Int64
}

// Registry is the process-wide statistics hub. Counters are atomic; the
// file/upstream maps are guarded by RWMutex and only ever hold pointers to
// atomically-updated entries, so hot-path adds take the read lock.
type Registry struct {
	// clock supplies ring windowing; injectable so tests are deterministic.
	clock func() time.Time

	totalNetwork  atomic.Int64
	totalSalvaged atomic.Int64
	conns         atomic.Int64
	stalls        atomic.Int64
	retries       atomic.Int64
	globalRing    rateRing

	filesMu   sync.RWMutex
	files     map[int64]*fileEntry
	upsMu     sync.RWMutex
	upstreams map[string]*upstreamEntry
}

// New returns a Registry using the wall clock.
func New() *Registry { return NewWithClock(time.Now) }

// NewWithClock returns a Registry whose ring windowing is driven by clock.
// Tests use it to make windowed rates deterministic.
func NewWithClock(clock func() time.Time) *Registry {
	return &Registry{
		clock:     clock,
		files:     make(map[int64]*fileEntry),
		upstreams: make(map[string]*upstreamEntry),
	}
}

func (r *Registry) nowSec() int64 { return r.clock().Unix() }

// file returns the entry for fileID, creating it on first use.
func (r *Registry) file(fileID int64) *fileEntry {
	if f := r.fileIfExists(fileID); f != nil {
		return f
	}
	r.filesMu.Lock()
	f := r.files[fileID]
	if f == nil {
		f = &fileEntry{live: make(map[string]int64)}
		r.files[fileID] = f
	}
	r.filesMu.Unlock()
	return f
}

func (r *Registry) fileIfExists(fileID int64) *fileEntry {
	r.filesMu.RLock()
	f := r.files[fileID]
	r.filesMu.RUnlock()
	return f
}

// upstream returns the entry for upstream, creating it on first use.
func (r *Registry) upstream(name string) *upstreamEntry {
	if u := r.upstreamIfExists(name); u != nil {
		return u
	}
	r.upsMu.Lock()
	u := r.upstreams[name]
	if u == nil {
		u = &upstreamEntry{}
		r.upstreams[name] = u
	}
	r.upsMu.Unlock()
	return u
}

func (r *Registry) upstreamIfExists(name string) *upstreamEntry {
	r.upsMu.RLock()
	u := r.upstreams[name]
	r.upsMu.RUnlock()
	return u
}

// AddNetwork records n globally downloaded bytes and feeds the global
// windowed rate ring.
func (r *Registry) AddNetwork(n int64) {
	r.totalNetwork.Add(n)
	r.globalRing.Add(r.nowSec(), n)
}

// AddFile records n downloaded bytes for fileID and feeds its rate ring.
func (r *Registry) AddFile(fileID int64, n int64) {
	f := r.file(fileID)
	f.done.Add(n)
	f.ring.Add(r.nowSec(), n)
}

// AddUpstream records n bytes served by upstream.
func (r *Registry) AddUpstream(upstream string, n int64) {
	r.upstream(upstream).bytes.Add(n)
}

// ReportUpstreamRate folds a fresh rate sample into the upstream's EMA
// (ema = α·sample + (1-α)·ema, α=0.2). CAS loop, no lock.
func (r *Registry) ReportUpstreamRate(upstream string, bps float64) {
	u := r.upstream(upstream)
	for {
		old := u.ema.Load()
		next := emaAlpha*bps + (1-emaAlpha)*math.Float64frombits(old)
		if u.ema.CompareAndSwap(old, math.Float64bits(next)) {
			return
		}
	}
}

// PenalizeUpstream halves the upstream's EMA (e.g. on stall/throttle).
func (r *Registry) PenalizeUpstream(upstream string) {
	u := r.upstream(upstream)
	for {
		old := u.ema.Load()
		next := math.Float64frombits(old) * penalizeFactor
		if u.ema.CompareAndSwap(old, math.Float64bits(next)) {
			return
		}
	}
}

// ConnStart opens a connection: bumps the global gauge, the file's gauge,
// the upstream's gauge, and pins upstream in the file's live-upstream set.
func (r *Registry) ConnStart(fileID int64, upstream string) {
	r.conns.Add(1)
	f := r.file(fileID)
	f.conns.Add(1)
	f.liveMu.Lock()
	f.live[upstream]++
	f.liveMu.Unlock()
	r.upstream(upstream).conns.Add(1)
}

// ConnEnd closes a connection opened by ConnStart. Calls must be paired per
// (fileID, upstream); a ConnEnd for an unknown file or upstream is dropped
// rather than recreating a removed entry.
func (r *Registry) ConnEnd(fileID int64, upstream string) {
	r.conns.Add(-1)
	if f := r.fileIfExists(fileID); f != nil {
		f.conns.Add(-1)
		f.liveMu.Lock()
		if n := f.live[upstream] - 1; n > 0 {
			f.live[upstream] = n
		} else {
			delete(f.live, upstream)
		}
		f.liveMu.Unlock()
	}
	if u := r.upstreamIfExists(upstream); u != nil {
		u.conns.Add(-1)
	}
}

// AddStall tallies a stall blamed on upstream.
func (r *Registry) AddStall(upstream string) { r.stalls.Add(1) }

// AddRetry tallies a retry blamed on upstream.
func (r *Registry) AddRetry(upstream string) { r.retries.Add(1) }

// AddSalvaged records n bytes recovered from local reference files instead
// of the network.
func (r *Registry) AddSalvaged(n int64) { r.totalSalvaged.Add(n) }

// SetFileTotal sets the expected size of fileID for done/total progress.
func (r *Registry) SetFileTotal(fileID int64, total int64) {
	r.file(fileID).total.Store(total)
}

// RemoveFile drops fileID's entry; the file finished and Snapshot should no
// longer show it. Late adds for the same ID recreate a fresh entry.
func (r *Registry) RemoveFile(fileID int64) {
	r.filesMu.Lock()
	delete(r.files, fileID)
	r.filesMu.Unlock()
}
