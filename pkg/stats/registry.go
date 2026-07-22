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
	ema   atomic.Uint64 // math.Float64bits of the per-file EMA rate estimate (α=0.2)

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
	ring  rateRing // per-upstream windowed byte rate (fed by AddUpstream)
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
	globalEMA     atomic.Uint64 // math.Float64bits of the global EMA rate estimate (α=0.2)

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

// updateFile keeps the entry reachable for the whole update. RemoveFile's
// write lock therefore orders deletion after every update that found or
// created the entry.
func (r *Registry) updateFile(fileID int64, update func(*fileEntry)) {
	r.filesMu.RLock()
	f := r.files[fileID]
	if f != nil {
		update(f)
		r.filesMu.RUnlock()
		return
	}
	r.filesMu.RUnlock()

	r.filesMu.Lock()
	f = r.files[fileID]
	if f == nil {
		f = &fileEntry{live: make(map[string]int64)}
		r.files[fileID] = f
	}
	update(f)
	r.filesMu.Unlock()
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
	r.updateFile(fileID, func(f *fileEntry) {
		f.done.Add(n)
		f.ring.Add(r.nowSec(), n)
	})
}

// AddUpstream records n bytes served by upstream and feeds its windowed rate
// ring.
func (r *Registry) AddUpstream(upstream string, n int64) {
	u := r.upstream(upstream)
	u.bytes.Add(n)
	u.ring.Add(r.nowSec(), n)
}

// foldEMA folds a fresh rate sample into an EMA held as Float64bits
// (ema = α·sample + (1-α)·ema, α=0.2). CAS loop, no lock.
func foldEMA(a *atomic.Uint64, sample float64) {
	for {
		old := a.Load()
		next := emaAlpha*sample + (1-emaAlpha)*math.Float64frombits(old)
		if a.CompareAndSwap(old, math.Float64bits(next)) {
			return
		}
	}
}

// ReportUpstreamRate folds a fresh rate sample into the upstream's EMA and the
// process-global EMA (both α=0.2). The upstream EMA is the upstream-policy
// signal; the global EMA is the smoothed whole-run rate signal.
func (r *Registry) ReportUpstreamRate(upstream string, bps float64) {
	foldEMA(&r.upstream(upstream).ema, bps)
	foldEMA(&r.globalEMA, bps)
}

// ReportFileRate folds a fresh whole-file rate sample into that file's EMA
// (α=0.2). Callers with a per-file rate measurement (transfer's
// reportRate) should call this so the per-file EMA is populated alongside the
// per-upstream one.
func (r *Registry) ReportFileRate(fileID int64, bps float64) {
	r.updateFile(fileID, func(f *fileEntry) { foldEMA(&f.ema, bps) })
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
	r.updateFile(fileID, func(f *fileEntry) {
		r.conns.Add(1)
		f.conns.Add(1)
		f.liveMu.Lock()
		f.live[upstream]++
		f.liveMu.Unlock()
		r.upstream(upstream).conns.Add(1)
	})
}

// subClamp atomically subtracts delta from a, never dropping below 0, so an
// unpaired or duplicate decrement can't drive a gauge negative.
func subClamp(a *atomic.Int64, delta int64) {
	if delta <= 0 {
		return
	}
	for {
		v := a.Load()
		nv := max(v-delta, 0)
		if a.CompareAndSwap(v, nv) {
			return
		}
	}
}

// ConnEnd closes a connection opened by ConnStart. Pairing is anchored on the
// file's live-upstream map: an end is honored only when the file entry still
// has a live connection for that upstream, so a duplicate/unpaired end (or one
// whose file was already removed and reconciled) is dropped rather than
// driving the global and upstream gauges negative.
func (r *Registry) ConnEnd(fileID int64, upstream string) {
	r.filesMu.RLock()
	f := r.files[fileID]
	if f == nil {
		r.filesMu.RUnlock()
		return // file removed (already reconciled) or never started: drop
	}
	f.liveMu.Lock()
	n := f.live[upstream]
	if n <= 0 {
		f.liveMu.Unlock()
		r.filesMu.RUnlock()
		return // unpaired end for this upstream: drop
	}
	if n == 1 {
		delete(f.live, upstream)
	} else {
		f.live[upstream] = n - 1
	}
	f.liveMu.Unlock()

	subClamp(&f.conns, 1)
	subClamp(&r.conns, 1)
	if u := r.upstreamIfExists(upstream); u != nil {
		subClamp(&u.conns, 1)
	}
	r.filesMu.RUnlock()
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
	r.updateFile(fileID, func(f *fileEntry) { f.total.Store(total) })
}

// RemoveFile drops fileID's entry; the file finished and Snapshot should no
// longer show it. Late adds for the same ID recreate a fresh entry.
//
// Connections still live on the removed file are reconciled here: their paired
// ConnEnd (if it ever arrives) will find no entry and be dropped, so the
// global and per-upstream gauges are decremented now by the removed entry's
// live conn count — otherwise removing a file mid-transfer would strand those
// gauges high forever.
func (r *Registry) RemoveFile(fileID int64) {
	r.filesMu.Lock()
	f := r.files[fileID]
	delete(r.files, fileID)
	r.filesMu.Unlock()
	if f == nil {
		return
	}
	// Snapshot and clear the live map under its own lock so a ConnEnd racing
	// the removal (holding the same f) either decrements before we clear it or
	// finds it empty and drops — never both.
	f.liveMu.Lock()
	var total int64
	for u, n := range f.live {
		total += n
		if up := r.upstreamIfExists(u); up != nil {
			subClamp(&up.conns, n)
		}
	}
	f.live = map[string]int64{}
	f.liveMu.Unlock()
	subClamp(&r.conns, total)
}
