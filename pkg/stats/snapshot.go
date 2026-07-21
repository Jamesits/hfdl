package stats

import (
	"math"
	"sort"
)

// FileStat is a point-in-time copy of one active file's counters.
type FileStat struct {
	Done      int64
	Total     int64
	Rate      float64  // windowed B/s
	Conns     int      // live connections on this file
	Upstreams []string // upstreams with at least one live connection, sorted
}

// UpstreamStat is a point-in-time copy of one upstream's counters.
type UpstreamStat struct {
	Bytes  int64
	EMABps float64 // EMA rate estimate (α=0.2)
	Conns  int     // live connections
}

// Snapshot is a deep copy of the registry for the TUI (4Hz poll) and OTel
// metric callbacks. Mutating it never affects the registry.
type Snapshot struct {
	TotalNetwork  int64
	TotalSalvaged int64
	GlobalRate    float64 // windowed B/s over the 10s ring
	Files         map[int64]FileStat
	Upstreams     map[string]UpstreamStat
	Conns         int
	Stalls        int64
	Retries       int64
}

// Snapshot deep-copies the registry: fresh maps and slices throughout.
// Entry pointers are stable, so only the (short) map read locks are taken;
// hot-path adds hold the same read lock and are never queued behind writers
// here.
func (r *Registry) Snapshot() *Snapshot {
	now := r.nowSec()
	s := &Snapshot{
		TotalNetwork:  r.totalNetwork.Load(),
		TotalSalvaged: r.totalSalvaged.Load(),
		GlobalRate:    r.globalRing.Rate(now),
		Files:         make(map[int64]FileStat),
		Upstreams:     make(map[string]UpstreamStat),
		Conns:         int(r.conns.Load()),
		Stalls:        r.stalls.Load(),
		Retries:       r.retries.Load(),
	}

	r.filesMu.RLock()
	for id, f := range r.files {
		f.liveMu.Lock()
		ups := make([]string, 0, len(f.live))
		for u := range f.live {
			ups = append(ups, u)
		}
		f.liveMu.Unlock()
		sort.Strings(ups)
		s.Files[id] = FileStat{
			Done:      f.done.Load(),
			Total:     f.total.Load(),
			Rate:      f.ring.Rate(now),
			Conns:     int(f.conns.Load()),
			Upstreams: ups,
		}
	}
	r.filesMu.RUnlock()

	r.upsMu.RLock()
	for name, u := range r.upstreams {
		s.Upstreams[name] = UpstreamStat{
			Bytes:  u.bytes.Load(),
			EMABps: math.Float64frombits(u.ema.Load()),
			Conns:  int(u.conns.Load()),
		}
	}
	r.upsMu.RUnlock()

	return s
}
