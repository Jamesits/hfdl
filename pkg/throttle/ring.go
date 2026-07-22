package throttle

import (
	"math"
	"sync"
	"time"
)

// windowSecs is the stats window length; also the worker-liveness TTL for
// DutyLimiter stats (a worker silent for a whole window is considered gone).
const windowSecs = 10

// windowRing is a fixed ring of per-second counters, advanced lazily without
// a background goroutine. Each slot is locked while its epoch and counters
// are read or changed, so rollover cannot lose or misattribute concurrent
// writes.
type windowRing struct {
	slots [windowSecs]struct {
		mu    sync.Mutex
		epoch int64 // unix second this slot currently counts
		vals  []int64
	}
}

// newWindowRing allocates a ring with the given number of counters per slot.
func newWindowRing(counters int) *windowRing {
	r := &windowRing{}
	for i := range r.slots {
		r.slots[i].epoch = math.MinInt64 // never inside any window
		r.slots[i].vals = make([]int64, counters)
	}
	return r
}

// Add records v into counter idx of t's second slot.
func (r *windowRing) Add(t time.Time, idx int, v int64) {
	ep := t.Unix()
	s := &r.slots[ep%windowSecs]
	s.mu.Lock()
	if s.epoch != ep {
		s.epoch = ep
		clear(s.vals)
	}
	s.vals[idx] += v
	s.mu.Unlock()
}

// Sum totals counter idx over the window (now-windowSecs, now], by wall
// second of t.
func (r *windowRing) Sum(t time.Time, idx int) int64 {
	now := t.Unix()
	var total int64
	for i := range r.slots {
		s := &r.slots[i]
		s.mu.Lock()
		ep := s.epoch
		if ep <= now && ep > now-windowSecs {
			total += s.vals[idx]
		}
		s.mu.Unlock()
	}
	return total
}
