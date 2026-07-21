package throttle

import (
	"math"
	"sync/atomic"
	"time"
)

// windowSecs is the stats window length; also the worker-liveness TTL for
// DutyLimiter stats (a worker silent for a whole window is considered gone).
const windowSecs = 10

// windowRing is a fixed ring of per-second atomic counters, lazily advanced
// on read (no background goroutine). Writes landing exactly on a second
// rollover race the slot reset and may be dropped; that is accepted for a
// UI/metrics stat and documented here rather than paid for with a lock on
// the hot path.
type windowRing struct {
	slots [windowSecs]struct {
		epoch atomic.Int64 // unix second this slot currently counts
		vals  []atomic.Int64
	}
}

// newWindowRing allocates a ring with the given number of counters per slot.
func newWindowRing(counters int) *windowRing {
	r := &windowRing{}
	for i := range r.slots {
		r.slots[i].epoch.Store(math.MinInt64) // never inside any window
		r.slots[i].vals = make([]atomic.Int64, counters)
	}
	return r
}

// Add records v into counter idx of t's second slot, claiming the slot on
// rollover.
func (r *windowRing) Add(t time.Time, idx int, v int64) {
	ep := t.Unix()
	s := &r.slots[ep%windowSecs]
	if old := s.epoch.Load(); old != ep {
		if s.epoch.CompareAndSwap(old, ep) {
			for i := range s.vals {
				s.vals[i].Store(0)
			}
		}
	}
	s.vals[idx].Add(v)
}

// Sum totals counter idx over the window (now-windowSecs, now], by wall
// second of t.
func (r *windowRing) Sum(t time.Time, idx int) int64 {
	now := t.Unix()
	var total int64
	for i := range r.slots {
		ep := r.slots[i].epoch.Load()
		if ep <= now && ep > now-windowSecs {
			total += r.slots[i].vals[idx].Load()
		}
	}
	return total
}
