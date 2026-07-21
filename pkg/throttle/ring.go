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
// on read (no background goroutine). A write racing a second-rollover of its
// slot is *dropped*, never mis-attributed to the new second: Add re-checks the
// slot's epoch after recording and backs its increment out if the slot rolled
// underneath it. Dropping a boundary sample is acceptable for a UI/metrics
// stat and avoids paying for a lock on the hot path.
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
// rollover. It guards against a concurrent rollover zeroing the slot between
// the epoch check and the Add — which would otherwise mis-attribute v to the
// new second: after adding, it re-reads the epoch and, if the slot no longer
// owns ep, backs the increment out and re-decides (a stale second is dropped;
// a backwards clock reclaims the slot).
func (r *windowRing) Add(t time.Time, idx int, v int64) {
	ep := t.Unix()
	s := &r.slots[ep%windowSecs]
	for {
		old := s.epoch.Load()
		switch {
		case old == ep:
			// slot already owns our second
		case old < ep:
			if !s.epoch.CompareAndSwap(old, ep) {
				continue // lost the rollover CAS; re-read and retry
			}
			for i := range s.vals {
				s.vals[i].Store(0)
			}
		default: // old > ep: our second already rolled out of this slot; drop.
			return
		}
		s.vals[idx].Add(v)
		if s.epoch.Load() == ep {
			return
		}
		// A concurrent rollover zeroed the slot after our check: undo and
		// re-decide against the slot's new epoch.
		s.vals[idx].Add(-v)
	}
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
