package stats

import "sync"

// windowSeconds is the sliding-window depth for all rate rings: a 10s
// per-second ring, the same construction the throttle stats use. Rates are
// bytes summed
// over the window divided by the window length; seconds with no traffic
// contribute empty buckets.
const windowSeconds = 10

// rateRing is a ring of per-second byte counters, one bucket per second,
// advanced lazily by whichever goroutine touches it next (no background
// goroutine). A short mutex serializes rotation against adds; the critical
// section is O(1) except on second rollover, where it zeroes at most
// windowSeconds buckets.
type rateRing struct {
	mu      sync.Mutex
	buckets [windowSeconds]int64
	curSec  int64
}

// rotate zeroes buckets for seconds that have fully elapsed since the last
// touch. Callers hold r.mu. A clock that moved backwards attributes traffic
// to the current bucket rather than rotating backwards.
func (r *rateRing) rotate(nowSec int64) {
	if nowSec <= r.curSec {
		return
	}
	delta := min(nowSec-r.curSec, windowSeconds)
	for k := int64(1); k <= delta; k++ {
		r.buckets[(r.curSec+k)%windowSeconds] = 0
	}
	r.curSec = nowSec
}

// Add attributes n bytes to the bucket for nowSec.
func (r *rateRing) Add(nowSec, n int64) {
	r.mu.Lock()
	r.rotate(nowSec)
	r.buckets[r.curSec%windowSeconds] += n
	r.mu.Unlock()
}

// Rate returns the windowed rate in bytes/second: bytes recorded in the
// window divided by the window length.
func (r *rateRing) Rate(nowSec int64) float64 {
	r.mu.Lock()
	r.rotate(nowSec)
	var sum int64
	for _, b := range r.buckets {
		sum += b
	}
	r.mu.Unlock()
	return float64(sum) / windowSeconds
}
