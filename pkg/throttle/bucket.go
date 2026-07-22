package throttle

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// BucketStats is a point-in-time snapshot of a Bucket.
type BucketStats struct {
	Rate         int64         // configured tokens per second (0 = unlimited)
	Burst        int64         // configured bucket capacity
	WindowedRate float64       // tokens actually consumed over the window, per second
	Waiters      int           // callers blocked right now
	WaitTotal    time.Duration // cumulative time callers spent blocked
	Utilization  float64       // WindowedRate / Rate (0 when unlimited)
}

// Bucket is a token bucket for API IOPS and global bandwidth. It is
// hand-rolled (rather than golang.org/x/time/rate) because the stats
// contract requires tracking live waiters and cumulative wait time.
//
// The initial token balance is the start argument to NewBucket (a negative
// start is treated as 0). A rate of 0 means unlimited: Wait never blocks
// (but still records consumption for WindowedRate) and Utilization is 0.
// A Wait for n > Burst is served in Burst-sized installments instead of
// failing, so a bandwidth bucket with burst below the caller's chunk size
// throttles instead of deadlocking.
type Bucket struct {
	clock clock

	rate  atomic.Int64
	burst atomic.Int64

	mu     sync.Mutex
	tokens float64
	last   time.Time     // last refill timestamp
	wake   chan struct{} // close-and-replace broadcast for SetRate (selectable sync.Cond)

	ring *windowRing // counter 0: tokens consumed

	waiters   atomic.Int64
	waitTotal atomic.Int64 // nanoseconds

	// waitCounter (hfdl.throttle.wait_seconds) is incremented with each
	// caller's blocked duration; waitAttrs is the precomputed {bucket} label.
	// nil counter = no metric. Set once before concurrent use.
	waitCounter metric.Float64Counter
	waitAttrs   metric.MeasurementOption
}

// NewBucket returns a Bucket limited to perSec tokens per second with the
// given burst capacity, pre-filled with start tokens. perSec <= 0 means
// unlimited. A limited bucket with burst < 1 is normalized to burst 1 so any
// Wait can terminate. start pre-fills the bucket (a negative start is treated
// as 0): pass burst for a bucket that may fire a full burst immediately, or 0
// for one that paces from the first request (no cold-start burst).
func NewBucket(perSec, burst, start int64) *Bucket {
	if perSec < 0 {
		perSec = 0
	}
	if perSec > 0 && burst < 1 {
		burst = 1
	}
	// A negative initial token count is nonsensical and would spuriously block
	// the first Wait; floor it at 0. No upper clamp: a start above burst is
	// trimmed to burst by refillLocked on the first Wait.
	if start < 0 {
		start = 0
	}
	b := &Bucket{
		clock: realClock{},
		wake:  make(chan struct{}),
		ring:  newWindowRing(1),
	}
	b.rate.Store(perSec)
	b.burst.Store(burst)
	b.tokens = float64(start)
	b.last = b.clock.Now()
	return b
}

// SetWaitCounter installs the hfdl.throttle.wait_seconds counter, incremented
// with each caller's blocked duration under the given bucket label
// (api|bandwidth). nil (the default) disables the metric. Call once before
// concurrent use.
func (b *Bucket) SetWaitCounter(c metric.Float64Counter, bucket string) {
	b.waitCounter = c
	b.waitAttrs = metric.WithAttributes(attribute.String("bucket", bucket))
}

// Limited reports whether the bucket currently enforces a rate (rate > 0).
// PacedTransport uses it to skip the small-chunk read cap while unlimited.
func (b *Bucket) Limited() bool { return b.rate.Load() > 0 }

// Wait blocks until n tokens are available and consumes them, or until ctx
// is done (returning ctx.Err()). n <= 0 never blocks.
func (b *Bucket) Wait(ctx context.Context, n int64) error {
	if n <= 0 {
		return nil
	}
	blocked := false
	var blockedFrom time.Time
	defer func() {
		if blocked {
			b.waiters.Add(-1)
			d := b.clock.Now().Sub(blockedFrom)
			b.waitTotal.Add(int64(d))
			if b.waitCounter != nil {
				// Record even on a cancelled Wait: the caller really did block
				// for d, so the cumulative wait must reflect it.
				b.waitCounter.Add(ctx, d.Seconds(), b.waitAttrs)
			}
		}
	}()

	remaining := n
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.mu.Lock()
		rate := b.rate.Load()
		now := b.clock.Now()
		if rate == 0 { // unlimited: never blocks
			b.ring.Add(now, 0, remaining)
			b.mu.Unlock()
			return nil
		}
		b.refillLocked(now, rate)
		take := remaining
		if br := b.burst.Load(); take > br {
			take = br
		}
		if b.tokens >= float64(take) {
			b.tokens -= float64(take)
			remaining -= take
			// Record each installment as it is consumed, not only on full
			// completion: a Wait cancelled mid-loop has already spent these
			// tokens, so they must show up in the windowed rate (and not leak).
			b.ring.Add(now, 0, take)
			b.mu.Unlock()
			if remaining == 0 {
				return nil
			}
			continue
		}
		deficit := float64(take) - b.tokens
		waitDur := time.Duration(deficit / float64(rate) * float64(time.Second))
		if waitDur <= 0 {
			waitDur = time.Millisecond // floor: avoid a busy spin on sub-tick deficits
		}
		ch := b.wake
		b.mu.Unlock()

		if !blocked {
			blocked = true
			blockedFrom = now
			b.waiters.Add(1)
		}

		// Go 1.23 automatically manages OS timer resolution on Windows for us.
		// https://go-review.googlesource.com/c/website/+/614435
		t := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-ch: // SetRate broadcast: re-evaluate against the new rate
			t.Stop()
		case <-t.C:
		}
	}
	return nil
}

// SetRate hot-updates the refill rate and burst capacity, waking every
// blocked waiter so they re-evaluate against the new configuration.
// perSec <= 0 switches to unlimited.
func (b *Bucket) SetRate(perSec, burst int64) {
	if perSec < 0 {
		perSec = 0
	}
	if perSec > 0 && burst < 1 {
		burst = 1
	}
	b.mu.Lock()
	b.refillLocked(b.clock.Now(), b.rate.Load())
	b.rate.Store(perSec)
	b.burst.Store(burst)
	if perSec > 0 {
		// Clamp tokens into [0, burst]. Switching from unlimited (where a
		// bucket built with burst < 1 can hold zero or negative tokens) to a
		// limited rate must not leave a negative balance that spuriously
		// blocks the next Wait, nor a balance above the new burst.
		if b.tokens > float64(burst) {
			b.tokens = float64(burst)
		}
		if b.tokens < 0 {
			b.tokens = 0
		}
	}
	close(b.wake)
	b.wake = make(chan struct{})
	b.mu.Unlock()
}

// Stats snapshots the bucket. WindowedRate is the 10s moving average of
// consumed tokens per second; it can transiently exceed Rate right after a
// burst drain, so Utilization may briefly pass 1.
func (b *Bucket) Stats() BucketStats {
	rate := b.rate.Load()
	wr := float64(b.ring.Sum(b.clock.Now(), 0)) / windowSecs
	var util float64
	if rate > 0 {
		util = wr / float64(rate)
	}
	return BucketStats{
		Rate:         rate,
		Burst:        b.burst.Load(),
		WindowedRate: wr,
		Waiters:      int(b.waiters.Load()),
		WaitTotal:    time.Duration(b.waitTotal.Load()),
		Utilization:  util,
	}
}

// refillLocked accrues tokens for the elapsed wall time, capped at burst.
func (b *Bucket) refillLocked(now time.Time, rate int64) {
	if rate <= 0 {
		b.last = now
		return
	}
	el := now.Sub(b.last)
	if el <= 0 {
		return
	}
	b.last = now
	b.tokens += float64(rate) * el.Seconds()
	if cap := float64(b.burst.Load()); b.tokens > cap {
		b.tokens = cap
	}
}
