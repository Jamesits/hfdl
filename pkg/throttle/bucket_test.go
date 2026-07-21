package throttle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", d, msg)
}

func TestBucketUnlimitedNeverBlocks(t *testing.T) {
	b := NewBucket(0, 0)
	start := time.Now()
	const total = int64(1 << 20)
	for range 1000 {
		if err := b.Wait(t.Context(), total); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el > 100*time.Millisecond {
		t.Fatalf("unlimited bucket blocked: %v", el)
	}
	s := b.Stats()
	if s.Rate != 0 || s.Utilization != 0 {
		t.Fatalf("unlimited stats: rate=%d util=%v", s.Rate, s.Utilization)
	}
	if s.Waiters != 0 || s.WaitTotal != 0 {
		t.Fatalf("unlimited bucket recorded blocking: %+v", s)
	}
	// consumption is still accounted for the windowed rate
	want := float64(1000*total) / windowSecs
	if s.WindowedRate != want {
		t.Fatalf("WindowedRate = %v, want %v", s.WindowedRate, want)
	}
}

func TestBucketRateLimitedThroughput(t *testing.T) {
	// 1000 tokens/s, burst 100 (starts full): consuming 500 tokens waits
	// for 400 of them -> ~400ms.
	b := NewBucket(1000, 100)
	start := time.Now()
	for range 10 {
		if err := b.Wait(t.Context(), 50); err != nil {
			t.Fatal(err)
		}
	}
	el := time.Since(start)
	if el < 300*time.Millisecond {
		t.Fatalf("too fast (%v): burst/rate not enforced", el)
	}
	if el > 2*time.Second {
		t.Fatalf("too slow (%v): over-throttled", el)
	}
}

func TestBucketBurstHonored(t *testing.T) {
	b := NewBucket(100, 50)
	start := time.Now()
	if err := b.Wait(t.Context(), 50); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > 50*time.Millisecond {
		t.Fatalf("initial burst not served immediately: %v", el)
	}
	start = time.Now()
	if err := b.Wait(t.Context(), 50); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 350*time.Millisecond {
		t.Fatalf("post-burst wait too short: %v (want ~500ms @100/s)", el)
	}
}

func TestBucketWaitersAndWaitTotal(t *testing.T) {
	b := NewBucket(10, 1)
	if err := b.Wait(t.Context(), 1); err != nil { // drain the initial burst
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- b.Wait(t.Context(), 10) // ~1s of refill
	}()
	waitFor(t, 2*time.Second, func() bool { return b.Stats().Waiters == 1 }, "one blocked waiter")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s := b.Stats()
	if s.Waiters != 0 {
		t.Fatalf("Waiters = %d after grant", s.Waiters)
	}
	if s.WaitTotal < 800*time.Millisecond {
		t.Fatalf("WaitTotal = %v, want >= ~1s of blocked time", s.WaitTotal)
	}
}

func TestBucketSetRateHot(t *testing.T) {
	b := NewBucket(100, 100)
	if err := b.Wait(t.Context(), 100); err != nil { // drain burst
		t.Fatal(err)
	}
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		done <- b.Wait(t.Context(), 200) // 2s at the initial rate
	}()
	waitFor(t, time.Second, func() bool { return b.Stats().Waiters == 1 }, "waiter blocked")
	time.Sleep(200 * time.Millisecond)
	b.SetRate(100_000, 100_000)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > 1200*time.Millisecond {
		t.Fatalf("SetRate did not wake/repace the waiter: %v", el)
	}
	s := b.Stats()
	if s.Rate != 100_000 || s.Burst != 100_000 {
		t.Fatalf("stats after SetRate: %+v", s)
	}
}

func TestBucketSetRateToUnlimitedWakesWaiter(t *testing.T) {
	b := NewBucket(10, 1)
	if err := b.Wait(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- b.Wait(t.Context(), 1000) // 100s at the initial rate
	}()
	waitFor(t, time.Second, func() bool { return b.Stats().Waiters == 1 }, "waiter blocked")
	b.SetRate(0, 0)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not released after SetRate(0,0)")
	}
}

func TestBucketWaitCancel(t *testing.T) {
	b := NewBucket(10, 1)
	if err := b.Wait(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- b.Wait(ctx, 100) // ~10s without cancel
	}()
	waitFor(t, time.Second, func() bool { return b.Stats().Waiters == 1 }, "waiter blocked")
	start := time.Now()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait returned %v, want context.Canceled", err)
	}
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("cancel took %v", el)
	}
	if w := b.Stats().Waiters; w != 0 {
		t.Fatalf("Waiters = %d after cancel", w)
	}
}

func TestBucketWaitNExceedsBurst(t *testing.T) {
	// n > burst must be served in installments, not deadlock: 100 instant
	// (burst), then 100 + 50 at 1000/s -> ~150ms.
	b := NewBucket(1000, 100)
	start := time.Now()
	if err := b.Wait(t.Context(), 250); err != nil {
		t.Fatal(err)
	}
	el := time.Since(start)
	if el < 100*time.Millisecond || el > 1500*time.Millisecond {
		t.Fatalf("n>burst wait took %v, want ~150ms", el)
	}
	if got := float64(b.ring.Sum(b.clock.Now(), 0)); got != 250 {
		t.Fatalf("recorded consumption = %v, want 250", got)
	}
}

func TestBucketWindowedRateFakeClock(t *testing.T) {
	fc := newFakeClock()
	b := NewBucket(0, 0) // unlimited: Wait consumes without pacing
	b.clock = fc
	for i := range 10 {
		if err := b.Wait(t.Context(), 100); err != nil {
			t.Fatal(err)
		}
		if i < 9 {
			fc.Advance(time.Second)
		}
	}
	// read at t=E+9: the (t-10, t] window covers all 10 ticks of 100
	if got := b.Stats().WindowedRate; got != 100 {
		t.Fatalf("WindowedRate = %v, want 100 (1000 tokens over 10s)", got)
	}
	fc.Advance(5 * time.Second) // window now covers ticks E+5..E+9: 5 ticks
	if got := b.Stats().WindowedRate; got != 50 {
		t.Fatalf("WindowedRate = %v, want 50", got)
	}
	fc.Advance(5 * time.Second) // nothing left inside the window
	if got := b.Stats().WindowedRate; got != 0 {
		t.Fatalf("WindowedRate = %v, want 0", got)
	}
}

func TestBucketConcurrent(t *testing.T) {
	b := NewBucket(100_000, 1000)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	var consumed atomic.Int64
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := b.Wait(ctx, 10); err != nil {
					return
				}
				consumed.Add(10)
			}
		}()
	}
	wg.Wait()
	// generous bounds: burst 1000 + ~300ms*100k/s, minus scheduling slop
	if got := consumed.Load(); got > 40_000 || got < 5_000 {
		t.Fatalf("consumed = %d, want roughly 10k-30k (rate 100k/s over 300ms)", got)
	}
}
