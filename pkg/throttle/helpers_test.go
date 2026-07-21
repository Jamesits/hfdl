package throttle

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// fakeClock is a manually advanced, goroutine-safe clock.
type fakeClock struct {
	ns atomic.Int64
}

func newFakeClock() *fakeClock {
	f := &fakeClock{}
	f.ns.Store(time.Unix(1_700_000_000, 0).UnixNano())
	return f
}

func (f *fakeClock) Now() time.Time { return time.Unix(0, f.ns.Load()).UTC() }

func (f *fakeClock) Advance(d time.Duration) { f.ns.Add(int64(d)) }

// fakeSleeper records every sleep slice and advances the fake clock by the
// requested duration plus overshoot (simulating sleep overshoot, which is
// how WaitCheck's remain goes negative). onCall runs after recording.
type fakeSleeper struct {
	clock     *fakeClock
	overshoot time.Duration
	onCall    func(n int)

	mu    sync.Mutex
	calls []time.Duration
}

func (s *fakeSleeper) sleep(ctx context.Context, d time.Duration) error {
	s.mu.Lock()
	s.calls = append(s.calls, d)
	n := len(s.calls)
	s.mu.Unlock()
	if s.onCall != nil {
		s.onCall(n)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.clock.Advance(d + s.overshoot)
	return nil
}

func (s *fakeSleeper) sum() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total time.Duration
	for _, d := range s.calls {
		total += d
	}
	return total
}

func (s *fakeSleeper) lens() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.calls...)
}
