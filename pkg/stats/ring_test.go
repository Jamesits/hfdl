package stats

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock for deterministic ring windowing.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(0, 0)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestRateRingWindowedRate(t *testing.T) {
	tests := []struct {
		name string
		run  func(r *rateRing, c *fakeClock)
		want float64
	}{
		{
			name: "bytes in window over window length",
			run: func(r *rateRing, c *fakeClock) {
				// 10 B in each of t=0..t=9, measured at t=9.
				for range 9 {
					r.Add(c.now().Unix(), 10)
					c.advance(time.Second)
				}
				r.Add(c.now().Unix(), 10)
			},
			want: 10, // 100 B over 10s
		},
		{
			name: "idle seconds count as zero",
			run: func(r *rateRing, c *fakeClock) {
				r.Add(c.now().Unix(), 100)
				c.advance(9 * time.Second)
			},
			want: 10, // 100 B over 10s, nine idle seconds
		},
		{
			name: "ring wraparound drops expired seconds",
			run: func(r *rateRing, c *fakeClock) {
				r.Add(c.now().Unix(), 100) // t=0
				c.advance(5 * time.Second)
				r.Add(c.now().Unix(), 50)  // t=5
				c.advance(5 * time.Second) // t=10: t=0 bucket reused and zeroed
			},
			want: 5, // only the 50 B from t=5 remain
		},
		{
			name: "full window expiry drains to zero",
			run: func(r *rateRing, c *fakeClock) {
				r.Add(c.now().Unix(), 1000)
				c.advance(20 * time.Second)
			},
			want: 0,
		},
		{
			name: "empty ring",
			run:  func(r *rateRing, c *fakeClock) {},
			want: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newFakeClock()
			var r rateRing
			tc.run(&r, c)
			if got := r.Rate(c.now().Unix()); got != tc.want {
				t.Fatalf("Rate() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRateRingSameSecondAccumulates(t *testing.T) {
	c := newFakeClock()
	var r rateRing
	for range 5 {
		r.Add(c.now().Unix(), 7)
	}
	if got := r.Rate(c.now().Unix()); got != 3.5 {
		t.Fatalf("Rate() = %v, want 3.5", got)
	}
}

func TestRateRingConcurrentAdds(t *testing.T) {
	c := newFakeClock()
	var r rateRing
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				r.Add(c.now().Unix(), 1)
			}
		}()
	}
	wg.Wait()
	if got := r.Rate(c.now().Unix()); got != 3200 {
		t.Fatalf("Rate() = %v, want 3200", got)
	}
}
