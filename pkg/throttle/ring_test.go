package throttle

import (
	"testing"
	"time"
)

func TestWindowRingWindowBoundaries(t *testing.T) {
	r := newWindowRing(2)
	base := time.Unix(1_700_000_000, 0)

	r.Add(base, 0, 5)
	r.Add(base, 1, 7)
	if got := r.Sum(base, 0); got != 5 {
		t.Fatalf("Sum(0) = %d, want 5", got)
	}
	if got := r.Sum(base, 1); got != 7 {
		t.Fatalf("Sum(1) = %d, want 7", got)
	}

	// 9s later: still inside the 10s window
	t9 := base.Add(9 * time.Second)
	r.Add(t9, 0, 11)
	if got := r.Sum(t9, 0); got != 16 {
		t.Fatalf("Sum at +9s = %d, want 16", got)
	}

	// 10s later: the base slot rolled over, base's 5 is gone
	t10 := base.Add(10 * time.Second)
	r.Add(t10, 0, 100)
	if got := r.Sum(t10, 0); got != 111 {
		t.Fatalf("Sum at +10s = %d, want 111 (base slot expired)", got)
	}

	// far future: everything expired
	if got := r.Sum(base.Add(time.Hour), 0); got != 0 {
		t.Fatalf("Sum at +1h = %d, want 0", got)
	}
	// reads before any write are zero
	if got := r.Sum(base.Add(-time.Hour), 0); got != 0 {
		t.Fatalf("Sum at -1h = %d, want 0", got)
	}
}
