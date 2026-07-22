package tune

import (
	"testing"
	"time"
)

// testCfg collapses the timing so tests step through states quickly with a
// synthetic clock.
func testCfg() Config {
	return Config{
		Settle:        10 * time.Second,
		ProbeInterval: 5 * time.Second,
		BackoffStart:  30 * time.Second,
		BackoffMax:    2 * time.Minute,
		KillFreeze:    30 * time.Second,
	}
}

// saturated returns a sample whose Assigned matches the budget.
func saturated(rate float64, budget int) Sample {
	return Sample{Rate: rate, Assigned: budget}
}

func TestRampAcceptsWhileRateRises(t *testing.T) {
	c := New(testCfg())
	now := time.Unix(0, 0)

	if got := c.Observe(now, 16, saturated(100, 1)); got != 2 {
		t.Fatalf("first probe budget = %d, want 2", got)
	}
	// Mid-settle: budget holds.
	now = now.Add(5 * time.Second)
	if got := c.Observe(now, 16, saturated(150, 2)); got != 2 {
		t.Fatalf("mid-settle budget = %d, want 2", got)
	}
	// Settle elapsed, rate doubled: accept 2, and after the probe interval
	// climb to 4.
	now = now.Add(5 * time.Second)
	if got := c.Observe(now, 16, saturated(200, 2)); got != 2 {
		t.Fatalf("accepted budget = %d, want 2", got)
	}
	now = now.Add(5 * time.Second)
	if got := c.Observe(now, 16, saturated(200, 2)); got != 4 {
		t.Fatalf("second probe budget = %d, want 4", got)
	}
}

// TestCommittedLagsProbe: Committed stays at the pre-probe budget while a
// probe is in flight, rises only on acceptance, and never rises across a
// revert — admission keyed off it can never open a file slot a revert
// cannot reclaim.
func TestCommittedLagsProbe(t *testing.T) {
	c := New(testCfg())
	now := time.Unix(0, 0)
	if got := c.Committed(); got != 1 {
		t.Fatalf("initial committed = %d, want 1", got)
	}
	if got := c.Observe(now, 16, saturated(100, 1)); got != 2 {
		t.Fatalf("probe budget = %d, want 2", got)
	}
	if got := c.Committed(); got != 1 {
		t.Fatalf("mid-probe committed = %d, want pre-probe 1", got)
	}
	// Settle elapsed, rate doubled: accepted.
	now = now.Add(10 * time.Second)
	if got := c.Observe(now, 16, saturated(200, 2)); got != 2 {
		t.Fatalf("accepted budget = %d, want 2", got)
	}
	if got := c.Committed(); got != 2 {
		t.Fatalf("post-accept committed = %d, want 2", got)
	}
	// Next probe reverts on a flat rate: committed never saw the probe.
	now = now.Add(5 * time.Second)
	if got := c.Observe(now, 16, saturated(200, 2)); got != 4 {
		t.Fatalf("second probe budget = %d, want 4", got)
	}
	if got := c.Committed(); got != 2 {
		t.Fatalf("mid-probe committed = %d, want 2", got)
	}
	now = now.Add(10 * time.Second)
	if got := c.Observe(now, 16, saturated(201, 4)); got != 2 {
		t.Fatalf("reverted budget = %d, want 2", got)
	}
	if got := c.Committed(); got != 2 {
		t.Fatalf("post-revert committed = %d, want 2", got)
	}
}

func TestFlatRateRevertsAndBacksOff(t *testing.T) {
	c := New(testCfg())
	now := time.Unix(0, 0)

	if got := c.Observe(now, 16, saturated(100, 1)); got != 2 {
		t.Fatalf("probe budget = %d, want 2", got)
	}
	// Settle elapsed, no gain: revert to 1.
	now = now.Add(10 * time.Second)
	if got := c.Observe(now, 16, saturated(102, 2)); got != 1 {
		t.Fatalf("reverted budget = %d, want 1", got)
	}
	// Within the 30s backoff no new probe fires.
	now = now.Add(20 * time.Second)
	if got := c.Observe(now, 16, saturated(100, 1)); got != 1 {
		t.Fatalf("backoff budget = %d, want 1", got)
	}
	// Backoff elapsed: probe again; flat again → next backoff doubles.
	now = now.Add(10 * time.Second)
	if got := c.Observe(now, 16, saturated(100, 1)); got != 2 {
		t.Fatalf("re-probe budget = %d, want 2", got)
	}
	now = now.Add(10 * time.Second)
	if got := c.Observe(now, 16, saturated(100, 2)); got != 1 {
		t.Fatalf("second revert budget = %d, want 1", got)
	}
	now = now.Add(45 * time.Second) // 30s < 45s < 60s: inside doubled backoff
	if got := c.Observe(now, 16, saturated(100, 1)); got != 1 {
		t.Fatalf("doubled backoff budget = %d, want 1", got)
	}
	now = now.Add(20 * time.Second) // 65s > 60s: doubled backoff elapsed
	if got := c.Observe(now, 16, saturated(100, 1)); got != 2 {
		t.Fatalf("post-doubled-backoff budget = %d, want 2", got)
	}
}

func TestUtilizationCeilingHoldsBudget(t *testing.T) {
	c := New(testCfg())
	now := time.Unix(0, 0)
	s := saturated(100, 1)
	s.Utilization = 0.97
	for range 10 {
		now = now.Add(time.Second)
		if got := c.Observe(now, 16, s); got != 1 {
			t.Fatalf("budget under utilization ceiling = %d, want 1", got)
		}
	}
}

func TestUnassignedBudgetHolds(t *testing.T) {
	c := New(testCfg())
	now := time.Unix(0, 0)
	// Budget 1 but nothing assigned (no active file): no probe.
	if got := c.Observe(now, 16, Sample{Rate: 100, Assigned: 0}); got != 1 {
		t.Fatalf("unassigned budget = %d, want 1", got)
	}
}

func TestKillFreezeRevertsProbe(t *testing.T) {
	c := New(testCfg())
	now := time.Unix(0, 0)
	// Seed the kill counter with a nonzero base: pre-existing kills must not
	// freeze a fresh controller.
	if got := c.Observe(now, 16, Sample{Rate: 100, Assigned: 1, Kills: 5}); got != 2 {
		t.Fatalf("probe budget = %d, want 2", got)
	}
	// A kill mid-probe reverts and freezes.
	now = now.Add(5 * time.Second)
	if got := c.Observe(now, 16, Sample{Rate: 300, Assigned: 2, Kills: 6}); got != 1 {
		t.Fatalf("post-kill budget = %d, want 1", got)
	}
	// Frozen: no probe even though everything else allows it.
	now = now.Add(20 * time.Second)
	if got := c.Observe(now, 16, Sample{Rate: 300, Assigned: 1, Kills: 6}); got != 1 {
		t.Fatalf("frozen budget = %d, want 1", got)
	}
	// Freeze elapsed: probing resumes.
	now = now.Add(15 * time.Second)
	if got := c.Observe(now, 16, Sample{Rate: 300, Assigned: 1, Kills: 6}); got != 2 {
		t.Fatalf("post-freeze budget = %d, want 2", got)
	}
}

func TestMaxBudgetClamps(t *testing.T) {
	c := New(testCfg())
	c.budget = 8
	now := time.Unix(0, 0)
	if got := c.Observe(now, 4, saturated(100, 8)); got != 4 {
		t.Fatalf("clamped budget = %d, want 4", got)
	}
	// At the ceiling: no probe.
	now = now.Add(time.Minute)
	if got := c.Observe(now, 4, saturated(100, 4)); got != 4 {
		t.Fatalf("ceiling budget = %d, want 4", got)
	}
}

func TestNextStep(t *testing.T) {
	cases := [][2]int{{1, 2}, {2, 4}, {4, 8}, {8, 10}, {12, 15}, {16, 20}}
	for _, tc := range cases {
		if got := nextStep(tc[0]); got != tc[1] {
			t.Errorf("nextStep(%d) = %d, want %d", tc[0], got, tc[1])
		}
	}
}
