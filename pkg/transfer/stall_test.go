package transfer

import (
	"testing"
	"time"
)

const (
	testWindow = 100 * time.Millisecond
	testFloor  = 1000
)

// TestStallNeverArmed: a connection that never meets the floor while the file
// was never armed is never soft-killed (a slow network — only layers 2/3
// apply), and a steady below-floor-but-progressing conn also survives the hard
// ceiling because it keeps delivering ≥ one floor per hardStallFactor windows.
func TestStallNeverArmed(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	c := m.driveConn(start)
	for w := 1; w <= 5; w++ {
		c.addBytes(testFloor / 10) // below floor, but real bytes flow
		m.driveSweep(start.Add(time.Duration(w) * testWindow))
		if c.killed {
			t.Fatalf("window %d: below-floor conn killed without arming (%s)", w, c.reason)
		}
	}
	if armed, _ := m.armedState(); armed {
		t.Fatal("file armed by below-floor traffic")
	}
}

// TestStallIdleDeadline: zero bytes for a window kills, armed from byte 0 — a
// never-started stream cannot hang the transfer.
func TestStallIdleDeadline(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	c := m.driveConn(start)
	m.driveSweep(start.Add(testWindow)) // no bytes at all
	if !c.killed || c.reason != stallIdle {
		t.Fatalf("killed=%v reason=%s, want idle kill", c.killed, c.reason)
	}
}

// TestStallArmedThenStalled: once the file is armed, a conn that drops below
// the floor while a peer sustains above it is soft-killed.
func TestStallArmedThenStalled(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	fast := m.driveConn(start)
	slow := m.driveConn(start)

	// Window 1: both above floor → file arms.
	fast.addBytes(2 * testFloor)
	slow.addBytes(2 * testFloor)
	m.driveSweep(start.Add(testWindow))
	if armed, _ := m.armedState(); !armed {
		t.Fatal("file not armed by above-floor window")
	}

	// Window 2: slow drops below floor, fast sustains → soft kill of slow only.
	fast.addBytes(2 * testFloor)
	slow.addBytes(testFloor / 10)
	m.driveSweep(start.Add(2 * testWindow))
	if !slow.killed || slow.reason != stallSoftFloor {
		t.Fatalf("slow: killed=%v reason=%s, want soft-floor kill", slow.killed, slow.reason)
	}
	if fast.killed {
		t.Fatalf("fast conn killed (%s)", fast.reason)
	}
}

// TestStallHysteresis: armed file, then ALL conns drop below the floor
// together. Because the sweep evaluates every conn for the same window before
// any kill, no conn sees a stale "above" verdict on a peer — soft enforcement
// suspends for everyone (no kill, no EMA penalty). The hard 10× ceiling still
// fires on the truly-hung trickle.
func TestStallHysteresis(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	a := m.driveConn(start)
	b := m.driveConn(start)

	// Window 1: both above floor → armed.
	a.addBytes(2 * testFloor)
	b.addBytes(2 * testFloor)
	m.driveSweep(start.Add(testWindow))
	if armed, _ := m.armedState(); !armed {
		t.Fatal("file not armed")
	}
	if a.killed || b.killed {
		t.Fatalf("armed window killed a=%v b=%v", a.killed, b.killed)
	}

	// Windows 2..10: both trickle 1 byte. Current-window hysteresis: nobody is
	// above the floor, so soft enforcement suspends — neither is soft-killed
	// (the old stale-verdict bug killed the first-evaluated conn on the
	// other's previous "above").
	for w := 2; w <= 10; w++ {
		a.addBytes(1)
		b.addBytes(1)
		m.driveSweep(start.Add(time.Duration(w) * testWindow))
		if a.killed || b.killed {
			t.Fatalf("window %d: soft-killed during hysteresis a=%v(%s) b=%v(%s)",
				w, a.killed, a.reason, b.killed, b.reason)
		}
	}

	// Window 11: the hard ceiling fires — a byte-per-window trickle delivers
	// far less than one floor per hardStallFactor windows, so it is hung.
	a.addBytes(1)
	b.addBytes(1)
	m.driveSweep(start.Add(11 * testWindow))
	if !a.killed || a.reason != stallHardCeiling {
		t.Fatalf("a: killed=%v reason=%s, want hard-ceiling kill", a.killed, a.reason)
	}
	if !b.killed || b.reason != stallHardCeiling {
		t.Fatalf("b: killed=%v reason=%s, want hard-ceiling kill", b.killed, b.reason)
	}
}

// TestStallHardCeilingSingleConn: with one connection total, hysteresis always
// suspends the soft layer (the lone conn can't be above the floor while below
// it), but the hard ceiling still fires on a hung trickle.
func TestStallHardCeilingSingleConn(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	c := m.driveConn(start)

	c.addBytes(2 * testFloor) // window 1: arm
	m.driveSweep(start.Add(testWindow))
	if armed, _ := m.armedState(); !armed {
		t.Fatal("not armed")
	}
	for w := 2; w <= 10; w++ {
		c.addBytes(1)
		m.driveSweep(start.Add(time.Duration(w) * testWindow))
		if c.killed {
			t.Fatalf("window %d: killed early (%s)", w, c.reason)
		}
	}
	c.addBytes(1)
	m.driveSweep(start.Add(11 * testWindow))
	if !c.killed || c.reason != stallHardCeiling {
		t.Fatalf("killed=%v reason=%s, want hard-ceiling kill", c.killed, c.reason)
	}
}

// TestStallSlowButProgressingSurvives: a pre-armed connection sustaining a
// meaningful-but-below-floor rate (floor/2 every window) is never hard-killed,
// even past hardStallFactor windows — the plan never fights a slow network.
// This is the case the old time-below-floor ceiling wrongly killed.
func TestStallSlowButProgressingSurvives(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	c := m.driveConn(start)

	c.addBytes(2 * testFloor) // window 1: arm
	m.driveSweep(start.Add(testWindow))
	// Windows 2..15: steady floor/2 — real progress, just below the floor.
	for w := 2; w <= 15; w++ {
		c.addBytes(testFloor / 2)
		m.driveSweep(start.Add(time.Duration(w) * testWindow))
		if c.killed {
			t.Fatalf("window %d: slow-but-progressing conn killed (%s)", w, c.reason)
		}
	}
}

// TestStallRecoveryRearm: with soft enforcement suspended (nobody above the
// floor), a fresh conn recovering above the floor lifts the suspension and the
// still-slow peers are soft-killed.
func TestStallRecoveryRearm(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	a := m.driveConn(start)
	b := m.driveConn(start)

	// Window 1: both above → armed.
	a.addBytes(2 * testFloor)
	b.addBytes(2 * testFloor)
	m.driveSweep(start.Add(testWindow))

	// Window 2: both dip below floor together → hysteresis suspends, nobody dies.
	a.addBytes(1)
	b.addBytes(1)
	m.driveSweep(start.Add(2 * testWindow))
	if a.killed || b.killed {
		t.Fatalf("suspended window killed a=%v b=%v", a.killed, b.killed)
	}

	// Window 3: a fresh conn c recovers above the floor; with a peer above
	// again, the still-slow a and b are soft-killed.
	c := m.driveConn(start.Add(2 * testWindow))
	c.addBytes(2 * testFloor)
	a.addBytes(1)
	b.addBytes(1)
	m.driveSweep(start.Add(3 * testWindow))
	if !a.killed || a.reason != stallSoftFloor {
		t.Fatalf("a: killed=%v reason=%s, want soft-floor kill after recovery", a.killed, a.reason)
	}
	if !b.killed || b.reason != stallSoftFloor {
		t.Fatalf("b: killed=%v reason=%s, want soft-floor kill after recovery", b.killed, b.reason)
	}
	if c.killed {
		t.Fatalf("c killed (%s)", c.reason)
	}
}
