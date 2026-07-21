package transfer

import (
	"testing"
	"time"
)

const (
	testWindow = 100 * time.Millisecond
	testFloor  = 1000
)

// TestStallNeverArmed: a connection that never meets the floor while the
// file was never armed is never soft-killed (it is just a slow network —
// only layers 2/3 apply, and it keeps trickling).
func TestStallNeverArmed(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	c := m.driveConn(start)
	for w := 1; w <= 5; w++ {
		c.addBytes(testFloor / 10) // below floor, but bytes flow
		m.driveEval(c, start.Add(time.Duration(w)*testWindow))
		if c.killed {
			t.Fatalf("window %d: below-floor conn killed without arming (%s)", w, c.reason)
		}
	}
	if armed, _ := m.armedState(); armed {
		t.Fatal("file armed by below-floor traffic")
	}
}

// TestStallIdleDeadline: zero bytes for a window kills, armed from byte 0 —
// a never-started stream cannot hang the transfer.
func TestStallIdleDeadline(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	c := m.driveConn(start)
	m.driveEval(c, start.Add(testWindow)) // no bytes at all
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
	m.driveEval(fast, start.Add(testWindow))
	m.driveEval(slow, start.Add(testWindow))
	if armed, _ := m.armedState(); !armed {
		t.Fatal("file not armed by above-floor window")
	}

	// Window 2: slow drops below floor, fast sustains → soft kill.
	fast.addBytes(2 * testFloor)
	slow.addBytes(testFloor / 10)
	m.driveEval(slow, start.Add(2*testWindow))
	if !slow.killed || slow.reason != stallSoftFloor {
		t.Fatalf("slow: killed=%v reason=%s, want soft-floor kill", slow.killed, slow.reason)
	}
	m.driveEval(fast, start.Add(2*testWindow))
	if fast.killed {
		t.Fatalf("fast conn killed (%s)", fast.reason)
	}
}

// TestStallHysteresis: armed file, then ALL conns drop below the floor.
// The first conn evaluated while a peer's verdict is still "above" (windows
// are per-conn) is soft-killed; after that no active conn is above the
// floor, soft enforcement suspends for the survivor (no kill, so no EMA
// penalty) — but the hard 10× ceiling still fires.
func TestStallHysteresis(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	a := m.driveConn(start)
	b := m.driveConn(start)

	// Window 1: both above floor → armed.
	a.addBytes(2 * testFloor)
	b.addBytes(2 * testFloor)
	m.driveEval(a, start.Add(testWindow))
	m.driveEval(b, start.Add(testWindow))

	// Window 2: both trickle. a is evaluated while b's verdict is stale
	// "above" → a is soft-killed. b is then evaluated with nobody above →
	// hysteresis suspends its soft kill.
	a.addBytes(1)
	b.addBytes(1)
	m.driveEval(a, start.Add(2*testWindow))
	if !a.killed || a.reason != stallSoftFloor {
		t.Fatalf("a: killed=%v reason=%s, want soft-floor kill", a.killed, a.reason)
	}
	m.driveEval(b, start.Add(2*testWindow))
	if b.killed {
		t.Fatalf("b killed during hysteresis (%s)", b.reason)
	}

	// Windows 3..10: b trickles alone below the floor. Soft enforcement
	// stays suspended; the hard ceiling needs 10 floorless windows.
	for w := 3; w <= 10; w++ {
		b.addBytes(1)
		m.driveEval(b, start.Add(time.Duration(w)*testWindow))
		if b.killed {
			t.Fatalf("window %d: b killed early (%s)", w, b.reason)
		}
	}
	b.addBytes(1)
	m.driveEval(b, start.Add(11*testWindow))
	if !b.killed || b.reason != stallHardCeiling {
		t.Fatalf("b: killed=%v reason=%s, want hard-ceiling kill", b.killed, b.reason)
	}
}

// TestStallHardCeilingSingleConn: with one connection total, hysteresis
// always suspends the soft layer (the lone conn can't be above the floor
// while below it), but the hard ceiling still fires.
func TestStallHardCeilingSingleConn(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	c := m.driveConn(start)

	c.addBytes(2 * testFloor) // window 1: arm
	m.driveEval(c, start.Add(testWindow))
	if armed, _ := m.armedState(); !armed {
		t.Fatal("not armed")
	}
	for w := 2; w <= 10; w++ {
		c.addBytes(1)
		m.driveEval(c, start.Add(time.Duration(w)*testWindow))
		if c.killed {
			t.Fatalf("window %d: killed early (%s)", w, c.reason)
		}
	}
	c.addBytes(1)
	m.driveEval(c, start.Add(11*testWindow))
	if !c.killed || c.reason != stallHardCeiling {
		t.Fatalf("killed=%v reason=%s, want hard-ceiling kill", c.killed, c.reason)
	}
}

// TestStallRecoveryRearm: with soft enforcement suspended (nobody above
// the floor), a conn recovering above the floor lifts the suspension and
// the still-slow peer is soft-killed.
func TestStallRecoveryRearm(t *testing.T) {
	m := newStallMonitor(testWindow, testFloor)
	start := time.Now()
	a := m.driveConn(start)
	b := m.driveConn(start)

	// Window 1: both above → armed.
	a.addBytes(2 * testFloor)
	b.addBytes(2 * testFloor)
	m.driveEval(a, start.Add(testWindow))
	m.driveEval(b, start.Add(testWindow))

	// Window 2: both dip; a dies on b's stale verdict, b is suspended.
	a.addBytes(1)
	b.addBytes(1)
	m.driveEval(a, start.Add(2*testWindow))
	m.driveEval(b, start.Add(2*testWindow))
	if !a.killed || b.killed {
		t.Fatalf("setup: a killed=%v b killed=%v, want a dead b alive", a.killed, b.killed)
	}

	// Window 3: a fresh conn c recovers above the floor; b, still slow, is
	// soft-killed now that hysteresis lifted.
	c := m.driveConn(start)
	c.addBytes(2 * testFloor)
	m.driveEval(c, start.Add(3*testWindow))
	b.addBytes(1)
	m.driveEval(b, start.Add(3*testWindow))
	if !b.killed || b.reason != stallSoftFloor {
		t.Fatalf("b: killed=%v reason=%s, want soft-floor kill after recovery", b.killed, b.reason)
	}
	if c.killed {
		t.Fatalf("c killed (%s)", c.reason)
	}
}
