package transfer

import (
	"testing"
	"time"
)

// gateHarness drives a killGate with a fake clock and settable rate/util.
type gateHarness struct {
	now  time.Time
	rate float64
	util float64
	g    *killGate
}

func newGateHarness() *gateHarness {
	h := &gateHarness{now: time.Unix(1000, 0)}
	h.g = newKillGate(
		func() time.Time { return h.now },
		func() float64 { return h.rate },
		func() float64 { return h.util },
	)
	return h
}

func TestKillGateNilAlwaysAllows(t *testing.T) {
	var g *killGate
	if !g.tryKill() {
		t.Fatal("nil gate must allow")
	}
}

func TestKillGateLimiterSuppresses(t *testing.T) {
	h := newGateHarness()
	h.rate, h.util = 100, killUtilCeiling
	if h.g.tryKill() {
		t.Fatal("kill granted while the bandwidth limiter is the constraint")
	}
	h.util = 0
	if !h.g.tryKill() {
		t.Fatal("kill denied with an idle limiter")
	}
}

func TestKillGateVerifyWindowGrantsExactlyOne(t *testing.T) {
	h := newGateHarness()
	h.rate = 100
	if !h.g.tryKill() {
		t.Fatal("first kill denied")
	}
	// A racing monitor in the same sweep, and any monitor mid-window: denied.
	if h.g.tryKill() {
		t.Fatal("second kill granted inside the verify window")
	}
	h.now = h.now.Add(killVerifyWindow / 2)
	if h.g.tryKill() {
		t.Fatal("kill granted halfway through the verify window")
	}
}

func TestKillGateProductiveKillResets(t *testing.T) {
	h := newGateHarness()
	h.rate = 100
	if !h.g.tryKill() {
		t.Fatal("first kill denied")
	}
	// The rate rose past the gain threshold: the kill freed capacity.
	h.rate = 100 * (1 + killGain)
	h.now = h.now.Add(killVerifyWindow)
	if !h.g.tryKill() {
		t.Fatal("kill denied after a productive verify window")
	}
}

func TestKillGateUnproductiveKillBacksOff(t *testing.T) {
	h := newGateHarness()
	h.rate = 100
	if !h.g.tryKill() {
		t.Fatal("first kill denied")
	}
	// Flat rate at window end: saturated pipe misread as stalling.
	h.now = h.now.Add(killVerifyWindow)
	if h.g.tryKill() {
		t.Fatal("kill granted right after an unproductive verify window")
	}
	h.now = h.now.Add(killSuppressMin)
	if !h.g.tryKill() {
		t.Fatal("kill denied after the first suppression hold elapsed")
	}

	// A second unproductive kill doubles the hold.
	h.now = h.now.Add(killVerifyWindow)
	if h.g.tryKill() {
		t.Fatal("kill granted after second unproductive window")
	}
	h.now = h.now.Add(killSuppressMin) // first hold width: not enough now
	if h.g.tryKill() {
		t.Fatal("suppression did not grow after a repeat unproductive kill")
	}
	h.now = h.now.Add(killSuppressMin) // 2x total
	if !h.g.tryKill() {
		t.Fatal("kill denied after the doubled hold elapsed")
	}
}
