package transfer

import (
	"sync"
	"time"
)

// killGate tuning. The verify window is measured in wall time, not stall
// windows, because the gate is shared by files with potentially different
// stall settings; 30s covers two default windows.
const (
	killVerifyWindow = 30 * time.Second
	killSuppressMin  = 30 * time.Second
	killSuppressMax  = 5 * time.Minute
	// killUtilCeiling: while the bandwidth bucket is this utilized, the
	// configured ceiling — not the connection — explains any below-floor
	// throughput, so throughput-based kills are suspended entirely.
	killUtilCeiling = 0.9
	// killGain is the minimum relative global-rate rise after a kill that
	// proves the kill productive (the freed share was re-absorbed by faster
	// connections). Below it the pipe was already saturated and further
	// kills are suppressed.
	killGain = 0.05
)

// killGate is the process-wide stall-kill coordinator shared by every
// stallMonitor. Per-file stall policy decides *which* connection deserves a
// throughput kill (layers 3 and 4); the gate decides whether such kills are
// currently believable at all:
//
//   - While the global bandwidth limiter is the active constraint
//     (utilization >= killUtilCeiling), throughput kills are suspended — the
//     limiter itself makes connections slow, and killing them would churn
//     rehandshakes without freeing anything.
//   - After any allowed kill, the gate enters a verify window during which
//     no further throughput kill fires. At its end the global rate is
//     compared to the pre-kill baseline: a rise means the kill freed real
//     capacity (business as usual); a flat rate means a saturated pipe or
//     congested uplink was mis-read as stalling, so kills are suppressed for
//     an exponentially growing hold.
//
// The idle-read layer (zero bytes for a full window) never consults the
// gate: a truly dead connection is dead regardless of pacing or congestion.
type killGate struct {
	now  func() time.Time
	rate func() float64 // global windowed download rate
	util func() float64 // bandwidth bucket utilization

	mu            sync.Mutex
	verifying     bool
	verifyUntil   time.Time
	baseRate      float64
	suppressUntil time.Time
	suppress      time.Duration // current suppression hold (0 = none yet)
}

func newKillGate(now func() time.Time, rate, util func() float64) *killGate {
	return &killGate{now: now, rate: rate, util: util}
}

// tryKill atomically checks whether a throughput kill (soft floor / hard
// ceiling) may fire and, if so, claims it: the pre-kill global rate is
// baselined and the verify window opens before this returns, so racing
// monitors can never both kill inside one window (check and claim happen
// under one lock). A nil gate always allows (tests construct bare
// stallMonitors).
func (g *killGate) tryKill() bool {
	if g == nil {
		return true
	}
	now := g.now()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.resolveLocked(now)
	if g.verifying || now.Before(g.suppressUntil) {
		return false
	}
	if g.util() >= killUtilCeiling {
		return false
	}
	g.verifying = true
	g.verifyUntil = now.Add(killVerifyWindow)
	g.baseRate = g.rate()
	return true
}

// resolveLocked closes an elapsed verify window: a risen rate proves kills
// productive and resets the suppression hold; a flat rate starts (or
// doubles) the suppression.
func (g *killGate) resolveLocked(now time.Time) {
	if !g.verifying || now.Before(g.verifyUntil) {
		return
	}
	g.verifying = false
	if g.rate() >= g.baseRate*(1+killGain) {
		g.suppress = 0
		return
	}
	if g.suppress == 0 {
		g.suppress = killSuppressMin
	} else {
		g.suppress = min(g.suppress*2, killSuppressMax)
	}
	g.suppressUntil = now.Add(g.suppress)
}
