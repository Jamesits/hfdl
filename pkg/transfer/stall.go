package transfer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Stall policy, four layers — layers 1 and 2 are never suspended:
//
//  1. Header deadline (enforced in httpSource.do): response headers within
//     cfg.HeaderTimeout.
//  2. Idle-read deadline: zero bytes read for StallWindow kills the
//     connection — armed from byte 0, so a never-started stream cannot hang
//     a transfer.
//  3. Hard no-progress ceiling: a connection that has not met the floor for
//     hardStallFactor × StallWindow is killed regardless of soft-policy
//     hysteresis — a trickling conn cannot survive even with one connection.
//  4. Soft throughput floor: per-connection sliding windows count
//     application bytes against StallMinBytes per StallWindow. Enforcement
//     arms only once some connection of the file has proven the path can
//     beat the floor (arming), suspends whenever no active connection is
//     above the floor (hysteresis — network-wide degradation must not cause
//     kill-and-rehandshake churn), and applies per connection only after
//     one full window post-connect (warmup, implicit in windowed
//     evaluation). Suspended periods never kill, so they never penalize EMA
//     or count retries.
//
// Layers 3 and 4 additionally pass through the process-wide killGate
// (killgate.go), which suspends throughput kills while the global bandwidth
// limiter is the active constraint, during post-kill verification, and
// during congestion suppression.
const hardStallFactor = 10

// stallReason identifies which layer killed a connection (span/event
// payloads and tests).
type stallReason int

const (
	stallNone        stallReason = iota
	stallIdle                    // layer 2: zero bytes for one window
	stallHardCeiling             // layer 3: floor unmet for hardStallFactor windows
	stallSoftFloor               // layer 4: armed, below floor while a peer is above
)

func (r stallReason) String() string {
	switch r {
	case stallIdle:
		return "idle-read-deadline"
	case stallHardCeiling:
		return "hard-no-progress-ceiling"
	case stallSoftFloor:
		return "soft-throughput-floor"
	}
	return "none"
}

// stallMonitor is the per-file stall enforcer. Windowed evaluation is
// factored into evalLocked (pure, clock-injected) so tests drive layers
// deterministically without sleeping; production conns are evaluated by a
// per-conn ticker goroutine.
type stallMonitor struct {
	window time.Duration
	floor  int64
	now    func() time.Time

	// gate is the process-wide throughput-kill coordinator (nil = always
	// allow, the bare-monitor test construction). Layers 3 and 4 consult it;
	// layer 2 (idle read) never does.
	gate *killGate

	mu    sync.Mutex
	armed bool
	conns map[*connTrack]struct{}
}

func newStallMonitor(window time.Duration, floor int64) *stallMonitor {
	return &stallMonitor{
		window: window,
		floor:  floor,
		now:    time.Now,
		conns:  make(map[*connTrack]struct{}),
	}
}

// connTrack is one connection's stall state. windowBytes is atomic: the
// streaming goroutine adds on every read while the eval sweep swaps.
//
// sinceFloorBytes accumulates application bytes since the last window that met
// the floor; it is the hard-ceiling's measure of TRUE progress (distinct from
// "time below floor"): a conn delivering at least one floor's worth of bytes
// per hardStallFactor windows is progressing, however slowly, and is never
// hard-killed — the plan never fights a slow network.
type connTrack struct {
	mon *stallMonitor

	start           time.Time
	windowStart     time.Time // start of the current, not-yet-evaluated window
	lastFloor       time.Time // last window end that met the floor (init: start)
	sinceFloorBytes int64     // bytes since lastFloor (hard-ceiling no-progress measure)
	above           bool      // current window met the floor
	killed          bool
	reason          stallReason
	cancel          context.CancelFunc

	windowBytes atomic.Int64
	stop        chan struct{} // closed by connEnd
	done        chan struct{} // closed when the watcher exits
}

// connStart registers a connection, arming its idle deadline from byte 0.
// cancel is the attempt-context cancel the monitor invokes on a kill.
func (m *stallMonitor) connStart(cancel context.CancelFunc) *connTrack {
	now := m.now()
	c := &connTrack{
		mon:         m,
		start:       now,
		windowStart: now,
		lastFloor:   now,
		cancel:      cancel,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	m.mu.Lock()
	m.conns[c] = struct{}{}
	m.mu.Unlock()
	go m.watch(c)
	return c
}

// connEnd deregisters a connection (attempt finished, however it ended) and
// waits for its watcher to exit.
func (m *stallMonitor) connEnd(c *connTrack) {
	m.mu.Lock()
	delete(m.conns, c)
	m.mu.Unlock()
	close(c.stop)
	<-c.done
}

// addBytes counts application bytes read from the body.
func (c *connTrack) addBytes(n int64) { c.windowBytes.Add(n) }

// killReason reports why the monitor killed this connection (stallNone if
// it did not).
func (c *connTrack) killReason() stallReason {
	c.mon.mu.Lock()
	defer c.mon.mu.Unlock()
	return c.reason
}

// watch drives the synchronized sweep once per window until connEnd. Every
// live connection's watcher calls sweep, which is idempotent per window
// boundary, so a full sweep runs regardless of which connection ticked.
func (m *stallMonitor) watch(c *connTrack) {
	defer close(c.done)
	t := time.NewTicker(m.window)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.sweep(m.now())
		case <-c.stop:
			return
		}
	}
}

// sweep evaluates the whole connection set for the elapsed window in two
// phases: first roll every conn whose window fully elapsed and record this
// window's verdict, then apply the kill layers. Computing all verdicts before
// any kill decision means layer 4's hysteresis sees a consistent same-window
// view of every peer — never a stale previous-window "above" verdict, which
// used to soft-kill a conn on a peer's about-to-be-updated result.
func (m *stallMonitor) sweep(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	type rolled struct {
		c   *connTrack
		wb  int64
		met bool
	}
	var due []rolled
	for c := range m.conns {
		if c.killed || now.Sub(c.windowStart) < m.window {
			continue // killed, or window not fully elapsed (warmup / mid-window)
		}
		wb := c.windowBytes.Swap(0)
		c.windowStart = c.windowStart.Add(m.window)
		c.sinceFloorBytes += wb
		met := m.floor <= 0 || wb >= m.floor
		if met {
			c.lastFloor = now
			c.sinceFloorBytes = 0
			c.above = true
			m.armed = true // the path proved it can beat the floor
		} else {
			c.above = false
		}
		due = append(due, rolled{c, wb, met})
	}
	if len(due) == 0 {
		return
	}
	anyAbove := m.anyAboveFloorLocked() // now reflects only this window's verdicts

	for _, r := range due {
		c := r.c
		if c.killed {
			continue
		}
		// Layer 2: idle-read deadline — zero bytes for a full window.
		if r.wb == 0 {
			m.killLocked(c, stallIdle)
			continue
		}
		// Layer 3: hard no-progress ceiling. Fires only on TRUE no-progress:
		// fewer than one floor's worth of bytes delivered in hardStallFactor
		// windows since the last floor-meeting window. A steadily
		// slow-but-progressing conn keeps sinceFloorBytes at or above the
		// floor and survives; a ~hung trickle (e.g. a byte per window)
		// eventually trips it even with a single connection. The process-wide
		// gate can suspend it (bandwidth ceiling active / kill verification /
		// congestion suppression) — a globally-limited conn trickles because
		// of the limiter, not because it is hung.
		if now.Sub(c.lastFloor) >= hardStallFactor*m.window && c.sinceFloorBytes < m.floor {
			if m.gate.tryKill() {
				m.killLocked(c, stallHardCeiling)
			}
			continue
		}
		// Layer 4: soft throughput floor. Armed file, this conn below floor,
		// hysteresis clear (some conn is above the floor THIS window), and
		// the process-wide gate granting the kill.
		if m.armed && !r.met && anyAbove && m.gate.tryKill() {
			m.killLocked(c, stallSoftFloor)
		}
	}
}

// anyAboveFloorLocked reports whether any live connection's last window met
// the floor.
func (m *stallMonitor) anyAboveFloorLocked() bool {
	for c := range m.conns {
		if !c.killed && c.above {
			return true
		}
	}
	return false
}

func (m *stallMonitor) killLocked(c *connTrack, r stallReason) {
	c.killed = true
	c.reason = r
	c.cancel()
}
