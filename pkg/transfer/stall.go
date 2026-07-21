package transfer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Stall policy, four layers — the hard layers are never suspended:
//
//  1. Header deadline (enforced in httpSource.do): response headers within
//     cfg.HeaderTimeout.
//  2. Idle-read deadline: zero bytes read for StallWindow kills the
//     connection — armed from byte 0, so a never-started stream cannot hang
//     a transfer.
//  3. Hard no-progress ceiling: a connection that has not met the floor for
//     hardStallFactor × StallWindow is killed regardless of soft-policy
//     hysteresis — a trickling conn cannot survive even with --connections 1.
//  4. Soft throughput floor: per-connection sliding windows count
//     application bytes against StallMinBytes per StallWindow. Enforcement
//     arms only once some connection of the file has proven the path can
//     beat the floor (arming), suspends whenever no active connection is
//     above the floor (hysteresis — network-wide degradation must not cause
//     kill-and-rehandshake churn), and applies per connection only after
//     one full window post-connect (warmup, implicit in windowed
//     evaluation). Suspended periods never kill, so they never penalize EMA
//     or count retries.
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
// streaming goroutine adds on every read while the eval ticker swaps.
type connTrack struct {
	mon *stallMonitor

	start     time.Time
	lastFloor time.Time // last window end that met the floor (init: start)
	above     bool      // last evaluated window met the floor
	killed    bool
	reason    stallReason
	cancel    context.CancelFunc

	windowBytes atomic.Int64
	stop        chan struct{} // closed by connEnd
	done        chan struct{} // closed when the watcher exits
}

// connStart registers a connection, arming its idle deadline from byte 0.
// cancel is the attempt-context cancel the monitor invokes on a kill.
func (m *stallMonitor) connStart(cancel context.CancelFunc) *connTrack {
	c := &connTrack{
		mon:    m,
		start:  m.now(),
		cancel: cancel,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	c.lastFloor = c.start
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

// watch evaluates the connection once per window until connEnd.
func (m *stallMonitor) watch(c *connTrack) {
	defer close(c.done)
	t := time.NewTicker(m.window)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.mu.Lock()
			m.evalLocked(c, m.now())
			m.mu.Unlock()
		case <-c.stop:
			return
		}
	}
}

// evalLocked applies layers 2–4 for one elapsed window.
func (m *stallMonitor) evalLocked(c *connTrack, now time.Time) {
	if c.killed {
		return
	}
	wb := c.windowBytes.Swap(0)

	// Layer 2: idle-read deadline — zero bytes for a full window.
	if wb == 0 {
		m.killLocked(c, stallIdle)
		return
	}

	met := m.floor <= 0 || wb >= m.floor
	if met {
		c.lastFloor = now
		c.above = true
		// Arming: the path has proven it can beat the floor, so dropping
		// below it is a stall, not "just a slow network".
		m.armed = true
	} else {
		c.above = false
	}

	// Layer 3: hard no-progress ceiling — never suspended, warmup included.
	if now.Sub(c.lastFloor) >= hardStallFactor*m.window {
		m.killLocked(c, stallHardCeiling)
		return
	}

	// Layer 4: soft throughput floor. Requires the file armed, this conn
	// below floor, and hysteresis clear (some active conn above floor).
	// Warmup is implicit: the first evaluation happens exactly one full
	// window after connect.
	if m.armed && !met && m.anyAboveFloorLocked() {
		m.killLocked(c, stallSoftFloor)
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

// armedState exposes (armed, anyAboveFloor) for tests.
func (m *stallMonitor) armedState() (bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.armed, m.anyAboveFloorLocked()
}

// driveConn is the test seam: register a connection without a ticker or
// cancel side effects, for deterministic clock-driven evaluation.
func (m *stallMonitor) driveConn(start time.Time) *connTrack {
	c := &connTrack{
		mon:       m,
		start:     start,
		lastFloor: start,
		cancel:    func() {},
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	close(c.done) // no watcher runs for driven conns
	m.mu.Lock()
	m.conns[c] = struct{}{}
	m.mu.Unlock()
	return c
}

// driveEval is the test seam: evaluate one window at an injected clock time.
func (m *stallMonitor) driveEval(c *connTrack, now time.Time) {
	m.mu.Lock()
	m.evalLocked(c, now)
	m.mu.Unlock()
}
