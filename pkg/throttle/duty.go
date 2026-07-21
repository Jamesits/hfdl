package throttle

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// MediaClass is throttle's own filesystem classification for duty-cycle
// derating; sched maps fcio.FsType onto it so throttle and fcio never
// import each other.
type MediaClass int

const (
	MediaUnknown MediaClass = iota
	MediaSSD
	MediaHDD
	MediaNetFS
)

func (m MediaClass) String() string {
	switch m {
	case MediaSSD:
		return "ssd"
	case MediaHDD:
		return "hdd"
	case MediaNetFS:
		return "netfs"
	case MediaUnknown:
		return "unknown"
	default:
		return "MediaClass(" + strconv.Itoa(int(m)) + ")"
	}
}

// FastCopy WaitCheck()/TransSize() constants (src/fastcopy.cpp:2196-2270,
// fastcopy.h). Durations are the port of the original millisecond values.
const (
	dutyBusyCap      = time.Second            // mainTick clamp: mainTick >= 1000 -> 1000
	dutyMaxRemain    = 700 * time.Millisecond // remain = min(remain, 700)
	dutySleepSlice   = 200 * time.Millisecond // SLEEP_UNIT
	dutySuspendGuard = 500 * time.Millisecond // remain < -500 (wall clock jumped) -> 0
	netFSDerate      = 1.4                    // IsNetFs: active fraction /= 1.4
	hddDerate        = 2.0                    // !IsSSD: active fraction /= 2 (HDD cache is too effective)
	waitMinBuf       = 256 * 1024             // WAITMIN_BUF,  active%% < 90 (FastCopy waitLv > 10)
	waitMidBuf       = 1024 * 1024            // WAITMID_BUF,  90 <= active%% < 100 (FastCopy waitLv 1..10)
)

// DutyCalc is the caller-held per-worker checkpoint state — the port of
// FastCopy's thread-local WaitCalc. One per worker goroutine; do not copy
// after first use (the limiter keys its worker registry by pointer).
type DutyCalc struct {
	lastTick   time.Time     // WaitCalc.lastTick
	lastRemain time.Duration // WaitCalc.lastRemain (may be negative: oversleep credit)
	self       *dutyWorker
}

// DutyStats is a point-in-time snapshot of a DutyLimiter.
type DutyStats struct {
	Level       int           // configured disk active-time ceiling %% (100 = unlimited, 0 = paused)
	ActiveRatio float64       // measured busy/wall over the window, aggregated across workers
	Workers     int           // workers seen checkpointing within the window
	SleepDebt   time.Duration // summed lastRemain backlog across live workers
	Media       MediaClass    // derating in effect
}

// dutyWorker is the limiter-side mirror of a DutyCalc used for stats; all
// fields are atomics so Stats never touches the caller-held DutyCalc.
type dutyWorker struct {
	lastSeen   atomic.Int64 // unixnano of last Checkpoint
	lastRemain atomic.Int64 // nanoseconds
	swept      atomic.Bool  // evicted from the registry; re-register on next Checkpoint
}

// DutyLimiter enforces a disk active-time ceiling: level p means the disk
// may be busy at most p%% of wall time (0..100; 100 = unlimited, 0 = pause).
// The sleep mechanics are a direct port of FastCopy's WaitCheck(): each
// worker calls Checkpoint after a chunk of I/O; the limiter measures the
// busy stretch since the worker's previous Checkpoint and sleeps off
// remain = busy*(1-A)/A + lastRemain (capped, sliced) so the measured
// active share converges to the active fraction A = p/100, derated /1.4 on
// netfs and /2 on non-SSD media (MediaUnknown takes the conservative !IsSSD
// branch, as in FastCopy).
type DutyLimiter struct {
	clock clock
	sleep sleeper

	level atomic.Int64
	media atomic.Int64

	ring *windowRing // counter 0: busy ns, counter 1: busy+sleep (wall) ns

	mu      sync.Mutex
	workers map[*DutyCalc]*dutyWorker
	change  chan struct{} // close-and-replace broadcast: SetLevel wakes paused workers
}

// NewDutyLimiter returns a limiter at the given level (clamped to 0..100;
// 100 = unlimited) for the given media class.
func NewDutyLimiter(level int, media MediaClass) *DutyLimiter {
	d := &DutyLimiter{
		clock:   realClock{},
		sleep:   sleepCtx,
		ring:    newWindowRing(2),
		workers: make(map[*DutyCalc]*dutyWorker),
		change:  make(chan struct{}),
	}
	d.level.Store(int64(clampLevel(level)))
	d.media.Store(int64(media))
	return d
}

// Checkpoint is FastCopy's WaitCheck(), ctx-aware: it accounts the busy
// stretch since the worker's previous call, then sleeps off the computed
// debt in slices of at most 200ms so cancellation stays responsive,
// returning ctx.Err() when cancelled. The first call for a DutyCalc only
// primes the tick baseline, like WaitCalc.lastTick == 0. At level 0 it
// pauses — blocking in <=200ms slices until SetLevel changes the level —
// and restarts the tick baseline on resume (pause time is not busy time).
func (d *DutyLimiter) Checkpoint(ctx context.Context, c *DutyCalc) error {
	now := d.clock.Now()
	level := d.level.Load()
	if level >= 100 {
		// Unlimited: no-op accounting-wise. lastTick stays fresh so a later
		// hot SetLevel(<100) does not measure the whole unlimited stretch
		// as one busy tick.
		c.lastTick = now
		return nil
	}
	if level == 0 {
		return d.pause(ctx, c)
	}
	if c.lastTick.IsZero() {
		c.lastTick = now
		d.register(c, now)
		return nil
	}
	busy := now.Sub(c.lastTick)
	c.lastTick = now
	if busy <= 0 {
		return nil
	}
	if busy > dutyBusyCap {
		busy = dutyBusyCap
	}

	active := float64(level) / 100.0
	switch MediaClass(d.media.Load()) {
	case MediaNetFS:
		active /= netFSDerate
	case MediaHDD, MediaUnknown:
		active /= hddDerate
	}
	remain := time.Duration(float64(busy) * (1 - active) / active)
	remain += c.lastRemain
	if remain > dutyMaxRemain {
		remain = dutyMaxRemain
	}

	var slept time.Duration
	for remain > 0 {
		if err := ctx.Err(); err != nil {
			c.lastRemain = remain
			d.account(c, now, busy, slept)
			return err
		}
		unit := time.Millisecond
		if remain > dutySleepSlice {
			unit = dutySleepSlice
		}
		before := d.clock.Now()
		err := d.sleep(ctx, unit)
		cur := d.clock.Now()
		el := cur.Sub(before)
		slept += el
		remain -= el
		if remain < -dutySuspendGuard {
			remain = 0 // wall clock jumped (suspend): drop the debt
		}
		if err != nil {
			c.lastTick = cur
			c.lastRemain = remain
			d.account(c, now, busy, slept)
			return err
		}
	}
	c.lastTick = d.clock.Now()
	c.lastRemain = remain // possibly negative: oversleep credit carries over
	d.account(c, now, busy, slept)
	return nil
}

// SetLevel hot-updates the disk active-time ceiling (clamped to 0..100;
// 100 = unlimited, 0 = pause) and wakes every paused worker.
func (d *DutyLimiter) SetLevel(pct int) {
	d.level.Store(int64(clampLevel(pct)))
	d.mu.Lock()
	close(d.change)
	d.change = make(chan struct{})
	d.mu.Unlock()
}

// SetMedia hot-updates the media class used for derating.
func (d *DutyLimiter) SetMedia(m MediaClass) {
	d.media.Store(int64(m))
}

// WriteChunkSize is FastCopy's TransSize() mapped to the active-time
// ceiling: full size when unlimited, 1MiB (WAITMID_BUF) under light
// limiting (90 <= p < 100), 256KiB (WAITMIN_BUF) under heavy limiting
// (p < 90).
func (d *DutyLimiter) WriteChunkSize(normal int64) int64 {
	level := d.level.Load()
	if level >= 100 {
		return normal
	}
	if level >= 90 {
		return waitMidBuf
	}
	return waitMinBuf
}

// Stats snapshots the limiter, lazily sweeping workers that have not
// checkpointed within the window (their DutyCalc re-registers on the next
// Checkpoint). ActiveRatio is busy/(busy+sleep) over the window aggregated
// across workers; SleepDebt is the summed lastRemain of live workers.
func (d *DutyLimiter) Stats() DutyStats {
	now := d.clock.Now()
	busy := d.ring.Sum(now, 0)
	wall := d.ring.Sum(now, 1)
	var ratio float64
	if wall > 0 {
		ratio = float64(busy) / float64(wall)
	}

	cutoff := now.Add(-windowSecs * time.Second).UnixNano()
	var workers int
	var debt int64
	d.mu.Lock()
	for c, w := range d.workers {
		if w.lastSeen.Load() < cutoff {
			w.swept.Store(true)
			delete(d.workers, c)
			continue
		}
		workers++
		debt += w.lastRemain.Load()
	}
	d.mu.Unlock()

	return DutyStats{
		Level:       int(d.level.Load()),
		ActiveRatio: ratio,
		Workers:     workers,
		SleepDebt:   time.Duration(debt),
		Media:       MediaClass(d.media.Load()),
	}
}

// account records one checkpoint into the window ring and the worker
// registry, re-registering workers whose entry was swept.
func (d *DutyLimiter) account(c *DutyCalc, t time.Time, busy, slept time.Duration) {
	d.ring.Add(t, 0, int64(busy))
	d.ring.Add(t, 1, int64(busy+slept))
	w := c.self
	if w == nil || w.swept.Load() {
		d.register(c, t)
		w = c.self
	}
	w.lastSeen.Store(t.UnixNano())
	w.lastRemain.Store(int64(c.lastRemain))
}

func (d *DutyLimiter) register(c *DutyCalc, t time.Time) {
	w := &dutyWorker{}
	w.lastSeen.Store(t.UnixNano())
	d.mu.Lock()
	d.workers[c] = w
	d.mu.Unlock()
	c.self = w
}

// pause blocks a level-0 worker until the level changes or ctx is done,
// in <=200ms slices so both stay responsive; SetLevel broadcasts an
// immediate wake. The tick baseline restarts on resume — pause time is
// neither busy nor duty sleep, so it is not accounted.
func (d *DutyLimiter) pause(ctx context.Context, c *DutyCalc) error {
	for d.level.Load() == 0 {
		d.mu.Lock()
		ch := d.change
		d.mu.Unlock()
		t := time.NewTimer(dutySleepSlice)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-ch:
			t.Stop()
		case <-t.C:
		}
	}
	c.lastTick = d.clock.Now()
	return nil
}

func clampLevel(l int) int {
	if l < 0 {
		return 0
	}
	if l > 100 {
		return 100
	}
	return l
}
