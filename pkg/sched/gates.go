package sched

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jamesits/hfdl/pkg/store"
)

// gate is a ctx-cancelable boolean condition with broadcast wakeups, used
// for the operator pause and the ENOSPC pause — the two suspend levels.
type gate struct {
	mu   sync.Mutex
	on   bool
	wake chan struct{} // closed and replaced on every state change
}

func newGate() *gate { return &gate{wake: make(chan struct{})} }

func (g *gate) set(on bool) {
	g.mu.Lock()
	if g.on != on {
		g.on = on
		close(g.wake)
		g.wake = make(chan struct{})
	}
	g.mu.Unlock()
}

func (g *gate) active() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.on
}

// wait blocks while the gate is on.
func (g *gate) wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		if !g.on {
			g.mu.Unlock()
			return nil
		}
		wake := g.wake
		g.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// waitResumable blocks while either the operator pause or the ENOSPC pause
// holds; download and install workers call it before every dequeue
// (suspend gates those two queues).
func (m *Manager) waitResumable(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !m.pause.active() && !m.enospc.active() {
			return nil
		}
		if err := m.pause.wait(ctx); err != nil {
			return err
		}
		if err := m.enospc.wait(ctx); err != nil {
			return err
		}
	}
}

// apiGate applies the Hub API rate constraint before one metadata call: the
// per-(endpoint,'api') 429 cooldown is waited out, then the api-iops bucket
// is consumed. Cooldowns are re-read after each wait because another worker
// may have extended them.
func (m *Manager) apiGate(ctx context.Context, endpoint string) error {
	for {
		if err := m.waitCooldown(ctx, endpoint, store.CooldownAPI); err != nil {
			return err
		}
		if m.cfg.API == nil {
			return ctx.Err()
		}
		if err := m.cfg.API.Wait(ctx, 1); err != nil {
			return err
		}
		// A 429 observed by a peer while we waited on the bucket must still
		// gate us: re-check once. A newly-set cooldown loops us back.
		until, err := m.st.CooldownUntil(ctx, endpoint, store.CooldownAPI)
		if err != nil {
			return err
		}
		if until.IsZero() || !m.nowFn().Before(until) {
			return nil
		}
	}
}

// casGate applies the CAS control-plane rate constraint (429 cooldown,
// kind 'cas'). No bucket: CAS traffic is reconstruction/token calls only.
func (m *Manager) casGate(ctx context.Context, endpoint string) error {
	return m.waitCooldown(ctx, endpoint, store.CooldownCAS)
}

// waitCooldown blocks until the (endpoint, kind) cooldown row expires.
func (m *Manager) waitCooldown(ctx context.Context, endpoint, kind string) error {
	for {
		until, err := m.st.CooldownUntil(ctx, endpoint, kind)
		if err != nil {
			return err
		}
		now := m.nowFn()
		if until.IsZero() || !now.Before(until) {
			return nil
		}
		t := time.NewTimer(until.Sub(now))
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
}

// setCooldown records a 429 gate. When the server sent no Retry-After the
// fallback applies: exponential backoff 1s→5m with ±20% jitter,
// stepped by attempt.
func (m *Manager) setCooldown(ctx context.Context, endpoint, kind string, retryAfter time.Duration, attempt int, reason string) error {
	d := retryAfter
	if d <= 0 {
		d = cooldownBackoff(attempt)
	}
	until := m.nowFn().Add(d)
	m.log.Info("cooldown set", "endpoint", endpoint, "kind", kind, "until", until, "reason", reason)
	return m.st.SetCooldown(ctx, endpoint, kind, until, reason)
}

// cooldownBackoff is the jittered exp-backoff ladder for cooldowns without
// a server hint: 1s→5m, ±20% jitter.
func cooldownBackoff(attempt int) time.Duration {
	d := time.Second << uint(min(attempt, 9))
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	// Deterministic ±20% jitter derived from the attempt number (no rng
	// state to carry; tests stay reproducible).
	jitter := time.Duration(int64(d) / 5 * int64((attempt%3)-1))
	return d + jitter
}

// OutOfSpaceError is terminal: the write probe kept failing after the
// configured bound, so disk-full is not transient (statfs-free was
// assumed to correlate with writability; quota/cgroup limits break
// that).
type OutOfSpaceError struct {
	Dir    string
	Probes int
}

func (e *OutOfSpaceError) Error() string {
	return fmt.Sprintf("sched: out of disk space at %s: write probe keeps failing after %d attempts",
		e.Dir, e.Probes)
}

const (
	// probeMinBytes is the probe floor; the actual probe is demand-sized
	// (a reservation quota that rejects the real allocation rejects only
	// a demand-honest probe).
	probeMinBytes = 4 << 20
	// probeGiveUpDefault bounds consecutive failed probes before the
	// pause escalates to terminal job errors.
	probeGiveUpDefault = 100
	// probeBackoffMax caps the probe poll cadence (1s→30s, ±20% jitter).
	probeBackoffMax = 30 * time.Second
)

// noteIOError classifies an IO error from any write/copy path; ENOSPC and
// EDQUOT trigger the global download+install pause. Returns true
// when the error was an out-of-space condition.
func (m *Manager) noteIOError(ctx context.Context, err error) bool {
	return m.noteIOErrorAt(ctx, err, "", enospcResumeBytes)
}

// noteIOErrorAt is noteIOError with the failing demand and directory
// known (fallocate of a file: its size; copies: their size). The demand
// scales the writability probe and the statfs watermark.
func (m *Manager) noteIOErrorAt(ctx context.Context, err error, dir string, demand int64) bool {
	if err == nil {
		return false
	}
	if !isOutOfSpace(err) {
		return false
	}
	if dir == "" {
		dir = m.cfg.Cache.Root()
	}
	if demand < enospcResumeBytes {
		demand = enospcResumeBytes
	}
	if m.enospc.active() {
		// Mid-episode larger demand: raise the watermark so the resume
		// proof covers it.
		if demand > m.enospcDemand {
			m.enospcDemand = demand
		}
		return true
	}
	m.enospcDemand = demand
	m.enospcDir = dir
	m.enospc.set(true)
	m.log.Warn("out of disk space; pausing download and install queues",
		"err", err, "dir", dir, "demand", demand)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.enospcWatcher(ctx)
	}()
	return true
}

// enospcWatcher polls until writability is PROVEN — statfs free above the
// recorded demand plus a successful write probe in the failure directory —
// then lifts the pause. Probe failures back off (1s→30s, jittered) and
// escalate terminally after the bound: a quota that keeps rejecting writes
// is not a transient condition.
func (m *Manager) enospcWatcher(ctx context.Context) {
	dir := m.enospcDir
	giveUp := m.probeGiveUpLimit
	if giveUp <= 0 {
		giveUp = probeGiveUpDefault
	}
	// Backoff state persists across episodes within one run: a flapping
	// volume reaches the cap quickly and stays there.
	backoff := time.Duration(m.enospcBackoffNs.Load())
	if backoff <= 0 {
		backoff = m.enospcPoll
		if backoff <= 0 || backoff > time.Second {
			backoff = time.Second
		}
	}
	capAt := m.probeBackoffMax
	if capAt <= 0 {
		capAt = probeBackoffMax
	}
	failures := 0
	for {
		sleep := backoff + time.Duration(int64(backoff)/5*int64((failures%3)-1))
		select {
		case <-ctx.Done():
			m.enospc.set(false)
			return
		case <-time.After(sleep):
		}

		writable := false
		free, err := m.statfsFreeFn(ctx, dir)
		switch {
		case err != nil:
			m.log.Debug("statfs during ENOSPC pause failed", "err", err)
		case free < m.enospcDemand:
			m.log.Debug("free space still below demand", "free", free, "demand", m.enospcDemand)
		default:
			// Demand-honest probe: on reservation filesystems only a
			// demand-sized fallocate proves the real write would succeed.
			size := max(m.enospcDemand, probeMinBytes)
			if perr := m.probeFn(ctx, dir, size); perr != nil {
				failures++
				if failures%10 == 0 {
					m.log.Warn("write probe keeps failing", "dir", dir, "failures", failures, "err", perr)
				} else {
					m.log.Debug("write probe failed", "dir", dir, "failures", failures, "err", perr)
				}
			} else {
				writable = true
			}
		}

		if writable {
			m.log.Info("disk writability proven; resuming queues", "free", free, "dir", dir)
			m.enospc.set(false)
			wake(m.wakeDownload)
			wake(m.wakeInstall)
			return
		}
		if failures >= giveUp {
			m.enospcGiveUp(ctx, dir, failures)
			return
		}
		backoff *= 2
		if backoff > capAt {
			backoff = capAt
		}
		m.enospcBackoffNs.Store(int64(backoff))
	}
}

// enospcGiveUp escalates a persistent out-of-space condition: every active
// job errors terminally and the pause lifts so the run can drain and
// report the failure.
func (m *Manager) enospcGiveUp(ctx context.Context, dir string, probes int) {
	err := &OutOfSpaceError{Dir: dir, Probes: probes}
	m.log.Error("out of disk space: write probe keeps failing; failing jobs", "err", err)
	m.recordError(err)
	var jobIDs []int64
	if qerr := m.st.DB().NewSelect().Model((*store.Job)(nil)).
		Column("id").
		Where("status IN (?, ?)", string(store.JobQueued), string(store.JobRunning)).
		Scan(ctx, &jobIDs); qerr != nil {
		m.log.Warn("list active jobs for ENOSPC give-up failed", "err", qerr)
	}
	for _, id := range jobIDs {
		if ferr := m.storeCall(ctx, func() error { return m.st.FinishJob(ctx, id, err) }); ferr != nil {
			m.log.Warn("finish job during ENOSPC give-up failed", "job_id", id, "err", ferr)
		}
	}
	m.enospc.set(false)
	wake(m.wakeDownload)
	wake(m.wakeInstall)
}
