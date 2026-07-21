package sched

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// setupENOSPCEpisode wires a manager with controllable statfs/probe seams
// and a fast poll cadence, then triggers an ENOSPC pause.
func setupENOSPCEpisode(t *testing.T, cap *logCapture) (*Manager, context.Context, context.CancelFunc) {
	t.Helper()
	hub := newFixtureHub(t, map[string][]byte{"x": []byte("y")})
	log := slog.New(slog.DiscardHandler)
	if cap != nil {
		log = slog.New(cap)
	}
	env := newTestEnvLog(t, hub, log)
	runCtx, cancel := context.WithCancel(t.Context())
	env.manager.detached = context.WithoutCancel(runCtx)
	env.manager.enospcPoll = 2 * time.Millisecond
	// Cap the probe backoff too: with only the fast poll set, a long give-up
	// bound (e.g. 11 probes) lets the 2ms base double to ~2s and the episode
	// spends seconds sleeping. Tests that want a specific cap override this.
	env.manager.probeBackoffMax = 64 * time.Millisecond
	return env.manager, runCtx, cancel
}

// TestENOSPCProbeKeepsFailing: statfs reports plenty of free space but the
// write probe keeps failing (the benchmark's quota case) — the pause must
// NOT lift, and after the bound it escalates to a terminal job error.
func TestENOSPCProbeKeepsFailing(t *testing.T) {
	m, runCtx, cancel := setupENOSPCEpisode(t, nil)
	defer cancel()

	m.statfsFreeFn = func(ctx context.Context, dir string) (int64, error) {
		return 1 << 40, nil // statfs says "plenty free" the whole time
	}
	var probes atomic.Int32
	m.probeFn = func(ctx context.Context, dir string, bytes int64) error {
		probes.Add(1)
		return fmt.Errorf("probe: %w", syscall.EDQUOT)
	}
	m.probeGiveUpLimit = 4

	if !m.noteIOError(runCtx, fmt.Errorf("fallocate: %w", syscall.EDQUOT)) {
		t.Fatal("EDQUOT not classified")
	}
	if !m.Snapshot().ENOSPCPaused {
		t.Fatal("not paused after EDQUOT")
	}

	// The pause must hold while probes fail, then escalate (recordError
	// lands before the pause lifts inside the give-up — wait for both).
	waitFor(t, "terminal escalation", func() bool {
		return m.joinedErrors() != nil && !m.Snapshot().ENOSPCPaused
	})
	var oos *OutOfSpaceError
	if !errors.As(m.joinedErrors(), &oos) {
		t.Fatalf("joined errors = %v, want *OutOfSpaceError", m.joinedErrors())
	}
	if got := probes.Load(); got < 4 {
		t.Errorf("probes = %d, want >= give-up bound 4", got)
	}
	if m.Snapshot().ENOSPCPaused {
		t.Error("still paused after give-up (pause must lift for drain)")
	}
}

// TestENOSPCProbeResumeOnce: probe fails a few times then succeeds — the
// pause lifts exactly once (no flap).
func TestENOSPCProbeResumeOnce(t *testing.T) {
	cap := &logCapture{}
	m, runCtx, cancel := setupENOSPCEpisode(t, cap)
	defer cancel()

	m.statfsFreeFn = func(ctx context.Context, dir string) (int64, error) {
		return 1 << 40, nil
	}
	var probes atomic.Int32
	m.probeFn = func(ctx context.Context, dir string, bytes int64) error {
		if probes.Add(1) <= 3 {
			return fmt.Errorf("probe: %w", syscall.ENOSPC)
		}
		return nil
	}

	m.noteIOError(runCtx, fmt.Errorf("write: %w", syscall.ENOSPC))
	waitFor(t, "resume after probe success", func() bool {
		return !m.Snapshot().ENOSPCPaused
	})
	if got := probes.Load(); got != 4 {
		t.Errorf("probes = %d, want 4 (3 failures then success)", got)
	}

	// De-flap: exactly one pause log, exactly one resume log, probe
	// failures at Debug (below the 10th-failure Warn threshold).
	if n := countLog(cap, slog.LevelWarn, "pausing download and install queues"); n != 1 {
		t.Errorf("pause logs = %d, want 1", n)
	}
	if n := countLog(cap, slog.LevelInfo, "resuming queues"); n != 1 {
		t.Errorf("resume logs = %d, want 1", n)
	}
	if n := countLog(cap, slog.LevelWarn, "write probe keeps failing"); n != 0 {
		t.Errorf("Warn probe-failure logs = %d, want 0 (Debug until every 10th)", n)
	}
	if n := countLog(cap, slog.LevelDebug, "write probe failed"); n != 3 {
		t.Errorf("Debug probe-failure logs = %d, want 3", n)
	}
}

// TestENOSPCProbeErrorAtTenth: every 10th probe failure logs at Warn.
func TestENOSPCProbeErrorAtTenth(t *testing.T) {
	cap := &logCapture{}
	m, runCtx, cancel := setupENOSPCEpisode(t, cap)
	defer cancel()

	m.statfsFreeFn = func(ctx context.Context, dir string) (int64, error) {
		return 1 << 40, nil
	}
	m.probeFn = func(ctx context.Context, dir string, bytes int64) error {
		return fmt.Errorf("probe: %w", syscall.ENOSPC)
	}
	m.probeGiveUpLimit = 11

	m.noteIOError(runCtx, fmt.Errorf("write: %w", syscall.ENOSPC))
	waitFor(t, "escalation at bound", func() bool {
		return m.joinedErrors() != nil
	})
	if n := countLog(cap, slog.LevelWarn, "write probe keeps failing"); n != 1 {
		t.Errorf("Warn probe-failure logs = %d, want 1 (every 10th of 11)", n)
	}
}

// TestENOSPCDemandScaledProbe: a reservation quota that rejects
// allocations ≥ X but allows smaller ones must NOT be passed by an
// undersized probe — with demand > X the pause holds and escalates; with
// demand < X it resumes.
func TestENOSPCDemandScaledProbe(t *testing.T) {
	quota := int64(128 << 20)
	newQuotaEnv := func() (*Manager, context.Context, context.CancelFunc, *atomic.Int64) {
		m, runCtx, cancel := setupENOSPCEpisode(t, nil)
		m.statfsFreeFn = func(ctx context.Context, dir string) (int64, error) {
			return 1 << 40, nil // statfs always looks free (the quota case)
		}
		var probes atomic.Int64
		m.probeFn = func(ctx context.Context, dir string, bytes int64) error {
			probes.Add(1)
			if bytes >= quota {
				return fmt.Errorf("probe(%d): %w", bytes, syscall.EDQUOT)
			}
			return nil
		}
		return m, runCtx, cancel, &probes
	}

	// demand (256MiB default) > quota: probe is demand-sized, keeps
	// failing, escalates — never a flapping resume.
	m, runCtx, cancel, _ := newQuotaEnv()
	defer cancel()
	m.probeGiveUpLimit = 3
	m.noteIOError(runCtx, fmt.Errorf("fallocate: %w", syscall.EDQUOT))
	waitFor(t, "escalation under quota", func() bool {
		return m.joinedErrors() != nil && !m.Snapshot().ENOSPCPaused
	})
	var oos *OutOfSpaceError
	if !errors.As(m.joinedErrors(), &oos) {
		t.Fatalf("joined = %v, want OutOfSpaceError", m.joinedErrors())
	}

	// Quota above the demand: the demand-sized probe succeeds, resume.
	m2, runCtx2, cancel2 := setupENOSPCEpisode(t, nil)
	defer cancel2()
	m2.statfsFreeFn = func(ctx context.Context, dir string) (int64, error) {
		return 1 << 40, nil
	}
	var sawSize atomic.Int64
	m2.probeFn = func(ctx context.Context, dir string, bytes int64) error {
		sawSize.Store(bytes)
		if bytes >= 1<<30 { // 1GiB quota: above the 256MiB demand
			return fmt.Errorf("probe(%d): %w", bytes, syscall.EDQUOT)
		}
		return nil
	}
	m2.noteIOError(runCtx2, fmt.Errorf("write: %w", syscall.ENOSPC))
	waitFor(t, "resume under high quota", func() bool { return !m2.Snapshot().ENOSPCPaused })
	if got := sawSize.Load(); got < enospcResumeBytes {
		t.Errorf("probe size = %d, want demand-sized (>= %d)", got, enospcResumeBytes)
	}
}

// TestENOSPCBackoffPersistsAcrossEpisodes: the exp-backoff state survives
// pause episodes within one run — a flapping volume reaches the cap and
// stays there.
func TestENOSPCBackoffPersistsAcrossEpisodes(t *testing.T) {
	m, runCtx, cancel := setupENOSPCEpisode(t, nil)
	defer cancel()
	m.statfsFreeFn = func(ctx context.Context, dir string) (int64, error) {
		return 1 << 40, nil
	}
	var failLeft atomic.Int32
	m.probeFn = func(ctx context.Context, dir string, bytes int64) error {
		if failLeft.Add(-1) >= 0 {
			return fmt.Errorf("probe: %w", syscall.ENOSPC)
		}
		return nil
	}

	// Episode 1: 6 failures then success → backoff has doubled 6 times
	// from the 2ms base (64ms) and must NOT reset.
	failLeft.Store(6)
	m.noteIOError(runCtx, fmt.Errorf("write: %w", syscall.ENOSPC))
	waitFor(t, "episode 1 resume", func() bool { return !m.Snapshot().ENOSPCPaused })
	b1 := time.Duration(m.enospcBackoffNs.Load())
	if b1 < 32*time.Millisecond {
		t.Fatalf("backoff after episode 1 = %v, want >= 32ms (persisted)", b1)
	}

	// Episode 2: starts from the persisted backoff, not the base.
	failLeft.Store(1)
	m.noteIOError(runCtx, fmt.Errorf("write: %w", syscall.ENOSPC))
	start := time.Now()
	waitFor(t, "episode 2 resume", func() bool { return !m.Snapshot().ENOSPCPaused })
	if elapsed := time.Since(start); elapsed < b1/2 {
		t.Errorf("episode 2 resumed in %v — backoff reset (persisted was %v)", elapsed, b1)
	}

	// Cap (small seam value): failures drive the backoff to the cap and
	// it stays there across failures — a flapping volume sits at the cap.
	m.probeBackoffMax = 64 * time.Millisecond
	failLeft.Store(12)
	m.noteIOError(runCtx, fmt.Errorf("write: %w", syscall.ENOSPC))
	waitFor(t, "cap episode resume", func() bool { return !m.Snapshot().ENOSPCPaused })
	if got := time.Duration(m.enospcBackoffNs.Load()); got != 64*time.Millisecond {
		t.Errorf("backoff = %v, want capped 64ms", got)
	}
}

func countLog(c *logCapture, level slog.Level, substr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.recs {
		if r.level == level && strings.Contains(r.msg, substr) {
			n++
		}
	}
	return n
}
