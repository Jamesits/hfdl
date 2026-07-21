//go:build unix

package sched

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestENOSPCPauseResume: an ENOSPC error pauses the download/install
// queues; the statfs watcher resumes them once space returns.
func TestENOSPCPauseResume(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{"x": []byte("y")})
	env := newTestEnv(t, hub)

	var free atomic.Int64
	free.Store(0) // disk "full"
	env.manager.statfsFreeFn = func(ctx context.Context, dir string) (int64, error) {
		return free.Load(), nil
	}
	env.manager.enospcPoll = 10 * time.Millisecond

	// Simulate a run context for the watcher.
	runCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env.manager.detached = context.WithoutCancel(runCtx)

	if !env.manager.noteIOError(runCtx, fmt.Errorf("write failed: %w", unix.ENOSPC)) {
		t.Fatal("noteIOError did not classify ENOSPC")
	}
	if !env.manager.Snapshot().ENOSPCPaused {
		t.Fatal("ENOSPCPaused = false, want true")
	}
	// A download worker would block now.
	gateCtx, gateCancel := context.WithTimeout(runCtx, 100*time.Millisecond)
	defer gateCancel()
	if err := env.manager.waitResumable(gateCtx); err == nil {
		t.Fatal("waitResumable returned during ENOSPC pause")
	}

	// Space returns: the watcher lifts the pause.
	free.Store(1 << 40)
	waitFor(t, "ENOSPC resume", func() bool {
		return !env.manager.Snapshot().ENOSPCPaused
	})
	if err := env.manager.waitResumable(runCtx); err != nil {
		t.Fatalf("waitResumable after resume: %v", err)
	}

	// Non-ENOSPC errors do not pause.
	if env.manager.noteIOError(runCtx, errors.New("plain io error")) {
		t.Fatal("plain error classified as ENOSPC")
	}
}

// TestENOSPCClassifiesEDQUOT too.
func TestENOSPCClassifiesEDQUOT(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{"x": []byte("y")})
	env := newTestEnv(t, hub)
	runCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env.manager.enospcPoll = 10 * time.Millisecond
	env.manager.statfsFreeFn = func(ctx context.Context, dir string) (int64, error) {
		return 1 << 40, nil // space instantly available
	}
	if !env.manager.noteIOError(runCtx, fmt.Errorf("quota: %w", syscall.EDQUOT)) {
		t.Fatal("EDQUOT not classified as out-of-space")
	}
}
