package sched

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/transfer"
)

// TestStallBackoffLeaserWaits reproduces the e2e tail-stall: a blackholed
// block GET is stall-killed, the block goes into durable backoff, and the
// leaser must wait the backoff out instead of reporting "no work" — the
// file completes with no Recover rescue (recoverInterval set huge).
func TestStallBackoffLeaserWaits(t *testing.T) {
	content := makeContent(1<<20, 103) // one 1MiB block, one connection
	hub := newFixtureHub(t, map[string][]byte{"m.bin": content}, "m.bin")
	hub.hangLeft["m.bin"] = 1 // first GET blackholes until the stall kill
	cap := &logCapture{}
	env := newTestEnvLog(t, hub, slog.New(cap), withLimits(func(l *config.Limits) {
		l.StallTimeout = 300 * time.Millisecond
	}))
	env.manager.recoverInterval = time.Hour // no periodic Recover rescue
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	start := time.Now()
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)

	// Exactly one stall cycle: hung attempt, then the good one after the
	// ~1s durable backoff (waited out by the leaser).
	if got := hub.resolveHits("m.bin"); got != 2 {
		t.Errorf("resolve hits = %d, want 2 (blackhole + retry)", got)
	}
	if got := env.fileStatus(t, "m.bin"); got != string(store.FileCached) {
		t.Errorf("file status = %s, want cached", got)
	}
	if elapsed > 30*time.Second {
		t.Errorf("completion took %v — leaser did not hide backoff (lease-expiry stall)", elapsed)
	}
	if !cap.has(slog.LevelInfo, "stall kill") {
		t.Errorf("no Info-level stall kill log captured")
	}
}

// TestRunFileNilCompletionHeals: a downloader that returns nil without
// completing the file (blocks still pending) must be caught by runFile's
// completion check — warned and requeued, not parked until lease expiry.
func TestRunFileNilCompletionHeals(t *testing.T) {
	content := makeContent(1<<20, 107)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": content}, "m.bin")
	cap := &logCapture{}
	env := newTestEnvLog(t, hub, slog.New(cap))
	env.manager.recoverInterval = time.Hour

	real := env.manager.runDownload
	var calls atomic.Int32
	env.manager.runDownload = func(ctx context.Context, task *transfer.FileTask, sink *fcio.File, progress transfer.ProgressSink) error {
		if calls.Add(1) == 1 {
			return nil // lie: nothing downloaded, blocks still pending
		}
		return real(ctx, task, sink, progress)
	}
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := env.fileStatus(t, "m.bin"); got != string(store.FileCached) {
		t.Errorf("file status = %s, want cached (nil-completion healed)", got)
	}
	if !cap.has(slog.LevelWarn, "returned without completing") {
		t.Errorf("no Warn log for nil-completion detection")
	}
	if n := calls.Load(); n < 2 {
		t.Errorf("runDownload called %d times, want >= 2 (requeue then real run)", n)
	}
}
