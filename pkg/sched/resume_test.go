package sched

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/transfer"
)

// TestResumeAfterCancel: cancel mid-run, then a new Manager on the same DB
// completes the file without re-fetching any byte the first run served
// (durable-only checkpoint resume: snapshot flushed ranges, fsync, persist).
func TestResumeAfterCancel(t *testing.T) {
	want := makeContent(8<<20, 37)
	hub := newFixtureHub(t, map[string][]byte{"big.bin": want}, "big.bin")
	hub.delay = 40 * time.Millisecond // ~8 blocks at 1MiB → cancel lands mid-run
	env := newTestEnv(t, hub)

	runCtx, cancel := context.WithCancel(t.Context())
	if err := env.manager.Submit(runCtx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- env.manager.Run(runCtx) }()

	// Cancel once at least one block is durably done.
	waitFor(t, "first completed block", func() bool {
		var n int64
		err := env.st.DB().QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM blocks WHERE status = 'done'").Scan(&n)
		return err == nil && n >= 1
	})
	cancel()
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Run = %v, want context.Canceled", err)
	}
	// Durable state after the cancel: the checkpointed interval set bounds
	// what run 2 may refetch (nothing already fsynced).
	var blob []byte
	if err := env.st.DB().QueryRowContext(t.Context(),
		"SELECT progress FROM files WHERE path = 'big.bin'").Scan(&blob); err != nil {
		t.Fatalf("load progress: %v", err)
	}
	var set transfer.IntervalSet
	covered := int64(0)
	if len(blob) > 0 {
		if err := set.UnmarshalBinary(blob); err != nil {
			t.Fatalf("progress blob corrupt after cancel: %v", err)
		}
		for _, iv := range set.Missing(int64(len(want))) {
			covered += iv.End - iv.Start
		}
		covered = int64(len(want)) - covered
	}
	t.Logf("after cancel: %d of %d bytes durably checkpointed", covered, len(want))
	if covered <= 0 {
		t.Fatalf("nothing checkpointed before cancel")
	}
	servedBefore := hub.payloadBytes()

	// New manager, same DB, no artificial delay: finishes from the blob.
	hub.delay = 0
	m2 := env.rebuild(t)
	if err := m2.Run(t.Context()); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	got, err := os.ReadFile(env.installedSnapshotPath("org/repo", "main", hub.sha, "big.bin"))
	if err != nil {
		t.Fatalf("read installed: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("installed content mismatch after resume")
	}
	// Run 2 fetched exactly the un-checkpointed remainder: no fsynced byte
	// was re-leased — durable-only checkpoints mean fsynced bytes are never
	// re-fetched.
	if served2 := hub.payloadBytes() - servedBefore; served2 != int64(len(want))-covered {
		t.Errorf("run 2 served %d bytes, want %d (size - checkpointed)", served2, int64(len(want))-covered)
	}
}

// TestRecoverStartupRequeues: rows stranded mid-lease (crash simulation)
// are requeued by the startup Recover and complete.
func TestRecoverStartupRequeues(t *testing.T) {
	want := makeContent(2<<20, 41)
	hub := newFixtureHub(t, map[string][]byte{"r.bin": want}, "r.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	// Seed a listed repo + queued file via a dry-run... simpler: submit and
	// run just the meta phase by starting and cancelling quickly? Instead
	// seed directly: enqueue + fake listing through the real meta worker.
	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	// Drive listing manually: lease the repo and complete it like the meta
	// worker would, then strand the file mid-download.
	repo, tok, err := env.st.LeaseMeta(ctx, time.Now())
	if err != nil {
		t.Fatalf("LeaseMeta: %v", err)
	}
	if err := env.st.SetCommitSHA(ctx, repo.ID, tok, hub.sha); err != nil {
		t.Fatalf("SetCommitSHA: %v", err)
	}
	if err := env.st.CompleteListing(ctx, repo.ID, tok, []store.FileEntry{{
		Path: "r.bin", Size: int64(len(want)),
		SHA256: sha256HexOf(want), IsLFS: true,
	}}); err != nil {
		t.Fatalf("CompleteListing: %v", err)
	}
	var fileID int64
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT id FROM files WHERE path = 'r.bin'").Scan(&fileID); err != nil {
		t.Fatal(err)
	}
	ftok, err := env.st.LeaseFileForDownload(ctx, fileID, time.Now())
	if err != nil {
		t.Fatalf("LeaseFileForDownload: %v", err)
	}
	if err := env.st.ReplacePendingBlocks(ctx, fileID, []store.Block{
		{FileID: fileID, Idx: 0, Offset: 0, Length: int64(len(want))},
	}); err != nil {
		t.Fatalf("ReplacePendingBlocks: %v", err)
	}
	lbs, err := env.st.LeaseBlocks(ctx, 1, store.BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if err != nil || len(lbs) != 1 {
		t.Fatalf("LeaseBlocks: %v %d", err, len(lbs))
	}
	// Crash: force every lease expired. A fresh manager's startup Recover
	// must requeue file + block and finish the download.
	if _, err := env.st.DB().ExecContext(ctx,
		"UPDATE files SET lease_until = ? WHERE id = ?", time.Now().Add(-time.Hour).UTC(), fileID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.st.DB().ExecContext(ctx,
		"UPDATE blocks SET lease_until = ? WHERE file_id = ?", time.Now().Add(-time.Hour).UTC(), fileID); err != nil {
		t.Fatal(err)
	}
	_ = ftok

	m2 := env.rebuild(t)
	if err := m2.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := env.fileStatus(t, "r.bin"); got != string(store.FileCached) {
		t.Errorf("file status = %s, want cached (Recover requeued the stranded rows)", got)
	}
}

// TestSetLimitsHot: bandwidth bucket and per-file conns update live; pause
// suspends the download and install queues.
func TestSetLimitsHot(t *testing.T) {
	want := makeContent(8<<20, 43)
	hub := newFixtureHub(t, map[string][]byte{"big.bin": want}, "big.bin")
	hub.delay = 30 * time.Millisecond
	env := newTestEnv(t, hub, withLimits(func(l *config.Limits) { l.Conns = 1 }))
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- env.manager.Run(ctx) }()

	// Wait for the download to start (one connection).
	waitFor(t, "download started", func() bool {
		return env.reg.Snapshot().Conns >= 1
	})

	// Hot bandwidth change: observable on the bucket.
	l := env.manager.currentLimits()
	l.MaxBandwidthBps = 12345678
	l.Conns = 4
	env.manager.SetLimits(l)
	if got := env.bw.Stats().Rate; got != 12345678 {
		t.Errorf("bandwidth bucket rate = %d, want 12345678 after SetLimits", got)
	}

	// Hot conns: SetParallelism grows the live worker set; the fixture
	// observes more than one concurrent request for the file.
	waitFor(t, "parallelism grew", func() bool {
		return env.reg.Snapshot().Conns > 1
	})

	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestSetLimitsPause: both-zero is the pause toggle.
func TestSetLimitsPause(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{"x": []byte("y")})
	env := newTestEnv(t, hub)
	l := env.manager.currentLimits()
	l.MaxBandwidthBps = 0
	l.DiskActivePct = 0
	env.manager.SetLimits(l)
	if !env.manager.Snapshot().Paused {
		t.Fatal("Paused = false after zero/zero SetLimits")
	}
	l.DiskActivePct = 100
	env.manager.SetLimits(l)
	if env.manager.Snapshot().Paused {
		t.Fatal("Paused = true after resume SetLimits")
	}
}

// TestPersistedLimits: settings survive a manager rebuild via kv.
func TestPersistedLimits(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{"x": []byte("y")})
	env := newTestEnv(t, hub)
	runCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env.manager.detached = context.WithoutCancel(runCtx)

	l := env.manager.currentLimits()
	l.MaxBandwidthBps = 424242
	env.manager.SetLimits(l)

	// A rebuilt manager loads the persisted limits at Run start. Drive a
	// trivial run: submit nothing, Run drains immediately.
	m2 := env.rebuild(t)
	if err := m2.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := m2.currentLimits().MaxBandwidthBps; got != 424242 {
		t.Errorf("rebuilt manager limits bandwidth = %d, want 424242 (persisted)", got)
	}
}

// TestSnapshotSanity: fields are populated and consistent after a run.
func TestSnapshotSanity(t *testing.T) {
	files := map[string][]byte{
		"a.bin": makeContent(1<<20, 47),
		"b.bin": makeContent(2<<20, 53),
	}
	hub := newFixtureHub(t, files, "a.bin", "b.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	s := env.manager.Snapshot()
	if s.Running {
		t.Error("Running = true after drain")
	}
	if s.Repo != "org/repo" || s.Revision != "main" {
		t.Errorf("repo header = %s@%s", s.Repo, s.Revision)
	}
	if s.CommitSHA != hub.sha {
		t.Errorf("CommitSHA = %q, want %q", s.CommitSHA, hub.sha)
	}
	if s.RepoStatus != string(store.RepoListed) {
		t.Errorf("RepoStatus = %q, want listed", s.RepoStatus)
	}
	var total int64
	for _, c := range files {
		total += int64(len(c))
	}
	if s.BytesTotal != total || s.BytesDone != total {
		t.Errorf("bytes = %d/%d, want %d/%d", s.BytesDone, s.BytesTotal, total, total)
	}
	if s.FilesTotal != 2 || s.FilesDone != 2 {
		t.Errorf("files = %d/%d, want 2/2", s.FilesDone, s.FilesTotal)
	}
	if s.PendingCount != 0 {
		t.Errorf("PendingCount = %d, want 0", s.PendingCount)
	}
	if len(s.Active) != 0 {
		t.Errorf("Active = %d, want 0 after drain", len(s.Active))
	}
	if s.DutyLevel != 100 {
		t.Errorf("DutyLevel = %d, want 100", s.DutyLevel)
	}
	if s.Queues[1].Depth != 0 {
		t.Errorf("download queue depth = %d, want 0", s.Queues[1].Depth)
	}
	if s.ETA != -1 && s.ETA != 0 {
		t.Errorf("ETA = %v, want -1 (unknown) or 0", s.ETA)
	}
	if _, err := os.Stat(filepath.Join(env.cache.Root(), "blobs")); err != nil {
		t.Errorf("blobs dir missing: %v", err)
	}
}

func sha256HexOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
