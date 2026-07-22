package sched

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/cache"
	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/stats"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/throttle"
	"github.com/jamesits/hfdl/pkg/transfer"
	"github.com/jamesits/hfdl/pkg/verify"
)

// offlineManagerAt builds an offline manager with a FRESH state DB over an
// existing HF cache root (the restart-with-empty-DB offline scenario).
func offlineManagerAt(t *testing.T, hub *fixtureHub, dir, cacheRoot string) *Manager {
	t.Helper()
	ctx := t.Context()
	log := slog.New(slog.DiscardHandler)
	st, err := store.Open(ctx, filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close(ctx) })
	engine := fcio.NewEngine(log, st, fcio.TierAuto)
	pool := fcio.NewPool(0, 64<<20)
	volumes := fcio.NewVolumeSet()
	cs, err := cache.OpenStore(ctx, cacheRoot, engine, log)
	if err != nil {
		t.Fatalf("cache.OpenStore: %v", err)
	}
	bw := throttle.NewBucket(0, 0, 0)
	api := throttle.NewBucket(1000, 1000, 1000)
	duty := throttle.NewDutyLimiter(100, throttle.MediaSSD)
	reg := stats.New()
	dl := transfer.NewDownloader(transfer.Config{
		Log: log, HTTP: hub.srv.Client(), Bandwidth: bw, Stats: reg,
		Engine: engine, Pool: pool,
		CheckpointInterval: 50 * time.Millisecond, HeaderTimeout: 5 * time.Second,
	})
	limits := config.DefaultLimits()
	limits.Conns = 4
	limits.MaxWorkers = 2
	limits.BlockSize = 1 << 20
	client := hfapi.NewClient(log, hub.srv.Client(), hub.srv.URL, "", 5*time.Second)
	return NewManager(ManagerConfig{
		Store:      st,
		Clients:    map[string]*hfapi.Client{hub.srv.URL: client},
		Downloader: dl,
		Verifier:   verify.NewChecker(engine, pool, duty, log, nil),
		Installer:  cache.NewInstaller(cs, engine, volumes, duty, log, nil),
		Cache:      cs, Engine: engine, Pool: pool, Volumes: volumes,
		Bandwidth: bw, API: api, Duty: duty, Stats: reg,
		Log: log, Limits: limits, CacheDir: cacheRoot,
		Offline: true,
	})
}

// TestOfflineServeFromCacheLayoutFreshDB: fresh state DB, offline, cache
// layout present → served with zero network traffic.
func TestOfflineServeFromCacheLayoutFreshDB(t *testing.T) {
	content := makeContent(1<<20, 109)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": content}, "m.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	// Populate the cache layout online.
	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("online Run: %v", err)
	}
	hitsAfterOnline := 0
	for _, n := range hub.hits {
		hitsAfterOnline += n
	}

	// Fresh state DB, offline: the layout alone must serve the repo.
	m := offlineManagerAt(t, hub, t.TempDir(), env.cache.Root())
	if err := m.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("offline Submit: %v", err)
	}
	if err := m.Run(ctx); err != nil {
		t.Fatalf("offline Run: %v", err)
	}

	totalHits := 0
	for _, n := range hub.hits {
		totalHits += n
	}
	if totalHits != hitsAfterOnline {
		t.Errorf("offline mode made %d network requests", totalHits-hitsAfterOnline)
	}
	got, err := os.ReadFile(env.installedSnapshotPath("org/repo", "main", hub.sha, "m.bin"))
	if err != nil {
		t.Fatalf("read served file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("offline layout serve content mismatch")
	}
}

// TestOfflineMissTerminal: no cached revision → terminal error fast, no
// meta requeue.
func TestOfflineMissTerminal(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{"x": []byte("y")})
	env := newTestEnv(t, hub)
	env.manager.cfg.Offline = true
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	start := time.Now()
	err := env.manager.Run(ctx)
	var offErr *OfflineError
	if !errors.As(err, &offErr) {
		t.Fatalf("Run = %v, want *OfflineError", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("offline miss took %v — not failing fast", elapsed)
	}
	var status string
	if err := env.st.DB().QueryRowContext(ctx, "SELECT status FROM repos").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(store.RepoError) {
		t.Errorf("repo status = %s, want error (terminal, no requeue)", status)
	}
	if got := env.jobStatus(t, 1); got != string(store.JobError) {
		t.Errorf("job status = %s, want error", got)
	}
}

// TestOfflinePartialSnapshot: listed repo, one of two files present in the
// snapshot layout → present file served, missing file errors by name.
func TestOfflinePartialSnapshot(t *testing.T) {
	present := makeContent(1<<20, 113)
	missing := makeContent(1<<20, 127)
	hub := newFixtureHub(t, map[string][]byte{
		"present.bin": present,
		"missing.bin": missing,
	}, "present.bin", "missing.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	// List without downloading (dry-run leaves files 'queued').
	dry := newTestEnvLog(t, hub, slog.New(slog.DiscardHandler))
	_ = dry // separate env would have its own DB; use env directly below
	// Dry-run via env's manager clone:
	mDry := env.rebuild(t)
	mDry.cfg.DryRun = true
	if err := mDry.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := mDry.Run(ctx); err != nil {
		t.Fatalf("dry Run: %v", err)
	}
	if got := env.fileStatus(t, "present.bin"); got != string(store.FileQueued) {
		t.Fatalf("present.bin status = %s, want queued after dry-run", got)
	}

	// Craft a partial layout: blob + snapshot symlink for present.bin only.
	presentID := (&fixtureFile{content: present, lfs: true}).blobID()
	blobPath := env.cache.BlobPath(presentID)
	if err := os.WriteFile(blobPath, present, 0o644); err != nil {
		t.Fatal(err)
	}
	snapDir := filepath.Join(env.cache.Root(), "models--org--repo", "snapshots", hub.sha)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(snapDir, blobPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(rel, filepath.Join(snapDir, "present.bin")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(env.cache.Root(), "models--org--repo", "refs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(env.cache.Root(), "models--org--repo", "refs", "main"), []byte(hub.sha), 0o644); err != nil {
		t.Fatal(err)
	}

	// Offline: present.bin serves from the layout; missing.bin errors by name.
	m := env.rebuild(t)
	m.cfg.Offline = true
	if err := m.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("offline Submit: %v", err)
	}
	err = m.Run(ctx)
	var offErr *OfflineError
	if !errors.As(err, &offErr) {
		t.Fatalf("Run = %v, want *OfflineError naming missing.bin", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("missing.bin")) {
		t.Errorf("error does not name the missing file: %v", err)
	}
	if got := env.fileStatus(t, "present.bin"); got != string(store.FileCached) {
		t.Errorf("present.bin status = %s, want cached (served from layout)", got)
	}
}

// TestMissingExplicitFilenameTerminal: an explicit file the repo does not
// carry fails the job terminally (hf: "File not found in repository.").
func TestMissingExplicitFilenameTerminal(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{"real.txt": []byte("r")})
	env := newTestEnv(t, hub)
	ctx := t.Context()

	err := env.manager.Submit(ctx, Job{Repo: "org/repo", Filenames: []string{"real.txt", "missing-file.txt"}})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	err = env.manager.Run(ctx)
	var nr *NotInRepoError
	if !errors.As(err, &nr) {
		t.Fatalf("Run = %v, want *NotInRepoError", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("missing-file.txt")) {
		t.Errorf("error does not name the missing file: %v", err)
	}
	var status string
	if err := env.st.DB().QueryRowContext(ctx, "SELECT status FROM repos").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(store.RepoError) {
		t.Errorf("repo status = %s, want error (terminal)", status)
	}
}

// TestDryRunReport: report lists the selected files with Cached=false when
// nothing is cached.
func TestDryRunReport(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{
		"a.bin": makeContent(1<<20, 131),
		"b.txt": []byte("hello"),
	}, "a.bin")
	env := newTestEnv(t, hub)
	env.manager.cfg.DryRun = true
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	report := env.manager.DryRunReport()
	if len(report) != 2 {
		t.Fatalf("report = %d entries, want 2", len(report))
	}
	byPath := map[string]DryRunEntry{}
	for _, e := range report {
		byPath[e.Path] = e
	}
	a := byPath["a.bin"]
	if a.Size != 1<<20 || a.Cached {
		t.Errorf("a.bin entry = %+v, want size 1MiB uncached", a)
	}
	wantDest := filepath.Join(env.cache.Root(), "models--org--repo", "snapshots", hub.sha, "a.bin")
	if a.DestPath != wantDest {
		t.Errorf("a.bin DestPath = %q, want %q", a.DestPath, wantDest)
	}
	b := byPath["b.txt"]
	if b.Size != 5 || b.Cached {
		t.Errorf("b.txt entry = %+v", b)
	}
	// Listing order (files.id): a.bin listed before b.txt? Tree order from
	// the fixture is map iteration — just assert both present (order
	// covered by the ORDER BY f.id query).
}

// TestDryRunReportJobScoped: a prior job's files must not leak into the
// report of the latest dry-run job, and no file appears twice.
func TestDryRunReportJobScoped(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{
		"config.json":    []byte(`{"a":1}`),
		"tokenizer.json": []byte(`{"b":2}`),
	})
	env := newTestEnv(t, hub)
	env.manager.cfg.DryRun = true
	ctx := t.Context()

	// Prior job selecting both files.
	if err := env.manager.Submit(ctx, Job{Repo: "org/repo", Filenames: []string{"config.json", "tokenizer.json"}}); err != nil {
		t.Fatalf("Submit 1: %v", err)
	}
	// Latest dry-run job selecting only config.json.
	if err := env.manager.Submit(ctx, Job{Repo: "org/repo", Filenames: []string{"config.json"}}); err != nil {
		t.Fatalf("Submit 2: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	report := env.manager.DryRunReport()
	if len(report) != 1 {
		t.Fatalf("report = %d entries, want exactly 1 (this job's selection): %+v", len(report), report)
	}
	if report[0].Path != "config.json" {
		t.Errorf("report[0].Path = %q, want config.json", report[0].Path)
	}
}

// TestDryRunReportWarmCacheFreshDB: a fresh state DB over a warm HF cache
// reports Cached from the blob store, like hf.
func TestDryRunReportWarmCacheFreshDB(t *testing.T) {
	content := makeContent(1<<20, 137)
	hub := newFixtureHub(t, map[string][]byte{
		"warm.bin": content,
		"cold.bin": makeContent(64, 139),
	}, "warm.bin", "cold.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	// Warm the cache with only warm.bin (explicit filename → cold.bin never
	// listed or fetched).
	if err := env.manager.Submit(ctx, Job{Repo: "org/repo", Filenames: []string{"warm.bin"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("warm Run: %v", err)
	}
	if _, ok := env.cache.HasBlob((&fixtureFile{content: content, lfs: true}).blobID()); !ok {
		t.Fatalf("warm blob missing")
	}

	// Fresh state DB over the same cache, dry-run (network allowed).
	m := offlineManagerAt(t, hub, t.TempDir(), env.cache.Root())
	m.cfg.Offline = false
	m.cfg.DryRun = true
	if err := m.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("dry Submit: %v", err)
	}
	if err := m.Run(ctx); err != nil {
		t.Fatalf("dry Run: %v", err)
	}

	report := m.DryRunReport()
	if len(report) != 2 {
		t.Fatalf("report = %d entries, want 2", len(report))
	}
	byPath := map[string]DryRunEntry{}
	for _, e := range report {
		byPath[e.Path] = e
	}
	if !byPath["warm.bin"].Cached {
		t.Errorf("warm.bin Cached = false, want true (blob present in warm cache)")
	}
	if byPath["cold.bin"].Cached {
		t.Errorf("cold.bin Cached = true, want false")
	}
}
