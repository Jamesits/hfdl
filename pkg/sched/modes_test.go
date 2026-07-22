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
	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/transfer"
	"github.com/jamesits/hfdl/pkg/verify"
)

// TestDryRun: listing runs (dest paths computed), but zero payload moves
// and the job completes.
func TestDryRun(t *testing.T) {
	files := map[string][]byte{
		"a.bin":     makeContent(1<<20, 59),
		"sub/b.txt": []byte("hello"),
	}
	hub := newFixtureHub(t, files, "a.bin")
	env := newTestEnv(t, hub)

	// Rebuild the manager with DryRun.
	log := slog.New(slog.DiscardHandler)
	client := hfapi.NewClient(log, hub.srv.Client(), hub.srv.URL, "", 5*time.Second)
	engine := fcio.NewEngine(log, env.st, fcio.TierAuto)
	pool := fcio.NewPool(0, 64<<20)
	volumes := fcio.NewVolumeSet()
	m := NewManager(ManagerConfig{
		Store:      env.st,
		Clients:    map[string]*hfapi.Client{hub.srv.URL: client},
		Downloader: transfer.NewDownloader(transfer.Config{Log: log, HTTP: hub.srv.Client(), Bandwidth: env.bw, Stats: env.reg, Engine: engine, Pool: pool}),
		Verifier:   verify.NewChecker(engine, pool, env.duty, log, nil),
		Installer:  cache.NewInstaller(env.cache, engine, volumes, env.duty, log, nil),
		Cache:      env.cache, Engine: engine, Pool: pool, Volumes: volumes,
		Bandwidth: env.bw, API: env.api, Duty: env.duty, Stats: env.reg,
		Log: log, Limits: env.limits, CacheDir: env.cache.Root(),
		DryRun: true,
	})
	ctx := t.Context()
	if err := m.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := m.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Metadata ran; zero payload requests.
	if got := hub.hitsFor("tree"); got != 1 {
		t.Errorf("tree hits = %d, want 1", got)
	}
	if got := hub.hitsFor("resolve"); got != 0 {
		t.Errorf("resolve hits = %d, want 0 (dry-run moves no payload)", got)
	}
	// Job done with computed would-be paths.
	if got := env.jobStatus(t, 1); got != string(store.JobDone) {
		t.Errorf("job status = %s, want done", got)
	}
	for _, p := range []string{"a.bin", "sub/b.txt"} {
		var dest string
		err := env.st.DB().QueryRowContext(ctx,
			"SELECT jf.dest_path FROM job_files jf JOIN files f ON f.id = jf.file_id WHERE f.path = ?", p).Scan(&dest)
		if err != nil {
			t.Fatalf("dest_path %s: %v", p, err)
		}
		want := filepath.Join(env.cache.Root(), "models--org--repo", "snapshots", hub.sha, p)
		if dest != want {
			t.Errorf("dest_path %s = %q, want %q", p, dest, want)
		}
		if _, err := os.Stat(dest); !os.IsNotExist(err) {
			t.Errorf("dry-run created %s", dest)
		}
	}
	// Nothing downloaded into blobs.
	entries, _ := os.ReadDir(filepath.Join(env.cache.Root(), "blobs"))
	if len(entries) != 0 {
		t.Errorf("blobs published during dry-run: %d", len(entries))
	}
}

// TestOfflineServesCache: a file already in the cache installs in offline
// mode; a repo needing the network fails fast with a typed OfflineError.
func TestOfflineServesCache(t *testing.T) {
	content := makeContent(1<<20, 61)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": content}, "m.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	// Phase 1 (online): download into the cache and install cache-mode.
	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("online Run: %v", err)
	}
	if got := env.fileStatus(t, "m.bin"); got != string(store.FileCached) {
		t.Fatalf("phase 1 status = %s, want cached", got)
	}
	payloadAfterOnline := hub.payloadBytes()

	// Phase 2 (offline): same DB, new local-dir job → served from cache
	// with zero network traffic.
	m2 := env.rebuild(t)
	m2.cfg.Offline = true
	if err := m2.Submit(ctx, Job{
		Repo: "org/repo", DestMode: store.DestModeLocalDir,
		DestDir: filepath.Join(env.dir, "out"),
	}); err != nil {
		t.Fatalf("offline Submit: %v", err)
	}
	if err := m2.Run(ctx); err != nil {
		t.Fatalf("offline Run: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(env.dir, "out", "m.bin"))
	if err != nil {
		t.Fatalf("read offline-installed file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("offline install content mismatch")
	}
	// hf local-dir parity: the commit tree cache exists and names m.bin.
	treeJSON, err := os.ReadFile(filepath.Join(env.dir, "out", ".cache", "huggingface", "trees", hub.sha+".json"))
	if err != nil {
		t.Fatalf("tree cache missing: %v", err)
	}
	if !bytes.Contains(treeJSON, []byte(`"m.bin"`)) || !bytes.Contains(treeJSON, []byte(`"format_version"`)) {
		t.Errorf("tree cache content wrong: %s", treeJSON)
	}
	if hub.payloadBytes() != payloadAfterOnline {
		t.Errorf("offline mode served payload bytes")
	}
	if got := hub.hitsFor("tree"); got != 1 {
		t.Errorf("offline mode hit the tree endpoint: %d hits", got)
	}

	// Phase 3 (offline): a repo that needs listing fails fast, typed.
	m3 := env.rebuild(t)
	m3.cfg.Offline = true
	if err := m3.Submit(ctx, Job{Repo: "org/repo", Revision: "other-rev"}); err != nil {
		t.Fatalf("offline Submit: %v", err)
	}
	err = m3.Run(ctx)
	var offErr *OfflineError
	if !errors.As(err, &offErr) {
		t.Fatalf("offline unknown-repo Run = %v, want *OfflineError", err)
	}
}

// TestOfflineReapUncachedFile: offline with a listed-but-undownloaded file
// fails the job fast.
func TestOfflineReapUncachedFile(t *testing.T) {
	content := makeContent(1<<20, 67)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": content}, "m.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	// Dry-run first: lists the repo without downloading.
	log := slog.New(slog.DiscardHandler)
	client := hfapi.NewClient(log, hub.srv.Client(), hub.srv.URL, "", 5*time.Second)
	engine := fcio.NewEngine(log, env.st, fcio.TierAuto)
	pool := fcio.NewPool(0, 64<<20)
	volumes := fcio.NewVolumeSet()
	mDry := NewManager(ManagerConfig{
		Store:      env.st,
		Clients:    map[string]*hfapi.Client{hub.srv.URL: client},
		Downloader: transfer.NewDownloader(transfer.Config{Log: log, HTTP: hub.srv.Client(), Bandwidth: env.bw, Stats: env.reg, Engine: engine, Pool: pool}),
		Verifier:   verify.NewChecker(engine, pool, env.duty, log, nil),
		Installer:  cache.NewInstaller(env.cache, engine, volumes, env.duty, log, nil),
		Cache:      env.cache, Engine: engine, Pool: pool, Volumes: volumes,
		Bandwidth: env.bw, API: env.api, Duty: env.duty, Stats: env.reg,
		Log: log, Limits: env.limits, CacheDir: env.cache.Root(),
		DryRun: true,
	})
	if err := mDry.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := mDry.Run(ctx); err != nil {
		t.Fatalf("dry Run: %v", err)
	}

	// Offline now: the file is listed but not cached → job fails fast.
	m2 := env.rebuild(t)
	m2.cfg.Offline = true
	if err := m2.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("offline Submit: %v", err)
	}
	err := m2.Run(ctx)
	var offErr *OfflineError
	if !errors.As(err, &offErr) {
		t.Fatalf("offline Run = %v, want *OfflineError", err)
	}
	if got := hub.hitsFor("resolve"); got != 0 {
		t.Errorf("offline mode made %d resolve requests", got)
	}
}
