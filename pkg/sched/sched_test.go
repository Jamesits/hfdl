package sched

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/store"
)

// TestHappyPath drives a full run against the fixture Hub: listing →
// download → verify → publish → cache-mode install with the huggingface_hub
// layout.
func TestHappyPath(t *testing.T) {
	files := map[string][]byte{
		"config.json": []byte(`{"arch":"test"}`),
		"model.bin":   makeContent(3<<20, 7),
	}
	hub := newFixtureHub(t, files, "model.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Job + files terminal.
	if got := env.fileStatus(t, "config.json"); got != string(store.FileCached) {
		t.Errorf("config.json status = %s, want cached", got)
	}
	if got := env.fileStatus(t, "model.bin"); got != string(store.FileCached) {
		t.Errorf("model.bin status = %s, want cached", got)
	}

	// Blob published with correct content.
	for path, want := range files {
		var blobID string
		err := env.st.DB().QueryRowContext(ctx,
			"SELECT CASE WHEN is_lfs THEN sha256 ELSE git_oid END FROM files WHERE path = ?", path).Scan(&blobID)
		if err != nil {
			t.Fatalf("blob id %s: %v", path, err)
		}
		got, err := os.ReadFile(env.cache.BlobPath(blobID))
		if err != nil {
			t.Fatalf("read blob %s: %v", path, err)
		}
		if hex.EncodeToString(got[:min(len(got), 16)]) != hex.EncodeToString(want[:min(len(want), 16)]) || len(got) != len(want) {
			t.Errorf("blob %s content mismatch (len %d != %d)", path, len(got), len(want))
		}
	}

	// Cache-mode install layout: refs/<rev> + snapshots/<sha>/<path> as a
	// relative symlink into blobs/.
	refPath := filepath.Join(env.cache.Root(), "models--org--repo", "refs", "main")
	sha, err := os.ReadFile(refPath)
	if err != nil {
		t.Fatalf("read refs/main: %v", err)
	}
	if string(sha) != hub.sha {
		t.Errorf("refs/main = %q, want %q", sha, hub.sha)
	}
	for path, want := range files {
		final := env.installedSnapshotPath("org/repo", hub.sha, path)
		target, err := os.Readlink(final)
		if err != nil {
			t.Fatalf("readlink %s: %v (want symlink)", final, err)
		}
		if filepath.IsAbs(target) {
			t.Errorf("snapshot link %s is absolute %q, want relative", final, target)
		}
		got, err := os.ReadFile(final)
		if err != nil {
			t.Fatalf("read installed %s: %v", final, err)
		}
		if len(got) != len(want) {
			t.Errorf("installed %s len %d != %d", final, len(got), len(want))
		}
	}

	// Every file fetched exactly once, tree+revision hit once.
	if got := hub.hitsFor("tree"); got != 1 {
		t.Errorf("tree hits = %d, want 1", got)
	}
	if got := hub.hitsFor("revision"); got != 1 {
		t.Errorf("revision hits = %d, want 1", got)
	}
	if got := hub.resolveHits("model.bin"); got != 3 { // 3MiB / 1MiB blocks
		t.Errorf("model.bin resolve hits = %d, want 3", got)
	}

	// Jobs done.
	var jobsLeft int64
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM jobs WHERE status <> 'done'").Scan(&jobsLeft); err != nil {
		t.Fatal(err)
	}
	if jobsLeft != 0 {
		t.Errorf("%d jobs not done", jobsLeft)
	}
}

// TestExplicitFilenames uses positional files: hf lists the full tree
// (tree cache parity) and filters client-side; only the named file is
// installed.
func TestExplicitFilenames(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{
		"a.txt": []byte("aaa"),
		"b.txt": []byte("bbb"),
	})
	env := newTestEnv(t, hub)
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo", Filenames: []string{"a.txt"}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := hub.hitsFor("tree"); got != 1 {
		t.Errorf("tree hits = %d, want 1 (hf always lists the full tree)", got)
	}
	if got := hub.hitsFor("paths-info"); got != 0 {
		t.Errorf("paths-info hits = %d, want 0", got)
	}
	if got := env.fileStatus(t, "a.txt"); got != string(store.FileCached) {
		t.Errorf("a.txt status = %s, want cached", got)
	}
	if _, err := os.Stat(env.installedSnapshotPath("org/repo", hub.sha, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("b.txt installed but was not selected")
	}
	if got := hub.resolveHits("b.txt"); got != 0 {
		t.Errorf("b.txt downloaded %d times despite explicit selection", got)
	}
}

// TestIncludeExcludeFnmatch exercises python-fnmatch filtering: `*` crosses
// path separators.
func TestIncludeExcludeFnmatch(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{
		"top.txt":    []byte("t"),
		"a/b/c.bin":  makeContent(128, 3),
		"a/b/c.txt":  []byte("c"),
		"a/deep.bin": makeContent(64, 4),
	})
	env := newTestEnv(t, hub)
	ctx := t.Context()

	// "*.bin" must match nested paths (python fnmatch: * crosses /).
	job := Job{Repo: "org/repo", Include: []string{"*.bin"}}
	if err := env.manager.Submit(ctx, job); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, p := range []string{"a/b/c.bin", "a/deep.bin"} {
		if got := env.fileStatus(t, p); got != string(store.FileCached) {
			t.Errorf("%s status = %s, want cached", p, got)
		}
		if _, err := os.Stat(env.installedSnapshotPath("org/repo", hub.sha, p)); err != nil {
			t.Errorf("%s not installed: %v", p, err)
		}
	}
	// Non-matching files never entered the pipeline.
	var n int64
	if err := env.st.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM files").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("files rows = %d, want 2 (include filter applied at listing)", n)
	}
}

// TestExcludeFnmatch: exclusion wins over inclusion, `*` crossing `/`.
func TestExcludeFnmatch(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{
		"keep/a.txt":  []byte("k"),
		"drop/b.txt":  []byte("d"),
		"keep/deep/c": []byte("c"),
	})
	env := newTestEnv(t, hub)
	ctx := t.Context()

	job := Job{Repo: "org/repo", Include: []string{"*"}, Exclude: []string{"drop/*"}}
	if err := env.manager.Submit(ctx, job); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var n int64
	if err := env.st.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM files").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("files rows = %d, want 2 (drop/* excluded)", n)
	}
	if got := env.fileStatus(t, "keep/deep/c"); got != string(store.FileCached) {
		t.Errorf("keep/deep/c status = %s, want cached", got)
	}
}

// TestMaxWorkersSerializes caps the active download set at one file: the
// fixture never sees two distinct files in flight at once.
func TestMaxWorkersSerializes(t *testing.T) {
	files := map[string][]byte{
		"f1.bin": makeContent(4<<20, 11),
		"f2.bin": makeContent(4<<20, 13),
		"f3.bin": makeContent(4<<20, 17),
	}
	hub := newFixtureHub(t, files)
	hub.delay = 30 * time.Millisecond
	env := newTestEnv(t, hub, withLimits(func(l *config.Limits) {
		l.MaxWorkers = 1
	}))
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := hub.maxConcurrentPaths(); got != 1 {
		t.Errorf("max concurrent files = %d, want 1 (MaxWorkers=1)", got)
	}
	for p := range files {
		if got := env.fileStatus(t, p); got != string(store.FileCached) {
			t.Errorf("%s status = %s, want cached", p, got)
		}
	}
}
