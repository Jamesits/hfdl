package sched

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/jamesits/hfdl/pkg/store"
)

// TestSalvageWholeFile: a --reference file with matching size+sha256 is
// copied into the cache; the file completes with ZERO resolve requests to
// the Hub (asserted at the fixture).
func TestSalvageWholeFile(t *testing.T) {
	content := makeContent(2<<20, 71)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": content}, "m.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	refDir := resolvedTempDir(t)
	refPath := filepath.Join(refDir, "local-copy.bin")
	if err := os.WriteFile(refPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo", References: []string{refDir}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := hub.hitsFor("resolve"); got != 0 {
		t.Errorf("resolve hits = %d, want 0 (fully salvaged)", got)
	}
	if got := env.fileStatus(t, "m.bin"); got != string(store.FileCached) {
		t.Errorf("file status = %s, want cached", got)
	}
	got, err := os.ReadFile(env.installedSnapshotPath("org/repo", hub.sha, "m.bin"))
	if err != nil {
		t.Fatalf("read installed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("salvaged content mismatch")
	}
	// The reference row is hashed; salvaged bytes counted.
	var refStatus string
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT status FROM reference_files WHERE path = ?", refPath).Scan(&refStatus); err != nil {
		t.Fatal(err)
	}
	if refStatus != string(store.RefHashed) {
		t.Errorf("reference status = %s, want hashed", refStatus)
	}
	if s := env.manager.Snapshot(); s.SalvagedBytes != int64(len(content)) {
		t.Errorf("SalvagedBytes = %d, want %d", s.SalvagedBytes, len(content))
	}
}

// TestSalvageCorruptReference: a reference that changes after hashing
// salvages bad bytes; the mandatory hash verify catches it, the file falls
// back to the network, and the final output is never the corrupt content.
// The reference is sized so the apply pass is still reading when the test
// corrupts it — the salvage startup (lease → match → acquireRW) plus the copy
// of tens of MiB comfortably outlasts the test's DB-poll + two pwrites, so the
// tail page lands corrupt no reliance on sub-millisecond interleavings.
func TestSalvageCorruptReference(t *testing.T) {
	content := makeContent(32<<20, 73)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": content}, "m.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	refDir := resolvedTempDir(t)
	refPath := filepath.Join(refDir, "local-copy.bin")
	if err := os.WriteFile(refPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo", References: []string{refDir}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- env.manager.Run(ctx) }()

	// Tight poll: the moment the reference is hashed, corrupt its first and
	// last pages in place (same size, two instant pwrites). The apply pass
	// reads sequentially, so the tail page lands corrupt no matter how far
	// the copy has progressed — the salvaged bytes can never verify.
	for {
		var st string
		err := env.st.DB().QueryRowContext(ctx,
			"SELECT status FROM reference_files WHERE path = ?", refPath).Scan(&st)
		if err == nil && st == string(store.RefHashed) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
		}
	}
	page := bytes.Repeat([]byte{0xEE}, 4096)
	cf, err := os.OpenFile(refPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cf.WriteAt(page, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := cf.WriteAt(page, int64(len(content))-int64(len(page))); err != nil {
		t.Fatal(err)
	}
	if err := cf.Close(); err != nil {
		t.Fatal(err)
	}

	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, err := os.ReadFile(env.installedSnapshotPath("org/repo", hub.sha, "m.bin"))
	if err != nil {
		t.Fatalf("read installed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("corrupt reference produced bad output")
	}
	if got := hub.hitsFor("resolve"); got == 0 {
		t.Errorf("no resolve hits — corrupt salvage must fall back to network")
	}
	var fails int
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT verify_fails FROM files WHERE path = 'm.bin'").Scan(&fails); err != nil {
		t.Fatal(err)
	}
	if fails != 1 {
		t.Errorf("verify_fails = %d, want 1 (salvaged bytes failed, network succeeded)", fails)
	}
}

// TestSalvageRejectsCacheAndDest: references resolving into the hfdl cache
// or the job destination are rejected at expansion — including via symlink.
func TestSalvageRejectsCacheAndDest(t *testing.T) {
	content := makeContent(1<<20, 79)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": content}, "m.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	// A file inside the cache root.
	insideCache := filepath.Join(env.cache.Root(), "blobs", "sneaky.bin")
	if err := os.WriteFile(insideCache, content, 0o644); err != nil {
		t.Fatal(err)
	}
	// A symlink in the reference dir pointing into the cache.
	refDir := resolvedTempDir(t)
	linkPath := filepath.Join(refDir, "via-symlink.bin")
	if err := os.Symlink(insideCache, linkPath); err != nil {
		t.Fatal(err)
	}
	// A legit reference next to it (different content, matching size is
	// irrelevant for this test — the point is only the legit row lands).
	legitPath := filepath.Join(refDir, "legit.bin")
	if err := os.WriteFile(legitPath, makeContent(1<<20, 83), 0o644); err != nil {
		t.Fatal(err)
	}
	// A reference inside the local-dir destination.
	destDir := filepath.Join(env.dir, "dest")
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatal(err)
	}
	inDest := filepath.Join(destDir, "out.bin")
	if err := os.WriteFile(inDest, content, 0o644); err != nil {
		t.Fatal(err)
	}

	job := Job{
		Repo: "org/repo", References: []string{refDir, insideCache, destDir},
		DestMode: store.DestModeLocalDir, DestDir: destDir,
	}
	if err := env.manager.Submit(ctx, job); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Only the legit reference row exists.
	var rows []string
	if err := env.st.DB().NewSelect().Model((*store.ReferenceFile)(nil)).
		Column("path").Scan(ctx, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0] != legitPath {
		t.Errorf("reference rows = %v, want only %q", rows, legitPath)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The target downloaded from the network (its size does not match the
	// legit reference's content hash).
	if got := hub.hitsFor("resolve"); got == 0 {
		t.Errorf("no resolve hits; target should have downloaded")
	}
}

// TestSalvageSizeGate: a reference whose size matches no pending target is
// never hashed (and hence never read).
func TestSalvageSizeGate(t *testing.T) {
	content := makeContent(1<<20, 89)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": content}, "m.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	refDir := resolvedTempDir(t)
	// 42 bytes: no target has this size.
	oddPath := filepath.Join(refDir, "odd-size.bin")
	if err := os.WriteFile(oddPath, makeContent(42, 97), 0o644); err != nil {
		t.Fatal(err)
	}
	infoBefore, err := os.Stat(oddPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo", References: []string{refDir}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Never leased for hashing: status stays pending.
	var st string
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT status FROM reference_files WHERE path = ?", oddPath).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != string(store.RefPending) {
		t.Errorf("size-mismatched reference status = %s, want pending (never read)", st)
	}
	infoAfter, err := os.Stat(oddPath)
	if err != nil {
		t.Fatal(err)
	}
	if !infoAfter.ModTime().Equal(infoBefore.ModTime()) {
		t.Errorf("size-mismatched reference was modified")
	}
	// The download itself succeeded via network.
	if got := hub.hitsFor("resolve"); got == 0 {
		t.Errorf("no resolve hits")
	}
}

// TestReferenceHashReuseAcrossRuns: a hashed reference is not re-hashed
// while its stat key is unchanged.
func TestReferenceHashReuseAcrossRuns(t *testing.T) {
	content := makeContent(1<<20, 101)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": content}, "m.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	refDir := resolvedTempDir(t)
	refPath := filepath.Join(refDir, "local-copy.bin")
	if err := os.WriteFile(refPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := env.manager.Submit(ctx, Job{Repo: "org/repo", References: []string{refDir}}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	var sha1 string
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT sha256 FROM reference_files WHERE path = ?", refPath).Scan(&sha1); err != nil {
		t.Fatal(err)
	}
	if sha1 == "" {
		t.Fatalf("reference not hashed after run 1")
	}

	// Second run with a fresh job for another revision... the same repo at
	// a different revision reuses the reference row without re-hashing:
	// status stays 'hashed' (AddReferenceFiles preserves it).
	m2 := env.rebuild(t)
	if err := m2.Submit(ctx, Job{Repo: "org/repo", References: []string{refDir}}); err != nil {
		t.Fatalf("Submit 2: %v", err)
	}
	var st string
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT status FROM reference_files WHERE path = ?", refPath).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != string(store.RefHashed) {
		t.Errorf("reference status after re-add = %s, want hashed (reused)", st)
	}
	if err := m2.Run(ctx); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if got := hub.hitsFor("resolve"); got != 0 {
		t.Errorf("run 2 resolve hits = %d, want 0 (salvaged again)", got)
	}
}
