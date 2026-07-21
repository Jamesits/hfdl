package sched

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/store"
)

// TestHashMismatchRequeue: the fixture serves corrupt bytes for the first
// resolve round; verify fails, the file resets and requeues, and the second
// (good) round verifies and installs.
func TestHashMismatchRequeue(t *testing.T) {
	want := makeContent(2<<20, 23)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": want}, "m.bin")
	// Corrupt exactly the first pass (2 blocks of 1MiB); the requeue after
	// the first verify failure fetches good bytes.
	hub.resolveWrong["m.bin"] = 2
	env := newTestEnv(t, hub)
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, err := os.ReadFile(env.installedSnapshotPath("org/repo", "main", hub.sha, "m.bin"))
	if err != nil {
		t.Fatalf("read installed: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("installed content mismatch after requeue")
	}
	var fails int
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT verify_fails FROM files WHERE path = 'm.bin'").Scan(&fails); err != nil {
		t.Fatal(err)
	}
	if fails != 1 {
		t.Errorf("verify_fails = %d, want 1", fails)
	}
}

// TestVerifyFailTwiceErrors: the fixture always serves corrupt bytes; after
// the second verify failure the file goes terminal-error and the job fails.
func TestVerifyFailTwiceErrors(t *testing.T) {
	want := makeContent(1<<20, 29)
	hub := newFixtureHub(t, map[string][]byte{"m.bin": want}, "m.bin")
	hub.resolveWrong["m.bin"] = 1 << 30 // always corrupt
	env := newTestEnv(t, hub)
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	err := env.manager.Run(ctx)
	if err == nil {
		t.Fatalf("Run succeeded, want verify-failure error")
	}
	if got := env.fileStatus(t, "m.bin"); got != string(store.FileError) {
		t.Errorf("file status = %s, want error", got)
	}
	var fails int
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT verify_fails FROM files WHERE path = 'm.bin'").Scan(&fails); err != nil {
		t.Fatal(err)
	}
	if fails != 2 {
		t.Errorf("verify_fails = %d, want 2", fails)
	}
	var jobErr string
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT status FROM jobs WHERE id = 1").Scan(&jobErr); err != nil {
		t.Fatal(err)
	}
	if jobErr != string(store.JobError) {
		t.Errorf("job status = %s, want error", jobErr)
	}
	// Never installed: the bad bytes must not reach the cache blob either.
	if _, err := os.Stat(env.installedSnapshotPath("org/repo", "main", hub.sha, "m.bin")); !os.IsNotExist(err) {
		t.Errorf("corrupt file was installed")
	}
}

// TestAPI429CooldownScoped: a 429 on the tree endpoint sets an
// (endpoint,'api') cooldown; the meta queue requeues and completes, and the
// download side never takes an upstream cooldown for the same endpoint.
func TestAPI429CooldownScoped(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{"m.bin": makeContent(1<<20, 31)}, "m.bin")
	hub.tree429Left = 1
	env := newTestEnv(t, hub)
	ctx := t.Context()

	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The tree was re-fetched after the cooldown.
	if got := hub.hitsFor("tree"); got != 2 {
		t.Errorf("tree hits = %d, want 2 (429 then retry)", got)
	}
	// Cooldown persisted, scoped to (endpoint, api).
	until, err := env.st.CooldownUntil(ctx, hub.srv.URL, store.CooldownAPI)
	if err != nil {
		t.Fatalf("CooldownUntil: %v", err)
	}
	if until.IsZero() {
		t.Errorf("no (endpoint,api) cooldown recorded")
	}
	// The download upstream for the same endpoint has NO cooldown: the api
	// gate does not leak into the download path: api and download cooldowns
	// are scoped separately.
	ups, err := env.st.UpstreamState(ctx)
	if err != nil {
		t.Fatalf("UpstreamState: %v", err)
	}
	for _, u := range ups {
		if u.Endpoint == hub.srv.URL && u.CooldownUntil != nil && u.CooldownUntil.After(time.Now()) {
			t.Errorf("upstream cooldown set for %s — api 429 leaked into download gate", u.Endpoint)
		}
	}
	// Downloads ran.
	if got := hub.hitsFor("resolve"); got == 0 {
		t.Errorf("no resolve requests; downloads did not run")
	}
}

// TestMeta404Terminal: a 404 repo errors terminally without retries.
func TestMeta404Terminal(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{"x": []byte("y")})
	env := newTestEnv(t, hub)
	ctx := t.Context()

	// A different repo than the fixture serves → 404 everywhere.
	if err := env.manager.Submit(ctx, Job{Repo: "org/other"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	err := env.manager.Run(ctx)
	if err == nil {
		t.Fatalf("Run succeeded, want terminal 404 error")
	}
	if got := hub.hitsFor("revision"); got != 1 {
		t.Errorf("revision hits = %d, want 1 (no retry on 404)", got)
	}
	var status string
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT status FROM repos WHERE name = 'org/other'").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(store.RepoError) {
		t.Errorf("repo status = %s, want error", status)
	}
	if got := env.jobStatus(t, 1); got != string(store.JobError) {
		t.Errorf("job status = %s, want error", got)
	}
}
