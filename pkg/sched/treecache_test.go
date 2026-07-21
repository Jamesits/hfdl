package sched

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestTreeCacheFullCommitListing: trees/<sha>.json carries the COMPLETE
// commit listing even when the job's include filter downloads only a
// subset — hf uses it as the commit tree cache. Both install modes.
func TestTreeCacheFullCommitListing(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{
		"keep/a.bin":  makeContent(64, 149),
		"drop/b.bin":  makeContent(96, 151),
		"drop/c.json": []byte(`{"x":1}`),
	}, "keep/a.bin", "drop/b.bin")
	env := newTestEnv(t, hub)
	ctx := t.Context()

	job := Job{
		Repo: "org/repo", Include: []string{"keep/*"},
		DestMode: "local-dir", DestDir: filepath.Join(env.dir, "out"),
	}
	if err := env.manager.Submit(ctx, job); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	readTree := func(path string) map[string]any {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read tree cache %s: %v", path, err)
		}
		var v struct {
			FormatVersion int            `json:"format_version"`
			Files         map[string]any `json:"files"`
		}
		if err := json.Unmarshal(b, &v); err != nil {
			t.Fatalf("parse tree cache: %v", err)
		}
		if v.FormatVersion != 1 {
			t.Errorf("format_version = %d, want 1", v.FormatVersion)
		}
		return v.Files
	}

	want := []string{"keep/a.bin", "drop/b.bin", "drop/c.json"}

	// local-dir mode.
	ldFiles := readTree(filepath.Join(env.dir, "out", ".cache", "huggingface", "trees", hub.sha+".json"))
	for _, p := range want {
		if _, ok := ldFiles[p]; !ok {
			t.Errorf("local-dir tree cache missing %q (has %v)", p, keys(ldFiles))
		}
	}

	// cache mode: second job, same repo, cache dest.
	job2 := Job{Repo: "org/repo", Include: []string{"keep/*"}}
	if err := env.manager.Submit(ctx, job2); err != nil {
		t.Fatalf("Submit 2: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	cmFiles := readTree(filepath.Join(env.cache.Root(), "models--org--repo", "trees", hub.sha+".json"))
	for _, p := range want {
		if _, ok := cmFiles[p]; !ok {
			t.Errorf("cache-mode tree cache missing %q (has %v)", p, keys(cmFiles))
		}
	}

	// Only the included file was downloaded (filter still applies to the
	// download machine).
	if got := hub.resolveHits("drop/b.bin"); got != 0 {
		t.Errorf("drop/b.bin downloaded %d times despite include filter", got)
	}
}

// TestTreeCacheExplicitFilenamesFullSet: an explicit 2-file job still
// gets the FULL commit tree cache in both modes (hf lists the full tree
// even for filename downloads).
func TestTreeCacheExplicitFilenamesFullSet(t *testing.T) {
	files := map[string][]byte{
		"f1.txt":      []byte("1"),
		"f2.txt":      []byte("2"),
		"other/a.bin": makeContent(48, 157),
		"other/b.bin": makeContent(48, 163),
		"other/c.bin": makeContent(48, 167),
	}
	hub := newFixtureHub(t, files)
	env := newTestEnv(t, hub)
	ctx := t.Context()

	job := Job{
		Repo: "org/repo", Filenames: []string{"f1.txt", "f2.txt"},
		DestMode: "local-dir", DestDir: filepath.Join(env.dir, "out"),
	}
	if err := env.manager.Submit(ctx, job); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	all := []string{"f1.txt", "f2.txt", "other/a.bin", "other/b.bin", "other/c.bin"}
	ld := readTreeFiles(t, filepath.Join(env.dir, "out", ".cache", "huggingface", "trees", hub.sha+".json"))
	for _, p := range all {
		if _, ok := ld[p]; !ok {
			t.Errorf("local-dir tree cache missing %q (has %v)", p, keys(ld))
		}
	}

	job2 := Job{Repo: "org/repo", Filenames: []string{"f1.txt", "f2.txt"}}
	if err := env.manager.Submit(ctx, job2); err != nil {
		t.Fatalf("Submit 2: %v", err)
	}
	if err := env.manager.Run(ctx); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	cm := readTreeFiles(t, filepath.Join(env.cache.Root(), "models--org--repo", "trees", hub.sha+".json"))
	for _, p := range all {
		if _, ok := cm[p]; !ok {
			t.Errorf("cache-mode tree cache missing %q (has %v)", p, keys(cm))
		}
	}

	// Only the two requested files downloaded.
	for p := range files {
		want := 0
		if p == "f1.txt" || p == "f2.txt" {
			want = 1
		}
		if got := hub.resolveHits(p); got != want {
			t.Errorf("resolve %s = %d, want %d", p, got, want)
		}
	}
}

func readTreeFiles(t *testing.T, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read tree cache %s: %v", path, err)
	}
	var v struct {
		Files map[string]any `json:"files"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("parse tree cache: %v", err)
	}
	return v.Files
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
