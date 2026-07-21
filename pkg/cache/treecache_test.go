package cache

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Entries are the complete commit listing (hf parity: the trees cache
// feeds try_to_load_from_cache), and hf writes json.dump(indent=1) with no
// trailing newline — the expected bytes below are pinned against hf 1.24.0.
func TestWriteTreeCacheExactBytes(t *testing.T) {
	_, in := newInstallEnv(t)
	destDir := t.TempDir()
	entries := []TreeEntry{
		{Path: "a.txt", Size: 3, BlobID: "sha1a"},
		{Path: "b.bin", Size: 5, BlobID: "sha1b", LFSSHA256: "lfsb", LFSSize: 5},
		{Path: "c.bin", Size: 7, BlobID: "sha1c", LFSSHA256: "lfsc", LFSSize: 7, XetHash: "xetc"},
	}
	if err := in.WriteTreeCache(t.Context(), destDir, testSHA, entries); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(destDir, ".cache", "huggingface", "trees", testSHA+".json"))
	if err != nil {
		t.Fatal(err)
	}
	// encoding/json marshals map keys sorted, so the shape is deterministic.
	want := `{
 "format_version": 1,
 "files": {
  "a.txt": {
   "size": 3,
   "blob_id": "sha1a"
  },
  "b.bin": {
   "size": 5,
   "blob_id": "sha1b",
   "lfs_sha256": "lfsb",
   "lfs_size": 5
  },
  "c.bin": {
   "size": 7,
   "blob_id": "sha1c",
   "lfs_sha256": "lfsc",
   "lfs_size": 7,
   "xet_hash": "xetc"
  }
 }
}`
	if string(raw) != want {
		t.Errorf("tree cache =\n%s\nwant\n%s", raw, want)
	}
	if len(raw) > 0 && raw[len(raw)-1] == '\n' {
		t.Error("tree cache has a trailing newline")
	}
}

func TestWriteTreeCacheEmpty(t *testing.T) {
	_, in := newInstallEnv(t)
	destDir := t.TempDir()
	if err := in.WriteTreeCache(t.Context(), destDir, testSHA, nil); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(destDir, ".cache", "huggingface", "trees", testSHA+".json"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{
 "format_version": 1,
 "files": {}
}`
	if string(raw) != want {
		t.Errorf("tree cache =\n%s\nwant\n%s", raw, want)
	}
}

// Cache mode: the same JSON lands in the model dir's trees/ subdir.
func TestWriteModelTreeCache(t *testing.T) {
	_, in := newInstallEnv(t)
	cacheDir := t.TempDir()
	entries := []TreeEntry{{Path: "a.txt", Size: 3, BlobID: "sha1a"}}
	if err := in.WriteModelTreeCache(t.Context(), cacheDir, "model", "org/repo", testSHA, entries); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(cacheDir, "models--org--repo", "trees", testSHA+".json"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{
 "format_version": 1,
 "files": {
  "a.txt": {
   "size": 3,
   "blob_id": "sha1a"
  }
 }
}`
	if string(raw) != want {
		t.Errorf("model tree cache =\n%s\nwant\n%s", raw, want)
	}
	// Dataset repo type pluralizes to datasets--.
	if got := ModelDirName("dataset", "o/r"); got != "datasets--o--r" {
		t.Errorf("ModelDirName(dataset) = %q", got)
	}
	if got := ModelDirName("space", "o/r"); got != "spaces--o--r" {
		t.Errorf("ModelDirName(space) = %q", got)
	}
	if got := ModelDirName("", "o/r"); got != "models--o--r" {
		t.Errorf("ModelDirName(empty) = %q", got)
	}
}

// The fallback never clobbers an existing trees file; the authoritative
// overwrite variant always rewrites.
func TestWriteTreeCacheIfAbsent(t *testing.T) {
	_, in := newInstallEnv(t)
	destDir := t.TempDir()
	full := []TreeEntry{{Path: "a.txt", Size: 3, BlobID: "sha1a"}}
	fallback := []TreeEntry{{Path: "b.txt", Size: 9, BlobID: "sha1b"}}
	path := filepath.Join(destDir, ".cache", "huggingface", "trees", testSHA+".json")

	written, err := in.WriteTreeCacheIfAbsent(t.Context(), destDir, testSHA, full)
	if err != nil || !written {
		t.Fatalf("first write: written=%v err=%v", written, err)
	}
	written, err = in.WriteTreeCacheIfAbsent(t.Context(), destDir, testSHA, fallback)
	if err != nil {
		t.Fatal(err)
	}
	if written {
		t.Error("IfAbsent clobbered an existing trees file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "a.txt") || strings.Contains(string(raw), "b.txt") {
		t.Errorf("content after IfAbsent = %s, want the first write's", raw)
	}

	// The overwrite variant is the authoritative listing-time write.
	if err := in.WriteTreeCache(t.Context(), destDir, testSHA, fallback); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "b.txt") {
		t.Errorf("content after overwrite = %s", raw)
	}
}

func TestWriteModelTreeCacheIfAbsent(t *testing.T) {
	_, in := newInstallEnv(t)
	cacheDir := t.TempDir()
	entries := []TreeEntry{{Path: "a.txt", Size: 3, BlobID: "sha1a"}}
	written, err := in.WriteModelTreeCacheIfAbsent(t.Context(), cacheDir, "model", "org/repo", testSHA, entries)
	if err != nil || !written {
		t.Fatalf("written=%v err=%v", written, err)
	}
	written, err = in.WriteModelTreeCacheIfAbsent(t.Context(), cacheDir, "model", "org/repo", testSHA, entries)
	if err != nil {
		t.Fatal(err)
	}
	if written {
		t.Error("IfAbsent clobbered an existing model trees file")
	}
}

func TestWriteTreeCacheErrors(t *testing.T) {
	_, in := newInstallEnv(t)
	if err := in.WriteTreeCache(t.Context(), t.TempDir(), "", []TreeEntry{{Path: "a", Size: 1, BlobID: "x"}}); err == nil {
		t.Error("empty commit sha accepted")
	}
	if err := in.WriteTreeCache(t.Context(), t.TempDir(), testSHA, []TreeEntry{{Path: "", Size: 1, BlobID: "x"}}); err == nil {
		t.Error("empty entry path accepted")
	}
}
