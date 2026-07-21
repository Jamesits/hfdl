package cache

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jamesits/hfdl/pkg/fcio"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// Blob ids must be bare hex digests (validateBlobID); these 40-char (git blob
// sha1-shaped) hex constants stand in for real ids in the store/publish tests.
const (
	hexIDa = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hexIDb = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	hexIDc = "cccccccccccccccccccccccccccccccccccccccc"
)

func newTestStore(t *testing.T) (*Store, *fcio.Engine) {
	t.Helper()
	e := fcio.NewEngine(discardLogger(), nil, fcio.TierAuto)
	s, err := OpenStore(t.Context(), filepath.Join(t.TempDir(), "cache"), e, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	return s, e
}

func TestOpenStoreLayout(t *testing.T) {
	s, _ := newTestStore(t)
	for _, dir := range []string{
		filepath.Join(s.Root(), "blobs"),
		filepath.Join(s.Root(), ".hfdl", "incomplete"),
	} {
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			t.Errorf("%s: err=%v isDir=%v", dir, err, info != nil && info.IsDir())
		}
	}
	if !filepath.IsAbs(s.Root()) {
		t.Errorf("Root() = %q, want absolute", s.Root())
	}
	// hf marks the cache root with a verbatim cache directory tag (literal
	// bytes pinned against hf 1.24.0, independent of the package const).
	tag, err := os.ReadFile(filepath.Join(s.Root(), "CACHEDIR.TAG"))
	if err != nil {
		t.Fatal(err)
	}
	wantTag := "Signature: 8a477f597d28d172789f06886806bc55\n" +
		"# This file is a cache directory tag created by huggingface_hub.\n" +
		"# For information about cache directory tags, see:\n" +
		"#\thttps://bford.info/cachedir/\n"
	if string(tag) != wantTag {
		t.Errorf("CACHEDIR.TAG = %q, want %q", tag, wantTag)
	}
}

// Like hf, OpenStore never rewrites an existing CACHEDIR.TAG.
func TestOpenStoreCachedirTagPreserved(t *testing.T) {
	s, e := newTestStore(t)
	tagPath := filepath.Join(s.Root(), "CACHEDIR.TAG")
	if err := os.WriteFile(tagPath, []byte("custom"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(t.Context(), s.Root(), e, discardLogger()); err != nil {
		t.Fatal(err)
	}
	tag, err := os.ReadFile(tagPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(tag) != "custom" {
		t.Errorf("CACHEDIR.TAG rewritten to %q", tag)
	}
}

func TestPaths(t *testing.T) {
	s, _ := newTestStore(t)
	if got, want := s.IncompletePath(42), filepath.Join(s.Root(), ".hfdl", "incomplete", "42.part"); got != want {
		t.Errorf("IncompletePath = %q, want %q", got, want)
	}
	if got, want := s.BlobPath(hexIDa), filepath.Join(s.Root(), "blobs", hexIDa); got != want {
		t.Errorf("BlobPath = %q, want %q", got, want)
	}
	// A blob id that is not a bare hex digest resolves to no path (never
	// escapes blobs/).
	if got := s.BlobPath("../../etc/passwd"); got != "" {
		t.Errorf("BlobPath(unsafe) = %q, want empty", got)
	}
}

func TestHasBlob(t *testing.T) {
	s, _ := newTestStore(t)
	if p, ok := s.HasBlob(hexIDa); ok || p != "" {
		t.Errorf("HasBlob(absent) = %q, %v", p, ok)
	}
	blob := s.BlobPath(hexIDb)
	if err := os.WriteFile(blob, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if p, ok := s.HasBlob(hexIDb); !ok || p != blob {
		t.Errorf("HasBlob(present) = %q, %v", p, ok)
	}
	// An unsafe blob id is never reported as published.
	if p, ok := s.HasBlob("../../etc/passwd"); ok || p != "" {
		t.Errorf("HasBlob(unsafe) = %q, %v", p, ok)
	}
}
