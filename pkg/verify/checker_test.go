package verify

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/otel"
	"github.com/jamesits/hfdl/pkg/throttle"
)

func newTestChecker(t *testing.T) (*Checker, *fcio.Engine) {
	t.Helper()
	e := fcio.NewEngine(slog.New(slog.DiscardHandler), nil, fcio.TierAuto)
	p := fcio.NewPool(64<<10, 1<<20)
	d := throttle.NewDutyLimiter(100, throttle.MediaSSD)
	return NewChecker(e, p, d, slog.New(slog.DiscardHandler), otel.Noop()), e
}

func writeTestFile(t *testing.T, e *fcio.Engine, content []byte) *fcio.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blob")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := e.Open(t.Context(), path, -1, fcio.Hints{Sequential: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("close %s: %v", path, err)
		}
	})
	return f
}

// Git blob sha1 vectors verified against `git hash-object --stdin`:
// "hello" → b6fc4c62…, "hello\n" → ce013625…, "" → e69de29b….
func TestHashGitBlobSHA1(t *testing.T) {
	c, e := newTestChecker(t)
	for _, tc := range []struct {
		content string
		want    string
	}{
		{"hello", "b6fc4c620b67d95f953a5c1c1230aaab5db5a1b0"},
		{"hello\n", "ce013625030ba8dba906f756967f9e9ca394464a"},
		{"", "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"},
	} {
		f := writeTestFile(t, e, []byte(tc.content))
		got, err := c.Hash(t.Context(), f, int64(len(tc.content)), false)
		if err != nil {
			t.Fatalf("Hash(%q): %v", tc.content, err)
		}
		if got != tc.want {
			t.Errorf("Hash(%q) = %s, want %s", tc.content, got, tc.want)
		}
	}
}

func TestHashSHA256(t *testing.T) {
	c, e := newTestChecker(t)
	f := writeTestFile(t, e, []byte("hello"))
	got, err := c.Hash(t.Context(), f, 5, true)
	if err != nil {
		t.Fatal(err)
	}
	const want = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if got != want {
		t.Errorf("Hash = %s, want %s", got, want)
	}
}

func TestVerifyOK(t *testing.T) {
	c, e := newTestChecker(t)
	f := writeTestFile(t, e, []byte("hello"))
	if err := c.Verify(t.Context(), f, 5, false, "b6fc4c620b67d95f953a5c1c1230aaab5db5a1b0"); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyMismatch(t *testing.T) {
	c, e := newTestChecker(t)
	content := []byte("hello")
	f := writeTestFile(t, e, content)
	const want = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	err := c.Verify(t.Context(), f, int64(len(content)), true, want)
	var merr *MismatchError
	if !errors.As(err, &merr) {
		t.Fatalf("Verify error = %v, want *MismatchError", err)
	}
	if merr.Path != f.Path() {
		t.Errorf("MismatchError.Path = %q, want %q", merr.Path, f.Path())
	}
	if merr.Want != want {
		t.Errorf("MismatchError.Want = %q, want %q", merr.Want, want)
	}
	const gotSHA = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if merr.Got != gotSHA {
		t.Errorf("MismatchError.Got = %q, want %q", merr.Got, gotSHA)
	}
}

// A larger-than-one-slab file exercises the ReadAll chunk loop and the
// per-chunk duty checkpoints.
func TestHashLargeFile(t *testing.T) {
	c, e := newTestChecker(t)
	content := make([]byte, 3<<20)
	for i := range content {
		content[i] = byte(i)
	}
	f := writeTestFile(t, e, content)
	got, err := c.Hash(t.Context(), f, int64(len(content)), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Verify(t.Context(), f, int64(len(content)), true, got); err != nil {
		t.Fatal(err)
	}
}

func TestHashContextCancel(t *testing.T) {
	c, e := newTestChecker(t)
	f := writeTestFile(t, e, make([]byte, 1<<20))
	dead, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Hash(dead, f, 1<<20, true); err == nil {
		t.Fatal("Hash on cancelled context: want error")
	}
}
