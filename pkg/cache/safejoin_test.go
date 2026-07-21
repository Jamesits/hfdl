package cache

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSafeJoin(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Escape hatch: a symlink inside root pointing outside it.
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	// Legit symlink staying inside root.
	if err := os.Symlink("sub", filepath.Join(root, "inner")); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		p       string
		wantErr bool
	}{
		{"nested file", "a/b/c.txt", false},
		{"single file", "config.json", false},
		{"dot components", "a/./b/./c.txt", false},
		{"absolute", "/etc/passwd", true},
		{"empty", "", true},
		{"dotdot", "..", true},
		{"dotdot prefix", "../x", true},
		{"dotdot interior", "a/../../x", true},
		{"symlink escape", "link/evil.txt", true},
		{"symlink inside root", "inner/ok.txt", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SafeJoin(root, tc.p)
			if tc.wantErr {
				var perr *PathSafetyError
				if !errors.As(err, &perr) {
					t.Fatalf("SafeJoin(%q) err = %v, want *PathSafetyError", tc.p, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("SafeJoin(%q): %v", tc.p, err)
			}
			if want := filepath.Join(root, filepath.FromSlash(tc.p)); got != want {
				t.Errorf("SafeJoin(%q) = %q, want %q", tc.p, got, want)
			}
		})
	}
}

// The leaf itself may be an existing symlink pointing outside the root —
// only the ancestor chain is containment-checked, because huggingface_hub
// snapshot entries are symlinks into blobs/ by design.
func TestSafeJoinLeafSymlinkAllowed(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(outside, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "leaf")); err != nil {
		t.Fatal(err)
	}
	if _, err := SafeJoin(root, "leaf"); err != nil {
		t.Fatalf("SafeJoin(leaf symlink): %v", err)
	}
}
