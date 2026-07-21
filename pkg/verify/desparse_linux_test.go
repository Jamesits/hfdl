//go:build linux

package verify

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/jamesits/hfdl/pkg/fcio"
	"golang.org/x/sys/unix"
)

// makeSparseFile writes two small data islands into a size-byte file,
// leaving a hole between them.
func makeSparseFile(t *testing.T, size int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blob")
	raw, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.WriteAt(bytes.Repeat([]byte("a"), 1000), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.WriteAt(bytes.Repeat([]byte("b"), 1000), size-1000); err != nil {
		t.Fatal(err)
	}
	if err := raw.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func extentCoverage(extents [][2]int64, size int64) (covered int64) {
	for _, ex := range extents {
		end := min(ex[1], size)
		if end > ex[0] {
			covered += end - ex[0]
		}
	}
	return covered
}

func assertDense(t *testing.T, path string, size int64) {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatal(err)
	}
	if st.Blocks*512 < size {
		t.Errorf("st_blocks = %d (%d bytes), file not dense for size %d", st.Blocks, st.Blocks*512, size)
	}
}

func TestDeSparseFallocate(t *testing.T) {
	c, e := newTestChecker(t)
	const size = int64(1 << 20)
	path := makeSparseFile(t, size)
	f, err := e.Open(t.Context(), path, -1, fcio.Hints{})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	method, holeBytes, err := c.DeSparse(t.Context(), f, size)
	if err != nil {
		t.Fatal(err)
	}
	// tmpfs/ext4 both take the fallocate path; if this filesystem does not,
	// the walk result is still valid — density is the invariant.
	if method != methodFallocate && method != methodWalk {
		t.Errorf("method = %q", method)
	}
	if method == methodFallocate && holeBytes != 0 {
		t.Errorf("fallocate holeBytes = %d, want 0", holeBytes)
	}
	assertDense(t, path, size)
}

func TestDeSparseWalk(t *testing.T) {
	c, e := newTestChecker(t)
	const size = int64(1 << 20)
	path := makeSparseFile(t, size)
	f, err := e.Open(t.Context(), path, -1, fcio.Hints{})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	before, err := f.DataExtents()
	if err != nil {
		t.Skipf("SEEK_DATA/SEEK_HOLE unsupported here: %v", err)
	}
	wantHoleBytes := size - extentCoverage(before, size)
	if wantHoleBytes <= 0 {
		t.Skipf("filesystem did not keep the hole (extents %v)", before)
	}

	// Force the tier-B trigger so the walk must run.
	c.fallocateFn = func(f *fcio.File, size int64) error { return unix.EOPNOTSUPP }
	method, holeBytes, err := c.DeSparse(t.Context(), f, size)
	if err != nil {
		t.Fatal(err)
	}
	if method != methodWalk {
		t.Fatalf("method = %q, want %q", method, methodWalk)
	}
	if holeBytes != wantHoleBytes {
		t.Errorf("holeBytes = %d, want %d (extents %v)", holeBytes, wantHoleBytes, before)
	}

	after, err := f.DataExtents()
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 || after[0][0] != 0 || after[0][1] != size {
		t.Errorf("extents after walk = %v, want one extent [0,%d)", after, size)
	}
	assertDense(t, path, size)
}

func TestDeSparseFallocateHardError(t *testing.T) {
	c, e := newTestChecker(t)
	const size = int64(4096)
	path := makeSparseFile(t, size)
	f, err := e.Open(t.Context(), path, -1, fcio.Hints{})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// ENOSPC (or any non-trigger errno) must surface, not start the walk.
	c.fallocateFn = func(f *fcio.File, size int64) error { return unix.ENOSPC }
	if _, _, err := c.DeSparse(t.Context(), f, size); err == nil {
		t.Fatal("want error for non-trigger fallocate failure")
	}
}
