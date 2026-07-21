//go:build linux

package fcio

import (
	"os"

	"golang.org/x/sys/unix"
)

// openForSync opens path read-only: on Linux both fsync and fadvise(DONTNEED)
// accept a read-only fd, so there is no reason to request write access (only
// Windows FlushFileBuffers needs it — see sync_windows.go).
func openForSync(path string) (*os.File, error) {
	return os.Open(path)
}

// dropFromCache advises the kernel to evict the whole file (offset 0, length 0)
// from the page cache. Best-effort: a failed hint costs cache residency, never
// correctness, so the error is discarded. Reuses posixFadvDontNeed from
// file_linux.go. DONTNEED evicts only clean pages, so the caller must fsync
// first.
func dropFromCache(f *os.File) {
	_ = unix.Fadvise(int(f.Fd()), 0, 0, posixFadvDontNeed)
}
