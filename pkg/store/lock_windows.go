//go:build windows

package store

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// acquireLock takes an exclusive, fail-immediately LockFileEx byte-range
// lock on <db>.lock (Windows has no flock).
func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("store: open lock file: %w", err)
	}
	ol := new(windows.Overlapped)
	err = windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, ol,
	)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w (lock file %s)", ErrLocked, path)
	}
	return f, nil
}

func releaseLock(f *os.File) error {
	if f == nil {
		return nil
	}
	windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, new(windows.Overlapped)) //nolint:errcheck
	return f.Close()
}
