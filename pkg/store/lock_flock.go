//go:build linux || darwin

package store

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// acquireLock takes an exclusive, non-blocking flock on <db>.lock. The lock
// file is never truncated or deleted: deleting it would let a second process
// lock a fresh inode while the first still holds the old one.
func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("store: open lock file: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w (lock file %s)", ErrLocked, path)
	}
	return f, nil
}

func releaseLock(f *os.File) error {
	if f == nil {
		return nil
	}
	unix.Flock(int(f.Fd()), unix.LOCK_UN) //nolint:errcheck // close releases regardless
	return f.Close()
}
