//go:build !unix && !windows

package store

import (
	"fmt"
	"os"
)

// acquireLock refuses to open the store on platforms with no wired advisory
// lock (everything but unix flock and Windows LockFileEx). The single-process
// guarantee (a second hfdl must fail fast) cannot be honored here, and
// silently skipping the lock would let two processes corrupt one WAL DB, so the
// honest degradation is to fail rather than pretend.
func acquireLock(path string) (*os.File, error) {
	return nil, fmt.Errorf("store: single-process lock unsupported on this platform (lock file %s): %w", path, ErrLocked)
}

func releaseLock(f *os.File) error {
	if f == nil {
		return nil
	}
	return f.Close()
}
