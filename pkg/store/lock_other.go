//go:build !linux && !darwin && !windows

package store

import (
	"fmt"
	"os"
)

// acquireLock is best-effort on platforms without a wired advisory lock:
// the lock file is created so operators can see which DB is active.
func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("store: open lock file: %w", err)
	}
	return f, nil
}

func releaseLock(f *os.File) error {
	if f == nil {
		return nil
	}
	return f.Close()
}
