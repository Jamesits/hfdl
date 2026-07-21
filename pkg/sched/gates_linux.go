//go:build linux

package sched

import (
	"context"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// writeProbe proves writability of dir: a small fallocate (the operation
// whose failure triggered the pause — reservation quotas reject it even
// when statfs looks free) plus a one-page write and fsync, cleaned up.
func writeProbe(ctx context.Context, dir string, bytes int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".hfdl-probe-*")
	if err != nil {
		return fmt.Errorf("sched: write probe: %w", err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	defer func() { _ = f.Close() }()
	if err := unix.Fallocate(int(f.Fd()), 0, 0, bytes); err != nil {
		return fmt.Errorf("sched: write probe fallocate: %w", err)
	}
	page := make([]byte, 4096)
	if _, err := f.Write(page); err != nil {
		return fmt.Errorf("sched: write probe write: %w", err)
	}
	return f.Sync()
}
