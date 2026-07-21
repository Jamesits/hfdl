//go:build !linux

package sched

import (
	"context"
	"fmt"
	"os"
)

// writeProbe without fallocate (non-linux): a one-page write + fsync in a
// temp file, cleaned up.
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
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		return fmt.Errorf("sched: write probe write: %w", err)
	}
	return f.Sync()
}
