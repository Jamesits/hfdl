//go:build !linux && !windows && !darwin

package sched

import (
	"context"
	"fmt"
	"os"
)

// writeProbe on genuinely-unknown platforms (Linux/Windows/macOS have real
// reservation probes): a demand-sized ftruncate — a full disk rejects the
// extend — plus a one-page write + fsync in a temp file, cleaned up.
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
	if err := f.Truncate(bytes); err != nil {
		return fmt.Errorf("sched: write probe set-size: %w", err)
	}
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		return fmt.Errorf("sched: write probe write: %w", err)
	}
	return f.Sync()
}
