//go:build windows

package sched

import (
	"context"
	"fmt"
	"os"
)

// writeProbe proves writability of dir with a demand-sized reservation: a temp
// file grown to bytes via SetEndOfFile (os.File.Truncate) — a full-disk or
// quota-limited volume rejects the extend even when a one-page write would
// still fit — plus a one-page write and fsync, cleaned up.
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
