//go:build darwin

package sched

import (
	"context"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// writeProbe proves writability of dir with a demand-sized reservation: a temp
// file preallocated to bytes via F_PREALLOCATE (a reservation quota rejects it
// even when statfs looks free) plus a one-page write and fsync, cleaned up. If
// F_PREALLOCATE is unsupported, ftruncate to the demand size still exercises a
// full-disk failure.
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
	st := &unix.Fstore_t{
		Flags:   unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  bytes,
	}
	if err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, st); err != nil {
		if terr := f.Truncate(bytes); terr != nil {
			return fmt.Errorf("sched: write probe reserve: %w", terr)
		}
	}
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		return fmt.Errorf("sched: write probe write: %w", err)
	}
	return f.Sync()
}
