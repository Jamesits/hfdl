//go:build windows

package sched

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

// isOutOfSpace classifies write/copy failures that trigger the global ENOSPC
// pause: Windows surfaces disk-full as ERROR_DISK_FULL / ERROR_HANDLE_DISK_FULL.
func isOutOfSpace(err error) bool {
	return errors.Is(err, windows.ERROR_DISK_FULL) ||
		errors.Is(err, windows.ERROR_HANDLE_DISK_FULL)
}

// statfsFree returns free bytes available to the caller on the volume holding
// dir (GetDiskFreeSpaceEx honors per-user quota via FreeBytesAvailableToCaller).
func statfsFree(ctx context.Context, dir string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	p, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return 0, fmt.Errorf("sched: statfs %s: %w", dir, err)
	}
	var freeAvail, total, totalFree uint64
	if err := windows.GetDiskFreeSpaceEx(p, &freeAvail, &total, &totalFree); err != nil {
		return 0, fmt.Errorf("sched: GetDiskFreeSpaceEx %s: %w", dir, err)
	}
	if freeAvail > 1<<62 {
		return 1 << 62, nil // guard the int64 conversion on absurd values
	}
	return int64(freeAvail), nil
}
