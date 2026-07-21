//go:build unix

package sched

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// isOutOfSpace classifies write/copy failures that trigger the global
// ENOSPC pause: ENOSPC and EDQUOT.
func isOutOfSpace(err error) bool {
	return errors.Is(err, unix.ENOSPC) || errors.Is(err, unix.EDQUOT)
}

// statfsFree returns free bytes available to unprivileged users on the
// filesystem holding dir.
func statfsFree(ctx context.Context, dir string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, fmt.Errorf("sched: statfs %s: %w", dir, err)
	}
	return int64(st.Bavail) * int64(st.Bsize), nil //nolint:unconvert // Bsize width varies by OS
}
