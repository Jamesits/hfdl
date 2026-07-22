//go:build !unix && !windows

package sched

import (
	"context"
	"errors"
	"strings"
)

// statfsFree on genuinely-unknown platforms (unix and Windows have real free-
// space queries) fails explicitly so callers can use a writability probe
// instead of treating the filesystem as having unlimited space.
func statfsFree(ctx context.Context, dir string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return 0, &FreeSpaceUnsupportedError{}
}

// isOutOfSpace without unix errnos: ENOSPC/EDQUOT surface through the
// wrapped message on platforms where x/sys/unix has no constants.
func isOutOfSpace(err error) bool {
	return err != nil && (errors.Is(err, errENOSPC) ||
		containsFold(err.Error(), "no space left") || containsFold(err.Error(), "quota exceeded"))
}

var errENOSPC = errors.New("no space left on device")

func containsFold(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if strings.EqualFold(s[i:i+len(sub)], sub) {
			return true
		}
	}
	return false
}
