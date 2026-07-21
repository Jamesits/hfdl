//go:build !unix && !windows

package sched

import (
	"context"
	"errors"
	"math"
	"strings"
)

// statfsFree on genuinely-unknown platforms (unix and Windows have real free-
// space queries) reports unlimited space: the ENOSPC pause still triggers on
// write errors, it just resumes on the next poll.
func statfsFree(ctx context.Context, dir string) (int64, error) {
	return math.MaxInt64, ctx.Err()
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
