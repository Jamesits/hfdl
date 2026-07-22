//go:build !unix && !windows

package sched

import (
	"errors"
	"os"
)

var errFileIdentityUnsupported = errors.New("sched: filesystem identity is unsupported on this platform")

// devIno on genuinely-unknown platforms (unix and Windows expose real dev/ino):
// size+mtime still invalidate correctly where dev/ino cannot be obtained.
func devIno(_ string, info os.FileInfo) (dev, ino uint64, err error) {
	return 0, 0, errFileIdentityUnsupported
}
