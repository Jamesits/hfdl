//go:build !unix && !windows

package sched

import "os"

// devIno on genuinely-unknown platforms (unix and Windows expose real dev/ino):
// size+mtime still invalidate correctly where dev/ino cannot be obtained.
func devIno(_ string, info os.FileInfo) (dev, ino uint64) { return 0, 0 }
