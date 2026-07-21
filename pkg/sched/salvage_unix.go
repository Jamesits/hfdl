//go:build unix

package sched

import (
	"os"
	"syscall"
)

// devIno extracts the (dev, ino) half of the reference invalidation key
// (size+mtime_ns+dev+ino). The path is unused on unix — os.FileInfo.Sys()
// already carries st_dev/st_ino.
func devIno(_ string, info os.FileInfo) (dev, ino uint64) {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Dev), st.Ino
	}
	return 0, 0
}
