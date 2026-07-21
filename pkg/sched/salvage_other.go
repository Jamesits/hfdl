//go:build !unix

package sched

import "os"

// devIno without unix stat: size+mtime still invalidate correctly on
// platforms that cannot expose dev/ino portably.
func devIno(info os.FileInfo) (dev, ino uint64) { return 0, 0 }
