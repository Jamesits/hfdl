//go:build darwin

package cache

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// reflinkFile clones src into a fresh dst inode via clonefile(2) (APFS
// copy-on-write) for local-dir installs. clonefile refuses to overwrite an
// existing dst (like O_EXCL), so a stale temp is never clobbered; any failure —
// non-CoW volume (ENOTSUP), cross-volume (EXDEV), or an existing dst (EEXIST) —
// surfaces so the caller falls back to a plain copy.
func reflinkFile(src, dst string) error {
	if err := unix.Clonefile(src, dst, 0); err != nil {
		return fmt.Errorf("cache: reflink %s → %s: %w", src, dst, err)
	}
	return nil
}
