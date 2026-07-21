//go:build linux

package cache

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func ioctlFiclone(dstFd, srcFd uintptr) error {
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, dstFd, unix.FICLONE, srcFd); errno != 0 {
		return errno
	}
	return nil
}

// reflinkFile clones src's extents into a fresh dst inode (FICLONE ioctl)
// for local-dir installs. O_EXCL keeps a stale temp from being
// clobbered; EOPNOTSUPP/EXDEV surface so the caller falls back to a copy.
func reflinkFile(src, dst string) error {
	s, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("cache: reflink open %s: %w", src, err)
	}
	defer s.Close()
	d, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("cache: reflink create %s: %w", dst, err)
	}
	// x/sys/unix v0.47 ships the FICLONE request constant but no wrapper,
	// so issue the ioctl directly: FICLONE(dstFd, srcFd) clones src's
	// extents into dst.
	if err := ioctlFiclone(d.Fd(), s.Fd()); err != nil {
		_ = d.Close()      // error path: the ioctl error is the one that matters
		_ = os.Remove(dst) // best-effort cleanup of the just-created empty dst
		return fmt.Errorf("cache: reflink %s → %s: %w", src, dst, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("cache: reflink close %s: %w", dst, err)
	}
	return nil
}
