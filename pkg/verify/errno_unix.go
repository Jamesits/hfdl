//go:build unix

package verify

import (
	"errors"

	"github.com/jamesits/hfdl/pkg/fcio"
	"golang.org/x/sys/unix"
)

// fallocateUnsupported reports the tier-B trigger: on Linux the fallocate
// syscall returns ENOSYS/EOPNOTSUPP/EINVAL (NFS<v4.2, some SMB, exFAT); macOS
// has no fallocate at all and fcio reports ErrFallocateUnsupported. Either way
// the de-sparse walk runs.
func fallocateUnsupported(err error) bool {
	return errors.Is(err, unix.ENOSYS) ||
		errors.Is(err, unix.EOPNOTSUPP) ||
		errors.Is(err, unix.EINVAL) ||
		errors.Is(err, fcio.ErrFallocateUnsupported)
}
