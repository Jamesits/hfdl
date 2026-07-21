//go:build unix

package verify

import (
	"errors"

	"golang.org/x/sys/unix"
)

// fallocateUnsupported reports the tier-B trigger errnos:
// NFS<v4.2, some SMB and exFAT reject fallocate with one of these.
func fallocateUnsupported(err error) bool {
	return errors.Is(err, unix.ENOSYS) ||
		errors.Is(err, unix.EOPNOTSUPP) ||
		errors.Is(err, unix.EINVAL)
}
