//go:build !unix

package verify

import (
	"errors"

	"github.com/jamesits/hfdl/pkg/fcio"
)

// fallocateUnsupported reports the tier-B trigger off unix: Windows (and any
// other non-unix platform) has no fallocate, so fcio.File.Fallocate reports
// ErrFallocateUnsupported and the de-sparse zero-fill walk runs. This replaces
// the former always-false stub, which suppressed the walk and left blobs that
// were never actually de-sparsed.
func fallocateUnsupported(err error) bool {
	return errors.Is(err, fcio.ErrFallocateUnsupported)
}
