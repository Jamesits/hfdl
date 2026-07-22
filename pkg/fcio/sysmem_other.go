//go:build !linux && !darwin && !windows

package fcio

import "errors"

// TotalRAM is unsupported on platforms without a physical-memory query; callers
// fall back to their fixed default cap.
func TotalRAM() (int64, error) {
	return 0, errors.New("fcio: TotalRAM unsupported on this platform")
}
