//go:build linux || windows

package fcio

import "unsafe"

// checkAligned enforces the direct-tier alignment rule on offset, length and
// buffer address (FastCopy minSectorSize / ALIGN_SIZE analog). Only the
// alignment-constrained direct tiers gate on this (linux O_DIRECT, windows
// FILE_FLAG_NO_BUFFERING); the macOS F_NOCACHE tier and the plain fallback
// impose no alignment, hence the build constraint. UnalignedError itself lives
// in file.go because it is part of fcio's cross-platform public API (pkg/verify
// matches it via errors.As on every platform).
func checkAligned(path string, p []byte, off, align int64) error {
	if len(p) == 0 {
		return nil
	}
	if align <= 0 {
		return &UnalignedError{Path: path, Off: off, Len: len(p), Align: align, Cause: "alignment unknown"}
	}
	if off%align != 0 {
		return &UnalignedError{Path: path, Off: off, Len: len(p), Align: align, Cause: "offset"}
	}
	if int64(len(p))%align != 0 {
		return &UnalignedError{Path: path, Off: off, Len: len(p), Align: align, Cause: "length"}
	}
	if uintptr(unsafe.Pointer(&p[0]))%uintptr(align) != 0 {
		return &UnalignedError{Path: path, Off: off, Len: len(p), Align: align, Cause: "buffer address"}
	}
	return nil
}
