//go:build !linux && !windows

package fcio

import "os"

// openForSync opens path read-only: POSIX fsync accepts a read-only fd and
// there is no cache hint to issue afterwards on these platforms (only Windows
// FlushFileBuffers needs a write handle — see sync_windows.go).
func openForSync(path string) (*os.File, error) {
	return os.Open(path)
}

// dropFromCache is a no-op here: macOS has no posix_fadvise(DONTNEED) and
// neither do the remaining fallbacks, matching the File.DontNeed no-ops on
// those tiers.
func dropFromCache(*os.File) {}
