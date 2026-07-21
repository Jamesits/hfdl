//go:build windows

package fcio

import (
	"os"

	"golang.org/x/sys/windows"
)

// openForSync opens path for a durability fsync (FlushFileBuffers). Unlike the
// plain os.OpenFile the other platforms use, it goes through CreateFile to set
// flags os.OpenFile cannot express:
//   - GENERIC_WRITE: FlushFileBuffers requires a write-access handle, else it
//     fails ERROR_ACCESS_DENIED (POSIX can fsync a read-only handle; Windows
//     cannot, so only this platform requests write access);
//   - FILE_FLAG_NO_BUFFERING: keep this handle's touch of the just-written blob
//     out of the system cache — the Windows stand-in for Linux fadvise(DONTNEED),
//     which has no post-open equivalent here, so it must be set at open time;
//   - FILE_FLAG_OPEN_NO_RECALL: do not recall the data from tiered/offline (HSM)
//     storage on open — we only flush it, never read it back.
//
// OPEN_EXISTING (not OPEN_ALWAYS): the blob must already exist; a missing file
// is an error, matching os.OpenFile without O_CREATE.
func openForSync(path string) (*os.File, error) {
	namep, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	access := uint32(windows.GENERIC_READ | windows.GENERIC_WRITE)
	share := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE)
	attrs := uint32(windows.FILE_ATTRIBUTE_NORMAL | windows.FILE_FLAG_NO_BUFFERING | windows.FILE_FLAG_OPEN_NO_RECALL)
	h, err := windows.CreateFile(namep, access, share, nil, windows.OPEN_EXISTING, attrs, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(h), path), nil
}

// dropFromCache is a no-op on Windows: FILE_FLAG_NO_BUFFERING at open already
// kept this handle out of the system cache, and there is no
// posix_fadvise(DONTNEED) equivalent to evict pages after the fact.
func dropFromCache(*os.File) {}
