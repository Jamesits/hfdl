//go:build windows

package sched

import (
	"os"

	"golang.org/x/sys/windows"
)

// devIno builds the (dev, ino) half of the reference invalidation key from the
// volume serial number and the 64-bit file index (GetFileInformationByHandle),
// the Windows analog of st_dev/st_ino. os.FileInfo.Sys() (a
// Win32FileAttributeData) carries neither, so the file is opened by path. On
// any failure it degrades to (0, 0); size+mtime still invalidate correctly.
func devIno(path string, _ os.FileInfo) (dev, ino uint64, identityErr error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, nil
	}
	h, err := windows.CreateFile(p, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return 0, 0, nil
	}
	defer windows.CloseHandle(h) //nolint:errcheck // read-only stat handle
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return 0, 0, nil
	}
	ino = uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)
	return uint64(info.VolumeSerialNumber), ino, nil
}
