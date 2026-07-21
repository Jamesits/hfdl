//go:build windows

package store

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// probeNetFS classifies dir's volume: SQLite WAL is unsafe on network shares,
// which Windows reports via GetDriveType == DRIVE_REMOTE (mapped drives and UNC
// roots). The FS name is a secondary signal for shares GetDriveType cannot
// resolve.
func probeNetFS(dir string) (kind string, netfs bool, err error) {
	root, err := volumeRoot(dir)
	if err != nil {
		return "", false, nil // cannot resolve: do not block startup
	}
	rootp, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return "", false, nil
	}
	if windows.GetDriveType(rootp) == windows.DRIVE_REMOTE {
		return "remote", true, nil
	}
	if name, ok := volumeFsName(root); ok {
		switch upperASCII(name) {
		case "NFS", "SMB", "CIFS", "WEBDAV":
			return lowerASCII(name), true, nil
		}
	}
	return "", false, nil
}

// volumeRoot resolves dir to its volume mount root (e.g. "C:\" or
// "\\server\share\").
func volumeRoot(dir string) (string, error) {
	p, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return "", fmt.Errorf("store: volume root %s: %w", dir, err)
	}
	buf := make([]uint16, 261)
	if err := windows.GetVolumePathName(p, &buf[0], uint32(len(buf))); err != nil {
		return "", fmt.Errorf("store: GetVolumePathName %s: %w", dir, err)
	}
	return windows.UTF16ToString(buf), nil
}

func volumeFsName(root string) (string, bool) {
	rootp, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return "", false
	}
	var serial, maxComp, flags uint32
	name := make([]uint16, 256)
	if err := windows.GetVolumeInformation(rootp, nil, 0, &serial, &maxComp, &flags, &name[0], uint32(len(name))); err != nil {
		return "", false
	}
	return windows.UTF16ToString(name), true
}

func upperASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}

func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}
