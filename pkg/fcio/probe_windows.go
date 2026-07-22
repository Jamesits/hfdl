//go:build windows

package fcio

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// IOCTL_STORAGE_QUERY_PROPERTY / StorageDeviceSeekPenaltyProperty are not
// exported by x/sys/windows, so the control code and query/descriptor structs
// are declared here.
const (
	ioctlStorageQueryProperty        = 0x002D1400
	storageDeviceSeekPenaltyProperty = 7 // STORAGE_PROPERTY_ID
	storageAccessAlignmentProperty   = 6 // STORAGE_PROPERTY_ID
	propertyStandardQuery            = 0 // STORAGE_QUERY_TYPE
	winMaxPath                       = 260
)

type storagePropertyQuery struct {
	PropertyID           uint32
	QueryType            uint32
	AdditionalParameters [1]byte
}

type deviceSeekPenaltyDescriptor struct {
	Version           uint32
	Size              uint32
	IncursSeekPenalty uint8 // BOOLEAN
	_                 [3]uint8
}

type storageAccessAlignmentDescriptor struct {
	Version                       uint32
	Size                          uint32
	BytesPerCacheLine             uint32
	BytesOffsetForCacheAlignment  uint32
	BytesPerLogicalSector         uint32
	BytesPerPhysicalSector        uint32
	BytesOffsetForSectorAlignment uint32
}

// volumeDirectAlignment returns the physical-sector constraint reported by
// the storage stack. The 4KiB floor preserves pool-slab alignment on devices
// whose logical or physical sectors are smaller.
func volumeDirectAlignment(path string) int64 {
	root, err := volumeRoot(path)
	if err != nil || len(root) < 2 || root[1] != ':' {
		return slabAlign
	}
	devp, err := windows.UTF16PtrFromString(`\\.\` + root[:2])
	if err != nil {
		return slabAlign
	}
	h, err := windows.CreateFile(devp, 0, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.OPEN_EXISTING, 0, 0)
	if err != nil {
		return slabAlign
	}
	defer windows.CloseHandle(h) //nolint:errcheck // query handle
	q := storagePropertyQuery{PropertyID: storageAccessAlignmentProperty, QueryType: propertyStandardQuery}
	var desc storageAccessAlignmentDescriptor
	var n uint32
	if err := windows.DeviceIoControl(h, ioctlStorageQueryProperty,
		(*byte)(unsafe.Pointer(&q)), uint32(unsafe.Sizeof(q)),
		(*byte)(unsafe.Pointer(&desc)), uint32(unsafe.Sizeof(desc)), &n, nil); err != nil || n == 0 {
		return slabAlign
	}
	align := int64(desc.BytesPerPhysicalSector)
	if logical := int64(desc.BytesPerLogicalSector); logical > align {
		align = logical
	}
	if align < slabAlign {
		align = slabAlign
	}
	return align
}

// ProbeFs classifies the volume behind path: network drives (GetDriveType ==
// DRIVE_REMOTE, or a network filesystem name) are FsNetFS; otherwise the
// medium's seek penalty (IOCTL_STORAGE_QUERY_PROPERTY) decides HDD vs SSD when
// the volume device can be opened, else FsUnknown — never an error.
func ProbeFs(ctx context.Context, path string) (FsType, error) {
	if err := ctx.Err(); err != nil {
		return FsUnknown, err
	}
	root, err := volumeRoot(path)
	if err != nil {
		return FsUnknown, nil
	}
	rootp, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return FsUnknown, nil
	}
	if windows.GetDriveType(rootp) == windows.DRIVE_REMOTE {
		return FsNetFS, nil
	}
	if fsName, ok := volumeFsName(root); ok {
		switch strings.ToUpper(fsName) {
		case "NFS", "SMB", "CIFS", "WEBDAV":
			return FsNetFS, nil
		}
	}
	if rotational, ok := seekPenalty(root); ok {
		if rotational {
			return FsHDD, nil
		}
		return FsSSD, nil
	}
	return FsUnknown, nil
}

// volumeRoot resolves path (which need not exist yet) to its volume mount root
// (e.g. "C:\" or "\\server\share\").
func volumeRoot(path string) (string, error) {
	abs, err := filepath.Abs(existingAncestor(path))
	if err != nil {
		return "", err
	}
	p, err := windows.UTF16PtrFromString(abs)
	if err != nil {
		return "", err
	}
	buf := make([]uint16, winMaxPath+1)
	if err := windows.GetVolumePathName(p, &buf[0], uint32(len(buf))); err != nil {
		return "", err
	}
	return windows.UTF16ToString(buf), nil
}

// volumeFsName returns the filesystem name (NTFS/FAT/exFAT/ReFS/...) of the
// volume mounted at root.
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

// seekPenalty reports (rotational, ok) for a drive-letter root ("X:\") via
// IOCTL_STORAGE_QUERY_PROPERTY on the volume device. UNC and mounted-folder
// roots return ok=false (medium unknown → conservative). Zero access rights
// are requested, so no administrator privilege is needed.
func seekPenalty(root string) (rotational, ok bool) {
	if len(root) < 2 || root[1] != ':' {
		return false, false
	}
	devp, err := windows.UTF16PtrFromString(`\\.\` + root[:2]) // \\.\C:
	if err != nil {
		return false, false
	}
	h, err := windows.CreateFile(devp, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil,
		windows.OPEN_EXISTING, 0, 0)
	if err != nil {
		return false, false
	}
	defer windows.CloseHandle(h) //nolint:errcheck // query handle
	q := storagePropertyQuery{PropertyID: storageDeviceSeekPenaltyProperty, QueryType: propertyStandardQuery}
	var desc deviceSeekPenaltyDescriptor
	var bytesReturned uint32
	if err := windows.DeviceIoControl(h, ioctlStorageQueryProperty,
		(*byte)(unsafe.Pointer(&q)), uint32(unsafe.Sizeof(q)),
		(*byte)(unsafe.Pointer(&desc)), uint32(unsafe.Sizeof(desc)),
		&bytesReturned, nil); err != nil {
		return false, false
	}
	if bytesReturned == 0 {
		return false, false
	}
	return desc.IncursSeekPenalty != 0, true
}

// statVolumeID identifies the volume behind path by its serial number so two
// paths on one volume share an id and a mixed-R/W job never overlaps another
// job on the same volume. Falls back to the uppercased mount root.
func statVolumeID(path string) (VolumeID, error) {
	root, err := volumeRoot(path)
	if err != nil {
		return "", fmt.Errorf("fcio: resolve volume %s: %w", path, err)
	}
	rootp, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return "", fmt.Errorf("fcio: resolve volume %s: %w", path, err)
	}
	var serial, maxComp, flags uint32
	if err := windows.GetVolumeInformation(rootp, nil, 0, &serial, &maxComp, &flags, nil, 0); err != nil {
		return VolumeID(strings.ToUpper(root)), nil
	}
	return VolumeID(fmt.Sprintf("%08X", serial)), nil
}
