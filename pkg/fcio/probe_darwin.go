//go:build darwin

package fcio

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

// ProbeFs classifies the volume behind path: a non-local mount (MNT_LOCAL
// clear) or a known network f_fstypename is FsNetFS. macOS exposes no portable
// per-device rotational flag, so local media is reported FsUnknown, which
// VolumeSet treats conservatively (like HDD) — never an error.
func ProbeFs(ctx context.Context, path string) (FsType, error) {
	if err := ctx.Err(); err != nil {
		return FsUnknown, err
	}
	p := existingAncestor(path)
	var st unix.Statfs_t
	if err := unix.Statfs(p, &st); err != nil {
		return FsUnknown, fmt.Errorf("fcio: statfs %s: %w", p, err)
	}
	if st.Flags&unix.MNT_LOCAL == 0 {
		return FsNetFS, nil
	}
	switch strings.ToLower(fsTypeName(&st)) {
	case "nfs", "smbfs", "afpfs", "webdav", "cifs", "ftp":
		return FsNetFS, nil
	}
	return FsUnknown, nil
}

// fsTypeName reads the NUL-terminated f_fstypename.
func fsTypeName(st *unix.Statfs_t) string {
	b := st.Fstypename[:]
	n := 0
	for n < len(b) && b[n] != 0 {
		n++
	}
	return string(b[:n])
}

// statVolumeID identifies the volume behind path by its filesystem id (f_fsid),
// so two paths on one mount share an id and a mixed-R/W job never overlaps
// another job on the same volume.
func statVolumeID(path string) (VolumeID, error) {
	p := existingAncestor(path)
	var st unix.Statfs_t
	if err := unix.Statfs(p, &st); err != nil {
		return "", fmt.Errorf("fcio: statfs volume %s: %w", p, err)
	}
	return VolumeID(fmt.Sprintf("%d:%d", st.Fsid.Val[0], st.Fsid.Val[1])), nil
}
