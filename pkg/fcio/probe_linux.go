//go:build linux

package fcio

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// statfs f_type magic for network filesystems (linux/magic.h).
const (
	nfsMagic  = 0x6969
	cifsMagic = 0x517b
	smbMagic  = 0xff534d42
)

// ProbeFs classifies the volume behind path: NFS/CIFS/SMB statfs magic is
// FsNetFS; otherwise the block device's /sys queue/rotational decides HDD vs
// SSD; paths with no block queue behind them (tmpfs, overlay, ...) are
// FsUnknown — never an error.
func ProbeFs(ctx context.Context, path string) (FsType, error) {
	if err := ctx.Err(); err != nil {
		return FsUnknown, err
	}
	p := existingAncestor(path)
	var sfs unix.Statfs_t
	if err := unix.Statfs(p, &sfs); err != nil {
		return FsUnknown, fmt.Errorf("fcio: statfs %s: %w", p, err)
	}
	switch uint64(sfs.Type) {
	case nfsMagic, cifsMagic, smbMagic:
		return FsNetFS, nil
	}
	var st unix.Stat_t
	if err := unix.Stat(p, &st); err != nil {
		return FsUnknown, fmt.Errorf("fcio: stat %s: %w", p, err)
	}
	rotational, err := lookupRotational(st.Dev)
	if err != nil {
		return FsUnknown, nil
	}
	if rotational {
		return FsHDD, nil
	}
	return FsSSD, nil
}

// lookupRotational resolves st_dev through /sys/dev/block and ascends from a
// partition symlink to the whole-device directory holding queue/rotational.
func lookupRotational(dev uint64) (bool, error) {
	link := fmt.Sprintf("/sys/dev/block/%d:%d", unix.Major(dev), unix.Minor(dev))
	target, err := os.Readlink(link)
	if err != nil {
		return false, fmt.Errorf("readlink %s: %w", link, err)
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(link), target)
	}
	for p := filepath.Clean(target); strings.HasPrefix(p, "/sys"); p = filepath.Dir(p) {
		b, err := os.ReadFile(filepath.Join(p, "queue", "rotational"))
		if err == nil {
			return strings.TrimSpace(string(b)) == "1", nil
		}
	}
	return false, fmt.Errorf("no queue/rotational found under %s", target)
}

// statVolumeID identifies the volume behind path by st_dev ("major:minor").
func statVolumeID(path string) (VolumeID, error) {
	p := existingAncestor(path)
	var st unix.Stat_t
	if err := unix.Stat(p, &st); err != nil {
		return "", fmt.Errorf("fcio: stat volume %s: %w", p, err)
	}
	return VolumeID(fmt.Sprintf("%d:%d", unix.Major(st.Dev), unix.Minor(st.Dev))), nil
}
