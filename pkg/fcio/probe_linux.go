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

// blockDeviceDir resolves st_dev through /sys/dev/block and ascends from the
// partition symlink to the whole-device sysfs directory — the one that owns
// queue/rotational. Both media classification and volume identity key on the
// whole device, so every partition of one disk resolves to the same
// directory. Errors (device-mapper, loop, netfs, tmpfs — no block queue
// behind them) leave the caller to fall back.
func blockDeviceDir(dev uint64) (string, error) {
	link := fmt.Sprintf("/sys/dev/block/%d:%d", unix.Major(dev), unix.Minor(dev))
	target, err := os.Readlink(link)
	if err != nil {
		return "", fmt.Errorf("readlink %s: %w", link, err)
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(link), target)
	}
	for p := filepath.Clean(target); strings.HasPrefix(p, "/sys"); p = filepath.Dir(p) {
		if _, err := os.Stat(filepath.Join(p, "queue", "rotational")); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("no queue/rotational found under %s", target)
}

// lookupRotational reports whether the whole device behind st_dev is a
// spinning disk (queue/rotational == 1).
func lookupRotational(dev uint64) (bool, error) {
	dir, err := blockDeviceDir(dev)
	if err != nil {
		return false, err
	}
	b, err := os.ReadFile(filepath.Join(dir, "queue", "rotational"))
	if err != nil {
		return false, fmt.Errorf("read rotational under %s: %w", dir, err)
	}
	return strings.TrimSpace(string(b)) == "1", nil
}

// statVolumeID identifies the whole physical device (spindle) behind path so
// two partitions on one disk share a VolumeID and a mixed-R/W job never
// overlaps another job on the same spindle. It keys on the whole
// device's dev "major:minor" (from the sysfs dev file), falling back to the
// device directory name and finally to the partition's own st_dev when the
// sysfs walk cannot resolve a whole device.
func statVolumeID(path string) (VolumeID, error) {
	p := existingAncestor(path)
	var st unix.Stat_t
	if err := unix.Stat(p, &st); err != nil {
		return "", fmt.Errorf("fcio: stat volume %s: %w", p, err)
	}
	if dir, err := blockDeviceDir(st.Dev); err == nil {
		if b, rerr := os.ReadFile(filepath.Join(dir, "dev")); rerr == nil {
			if s := strings.TrimSpace(string(b)); s != "" {
				return VolumeID(s), nil
			}
		}
		return VolumeID(filepath.Base(dir)), nil
	}
	return VolumeID(fmt.Sprintf("%d:%d", unix.Major(st.Dev), unix.Minor(st.Dev))), nil
}
