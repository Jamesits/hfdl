//go:build linux

package store

import (
	"golang.org/x/sys/unix"
)

// Network filesystem magic numbers (linux/statfs.h, magic.h).
const (
	nfsMagic  = 0x6969
	smbMagic  = 0x517b
	cifsMagic = 0xff534d42
)

// classifyFsMagic reports whether a statfs f_type is a network filesystem
// WAL must not run on. Split from the probe so it is unit-testable without a
// network mount.
func classifyFsMagic(magic int64) (kind string, netfs bool) {
	switch magic {
	case nfsMagic:
		return "nfs", true
	case smbMagic:
		return "smb", true
	case cifsMagic:
		return "cifs", true
	default:
		return "", false
	}
}

// probeNetFS statfs-probes dir (the DB's directory, not the file: the file
// may not exist yet).
func probeNetFS(dir string) (kind string, netfs bool, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return "", false, err
	}
	kind, netfs = classifyFsMagic(st.Type)
	return kind, netfs, nil
}
