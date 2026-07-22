//go:build darwin

package store

import (
	"golang.org/x/sys/unix"
)

// probeNetFS classifies dir's volume: SQLite WAL is unsafe on network mounts,
// which macOS reports via a clear MNT_LOCAL flag (and, secondarily, a network
// f_fstypename such as nfs/smbfs/afpfs/webdav).
func probeNetFS(dir string) (kind string, netfs bool, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return "", false, err
	}
	name := fstypename(&st)
	switch name {
	case "nfs", "smbfs", "afpfs", "webdav", "cifs", "ftp":
		return name, true, nil
	}
	if st.Flags&unix.MNT_LOCAL == 0 {
		if name == "" {
			name = "remote"
		}
		return name, true, nil
	}
	return "", false, nil
}

// fstypename reads the NUL-terminated f_fstypename.
func fstypename(st *unix.Statfs_t) string {
	b := st.Fstypename[:]
	n := 0
	for n < len(b) && b[n] != 0 {
		n++
	}
	return string(b[:n])
}
