//go:build linux

package store

import "testing"

func TestClassifyFsMagic(t *testing.T) {
	cases := []struct {
		magic int64
		kind  string
		netfs bool
	}{
		{nfsMagic, "nfs", true},
		{smbMagic, "smb", true},
		{cifsMagic, "cifs", true},
		{0xef53, "", false},     // ext2/3/4
		{0x01021994, "", false}, // tmpfs
		{0x58465342, "", false}, // xfs
		{0x9123683e, "", false}, // btrfs
		{0x2fc12fc1, "", false}, // zfs
		{0x7461636f, "", false}, // overlayfs
		{0x0000c0ff, "", false}, // unknown future fs: allowed, not refused
	}
	for _, c := range cases {
		kind, netfs := classifyFsMagic(c.magic)
		if kind != c.kind || netfs != c.netfs {
			t.Errorf("classifyFsMagic(%#x) = (%q, %v), want (%q, %v)",
				c.magic, kind, netfs, c.kind, c.netfs)
		}
	}
}

func TestProbeNetFSLocalDir(t *testing.T) {
	// A tempdir is on a local fs in CI/dev: probe must not refuse it.
	kind, netfs, err := probeNetFS(t.TempDir())
	if err != nil {
		t.Fatalf("probeNetFS: %v", err)
	}
	if netfs {
		t.Fatalf("probeNetFS(tempdir) = netfs %q, want local", kind)
	}
}
