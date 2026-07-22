//go:build linux

package fcio

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPhysicalDeviceIdentityTraversesStacks(t *testing.T) {
	root := t.TempDir()
	oldRoot := sysfsRoot
	sysfsRoot = root
	t.Cleanup(func() { sysfsRoot = oldRoot })

	block := filepath.Join(root, "block")
	makeDevice := func(name, dev string, slaves ...string) string {
		t.Helper()
		dir := filepath.Join(block, name)
		if err := os.MkdirAll(filepath.Join(dir, "queue"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "queue", "rotational"), []byte("0\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "dev"), []byte(dev+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(dir, "slaves"), 0o755); err != nil {
			t.Fatal(err)
		}
		for _, slave := range slaves {
			if err := os.Symlink(filepath.Join(block, slave), filepath.Join(dir, "slaves", slave)); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}
	sda := makeDevice("sda", "8:0")
	makeDevice("sdb", "8:16")
	dm := makeDevice("dm-0", "253:0", "sda")
	md := makeDevice("md0", "9:0", "sdb", "sda")

	// Model a partition symlink whose parent whole-device directory owns queue.
	partition := filepath.Join(sda, "sda1")
	if err := os.MkdirAll(partition, 0o755); err != nil {
		t.Fatal(err)
	}
	dev := unix.Mkdev(8, 1)
	link := filepath.Join(root, "dev", "block", fmt.Sprintf("%d:%d", unix.Major(dev), unix.Minor(dev)))
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(partition, link); err != nil {
		t.Fatal(err)
	}
	whole, err := blockDeviceDir(dev)
	if err != nil {
		t.Fatalf("resolve partition: %v", err)
	}
	partitionIDs, err := physicalDeviceIDs(whole)
	if err != nil {
		t.Fatal(err)
	}
	dmIDs, err := physicalDeviceIDs(dm)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(partitionIDs) != fmt.Sprint(dmIDs) || fmt.Sprint(dmIDs) != "[8:0]" {
		t.Fatalf("partition IDs %v and dm IDs %v must collide", partitionIDs, dmIDs)
	}
	mdIDs, err := physicalDeviceIDs(md)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(mdIDs); got != "[8:0 8:16]" {
		t.Fatalf("multi-slave IDs = %s, want sorted [8:0 8:16]", got)
	}
}
