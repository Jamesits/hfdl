//go:build !windows

package cache

import (
	"fmt"
	"os"
)

// fsyncDir fsyncs a directory so a link/rename inside it survives a crash:
// namespace durability requires fsyncing the parent directory after the
// entry is created.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("cache: open dir %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("cache: fsync dir %s: %w", dir, err)
	}
	return nil
}
