//go:build windows

package cache

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// replaceFile atomically replaces dst with src on Windows via MoveFileEx.
// Plain os.Rename fails when dst already exists, and a remove-then-rename
// fallback has a crash window where dst does not exist at all (data loss).
// MOVEFILE_REPLACE_EXISTING replaces the target in one atomic operation;
// MOVEFILE_WRITE_THROUGH flushes the rename to disk before returning.
func replaceFile(src, dst string) error {
	from, err := windows.UTF16PtrFromString(src)
	if err != nil {
		return fmt.Errorf("cache: replace src %s: %w", src, err)
	}
	to, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return fmt.Errorf("cache: replace dst %s: %w", dst, err)
	}
	if err := windows.MoveFileEx(from, to,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return fmt.Errorf("cache: MoveFileEx %s → %s: %w", src, dst, err)
	}
	return nil
}
