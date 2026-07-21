//go:build !windows

package cache

import "os"

// replaceFile atomically renames src onto dst. On POSIX, rename() replaces an
// existing dst in a single atomic operation, so a crash never leaves dst
// missing — no remove-then-rename gap is needed (that gap is a Windows-only
// workaround, handled in replace_windows.go).
func replaceFile(src, dst string) error {
	return os.Rename(src, dst)
}
