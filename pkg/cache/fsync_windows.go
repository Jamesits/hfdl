//go:build windows

package cache

// fsyncDir is a no-op on Windows. CreateFile() is sufficient.
// https://ayende.com/blog/202660-b/fsync-ing-a-directory-on-linux-and-not-windows
func fsyncDir(string) error {
	return nil
}
