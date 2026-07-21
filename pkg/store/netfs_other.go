//go:build !linux && !windows && !darwin

package store

// probeNetFS is a no-op only on genuinely-unknown platforms (Linux, Windows and
// macOS have real network-filesystem detection): hfdl cannot classify the
// volume here, so it does not block startup.
func probeNetFS(dir string) (kind string, netfs bool, err error) {
	return "", false, nil
}
