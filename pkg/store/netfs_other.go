//go:build !linux

package store

// probeNetFS is a no-op off Linux: hfdl's netfs detection targets statfs
// magic numbers, which have no portable equivalent elsewhere.
func probeNetFS(dir string) (kind string, netfs bool, err error) {
	return "", false, nil
}
