//go:build darwin

package fcio

import "golang.org/x/sys/unix"

// allocArena mmaps an anonymous arena: page-aligned by construction and
// zero-filled, which gives slab alignment and the pre-zeroed ZeroBuf.
func allocArena(n int64) ([]byte, error) {
	return unix.Mmap(-1, 0, int(n), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
}

// freeArena unmaps an mmap'd arena (Pool.Close). Never called on a heap
// fallback arena.
func freeArena(b []byte) error {
	return unix.Munmap(b)
}
