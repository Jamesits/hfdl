//go:build !linux

package fcio

// Plain tier never needs page-aligned buffers, so the arena is a plain heap
// allocation off Linux.
func allocArena(n int64) ([]byte, error) { return make([]byte, n), nil }
