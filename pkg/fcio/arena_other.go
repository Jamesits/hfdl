//go:build !linux && !windows && !darwin

package fcio

// On genuinely-unknown platforms (Windows/macOS have real page-aligned arenas)
// the plain tier never needs page-aligned buffers, so the arena is a plain heap
// allocation.
func allocArena(n int64) ([]byte, error) { return make([]byte, n), nil }

// freeArena is a no-op off Linux: the heap arena is reclaimed by the GC.
func freeArena(b []byte) error { return nil }
