//go:build !linux

package fcio

// Plain tier has no alignment requirements, so pipeline scratch is a heap
// allocation.
func mmapScratch(n int) ([]byte, func(), error) {
	return make([]byte, n), func() {}, nil
}
