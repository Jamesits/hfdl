//go:build windows

package fcio

// mmapScratch allocates page-aligned pipeline scratch (VirtualAlloc, via
// allocArena) so the FILE_FLAG_NO_BUFFERING direct tier gets a suitably
// aligned buffer address.
func mmapScratch(n int) ([]byte, func(), error) {
	full := (n + int(slabAlign) - 1) / int(slabAlign) * int(slabAlign)
	buf, err := allocArena(int64(full))
	if err != nil {
		return nil, nil, err
	}
	return buf[:n], func() { _ = freeArena(buf) }, nil
}
