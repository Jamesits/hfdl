//go:build linux

package fcio

import "golang.org/x/sys/unix"

// mmapScratch allocates page-aligned pipeline scratch so O_DIRECT reads get
// a suitably aligned buffer address.
func mmapScratch(n int) ([]byte, func(), error) {
	full := (n + int(slabAlign) - 1) / int(slabAlign) * int(slabAlign)
	buf, err := unix.Mmap(-1, 0, full, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		return nil, nil, err
	}
	return buf[:n], func() { _ = unix.Munmap(buf) }, nil
}
