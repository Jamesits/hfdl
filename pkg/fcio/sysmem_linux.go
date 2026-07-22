//go:build linux

package fcio

import "golang.org/x/sys/unix"

// TotalRAM reports total physical memory in bytes via sysinfo(2). The raw
// Totalram field counts Unit-sized blocks (Unit is bytes on current kernels,
// but the multiply is kept explicit so a page-reporting kernel is handled too).
// Totalram is uint32 on 32-bit arches and uint64 on 64-bit; the uint64
// conversions keep the arithmetic identical across the GOOS/GOARCH matrix.
func TotalRAM() (int64, error) {
	var si unix.Sysinfo_t
	if err := unix.Sysinfo(&si); err != nil {
		return 0, err
	}
	unit := uint64(si.Unit)
	if unit == 0 {
		unit = 1
	}
	return int64(uint64(si.Totalram) * unit), nil
}
