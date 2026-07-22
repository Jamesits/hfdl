//go:build darwin

package fcio

import "golang.org/x/sys/unix"

// TotalRAM reports total physical memory in bytes via the hw.memsize sysctl,
// which is already a byte count (unlike Linux's block-scaled sysinfo).
func TotalRAM() (int64, error) {
	n, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}
