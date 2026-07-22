//go:build windows

package fcio

import (
	"fmt"
	"unsafe"

	"github.com/jamesits/goinvoke"
	"golang.org/x/sys/windows"
)

// kernel32 declares the GlobalMemoryStatusEx export goinvoke resolves by field
// name. x/sys/windows does not wrap it, so it is invoked through the raw proc.
// The bare DLL name restricts the load to the Windows system directory
// (NewLazySystemDLL) instead of the general DLL search order.
type kernel32 struct {
	GlobalMemoryStatusEx *windows.LazyProc
}

// memoryStatusEx mirrors MEMORYSTATUSEX (x/sys/windows does not define it).
// dwLength must be set to the struct size before the call or
// GlobalMemoryStatusEx rejects it.
type memoryStatusEx struct {
	dwLength                uint32
	dwMemoryLoad            uint32
	ullTotalPhys            uint64
	ullAvailPhys            uint64
	ullTotalPageFile        uint64
	ullAvailPageFile        uint64
	ullTotalVirtual         uint64
	ullAvailVirtual         uint64
	ullAvailExtendedVirtual uint64
}

// TotalRAM reports total physical memory in bytes via GlobalMemoryStatusEx.
func TotalRAM() (int64, error) {
	var k32 kernel32
	if err := goinvoke.Unmarshal("kernel32.dll", &k32); err != nil {
		return 0, fmt.Errorf("load kernel32.dll: %w", err)
	}
	ms := memoryStatusEx{}
	ms.dwLength = uint32(unsafe.Sizeof(ms))
	// BOOL return: zero means failure, and only then is the last-error value
	// meaningful.
	if r, _, err := k32.GlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&ms))); r == 0 {
		return 0, fmt.Errorf("GlobalMemoryStatusEx: %w", err)
	}
	return int64(ms.ullTotalPhys), nil
}
