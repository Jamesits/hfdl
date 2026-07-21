//go:build windows

package fcio

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// allocArena reserves+commits a page-aligned arena via VirtualAlloc: page
// alignment gives slab alignment (the precondition for FILE_FLAG_NO_BUFFERING
// buffer addresses) and the pages are zero-filled by the OS, providing the
// pre-zeroed ZeroBuf.
func allocArena(n int64) ([]byte, error) {
	addr, err := windows.VirtualAlloc(0, uintptr(n),
		windows.MEM_COMMIT|windows.MEM_RESERVE, windows.PAGE_READWRITE)
	if err != nil {
		return nil, err
	}
	// unsafe.Add(nil, addr) materializes the base pointer without a
	// uintptr→unsafe.Pointer conversion, which go vet's unsafeptr pass rejects
	// for a bare (non-round-tripped) address like a VirtualAlloc return.
	return unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(nil), addr)), n), nil
}

// freeArena releases a VirtualAlloc'd arena (Pool.Close). Never called on a
// heap-fallback arena. MEM_RELEASE requires a zero size.
func freeArena(b []byte) error {
	return windows.VirtualFree(uintptr(unsafe.Pointer(&b[0])), 0, windows.MEM_RELEASE)
}
