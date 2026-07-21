//go:build linux

package fcio

import (
	"testing"
	"unsafe"
)

func TestPoolSlabAlignment(t *testing.T) {
	p := NewPool(64<<10, 4*64<<10)
	for range 4 {
		b, err := p.Get(t.Context())
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		ptr := uintptr(unsafe.Pointer(&b.Data()[:1][0]))
		if ptr%4096 != 0 {
			t.Fatalf("slab base %#x not page-aligned", ptr)
		}
		b.Release()
	}
	zptr := uintptr(unsafe.Pointer(&p.ZeroBuf().Data()[0]))
	if zptr%4096 != 0 {
		t.Fatalf("zero buf base %#x not page-aligned", zptr)
	}
}
