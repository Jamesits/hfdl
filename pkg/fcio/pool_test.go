package fcio

import (
	"context"
	"testing"
	"time"
)

func TestPoolGetBlocksUntilRelease(t *testing.T) {
	ctx := t.Context()
	p := NewPool(64<<10, 2*64<<10) // 2 slabs
	b1, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("get 1: %v", err)
	}
	b2, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("get 2: %v", err)
	}
	defer b2.Release()

	got := make(chan *Buf, 1)
	go func() {
		b, err := p.Get(ctx)
		if err == nil {
			got <- b
		}
	}()
	select {
	case <-got:
		t.Fatal("Get returned while pool exhausted")
	case <-time.After(150 * time.Millisecond):
	}
	b1.Release()
	select {
	case b := <-got:
		b.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("Get still blocked after Release")
	}
}

func TestPoolGetContextCancel(t *testing.T) {
	p := NewPool(64<<10, 64<<10)
	b, err := p.Get(t.Context())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer b.Release()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.Get(ctx); err == nil {
		t.Fatal("exhausted Get must fail with cancelled context")
	}
}

func TestPoolZeroBuf(t *testing.T) {
	p := NewPool(64<<10, 64<<10)
	z := p.ZeroBuf()
	if int64(len(z.Data())) != p.SlabSize() {
		t.Fatalf("zero buf len = %d, want slab %d", len(z.Data()), p.SlabSize())
	}
	for i, v := range z.Data()[:4096] {
		if v != 0 {
			t.Fatalf("zero buf byte %d = %d", i, v)
		}
	}
	z.Release() // no-op: pool-owned, never enters circulation
	if again := p.ZeroBuf(); again != z {
		t.Fatal("ZeroBuf must return the shared pool-owned slab")
	}
	// the single real slab must still be available exactly once
	if _, err := p.Get(t.Context()); err != nil {
		t.Fatalf("get: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.Get(ctx); err == nil {
		t.Fatal("pool must be exhausted after its one slab is checked out")
	}
}

func TestBufSetLenClamp(t *testing.T) {
	p := NewPool(64<<10, 64<<10)
	b, err := p.Get(t.Context())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer b.Release()
	if len(b.Data()) != 0 {
		t.Fatalf("fresh buf len = %d, want 0", len(b.Data()))
	}
	b.SetLen(100)
	if len(b.Data()) != 100 || int64(cap(b.Data())) != p.SlabSize() {
		t.Fatalf("len/cap = %d/%d, want 100/%d", len(b.Data()), cap(b.Data()), p.SlabSize())
	}
	b.SetLen(-5)
	if len(b.Data()) != 0 {
		t.Fatal("negative SetLen must clamp to 0")
	}
	b.SetLen(1 << 30)
	if int64(len(b.Data())) != p.SlabSize() {
		t.Fatal("oversize SetLen must clamp to slab size")
	}
}

func TestBufDoubleRelease(t *testing.T) {
	p := NewPool(64<<10, 64<<10)
	b, err := p.Get(t.Context())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	b.Release()
	b.Release() // idempotent, must not re-queue the slab
	c, err := p.Get(t.Context())
	if err != nil {
		t.Fatalf("get after release: %v", err)
	}
	defer c.Release()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.Get(ctx); err == nil {
		t.Fatal("double Release must not duplicate the slab in the free list")
	}
}

func TestPoolDefaults(t *testing.T) {
	p := NewPool(0, 0)
	if p.SlabSize() != DefaultSlabSize {
		t.Fatalf("slab = %d, want %d", p.SlabSize(), DefaultSlabSize)
	}
}
