package fcio

import (
	"context"
	"sync/atomic"
)

const (
	// DefaultSlabSize is the FastCopy mainBuf-style slab size.
	// Blocks flush in slab-sized pieces, so RAM per connection is bounded
	// regardless of the scheduling block size.
	DefaultSlabSize int64 = 8 << 20
	// DefaultPoolCap is used when the caller passes a non-positive cap.
	DefaultPoolCap int64 = 64 << 20
	// slabAlign is the alignment every slab base satisfies. mmap arenas are
	// page-aligned by construction and slab sizes are rounded up to a
	// multiple of it, so slab pointer % slabAlign == 0 always holds on
	// Linux — the precondition for direct-tier buffer addresses.
	slabAlign int64 = 4096
)

// Pool is one arena carved into aligned slabs with a free list; exhaustion
// back-pressures callers in Get, coupling fetch rate to flush rate.
type Pool struct {
	slabSize int64
	arena    []byte
	free     chan *Buf
	zero     *Buf
}

// NewPool mmaps an arena of ceil(capBytes/slabSize) slabs plus one reserved
// slab kept pre-zeroed for padding/de-sparse writes. slabSize defaults to
// DefaultSlabSize when 0 and is rounded up to a multiple of slabAlign.
func NewPool(slabSize, capBytes int64) *Pool {
	if slabSize <= 0 {
		slabSize = DefaultSlabSize
	}
	slabSize = (slabSize + slabAlign - 1) / slabAlign * slabAlign
	if capBytes < slabSize {
		capBytes = slabSize
	}
	n := int(capBytes / slabSize)
	total := int64(n+1) * slabSize
	arena, err := allocArena(total)
	if err != nil {
		// mmap is best-effort (it exists for page alignment); a heap arena
		// keeps the pool functional — direct-tier alignment checks will
		// simply reject its slabs.
		arena = make([]byte, total)
	}
	p := &Pool{slabSize: slabSize, arena: arena, free: make(chan *Buf, n)}
	p.zero = &Buf{pool: p, slab: arena[:slabSize:slabSize], fill: int(slabSize), zero: true}
	for i := range n {
		start := int64(i+1) * slabSize
		p.free <- &Buf{pool: p, slab: arena[start : start+slabSize : start+slabSize]}
	}
	return p
}

// Get takes a slab, blocking while the pool is exhausted (backpressure) or
// until ctx is done. The returned slab has len 0 and dirty contents; callers
// SetLen before use.
func (p *Pool) Get(ctx context.Context) (*Buf, error) {
	select {
	case b := <-p.free:
		b.fill = 0
		b.released.Store(false)
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ZeroBuf returns the pool-owned pre-zeroed slab used as the source for
// padding/de-sparse writes. It is shared, never enters the free list, and
// Release on it is a no-op; callers must treat its contents as read-only.
func (p *Pool) ZeroBuf() *Buf { return p.zero }

// SlabSize reports the size (capacity) of every slab.
func (p *Pool) SlabSize() int64 { return p.slabSize }

// Buf is one checked-out slab. Data has len = fill and cap = slab size.
type Buf struct {
	pool     *Pool
	slab     []byte
	fill     int
	zero     bool
	released atomic.Bool
}

// Data returns the filled portion of the slab (cap is always the slab size).
func (b *Buf) Data() []byte { return b.slab[:b.fill] }

// SetLen sets the filled length, clamped to [0, slab size].
func (b *Buf) SetLen(n int) {
	if n < 0 {
		n = 0
	}
	if n > len(b.slab) {
		n = len(b.slab)
	}
	b.fill = n
}

// Release returns the slab to the pool. It is idempotent and a no-op on the
// shared ZeroBuf. Using the Buf after Release is a caller error.
func (b *Buf) Release() {
	if b.zero || b.pool == nil {
		return
	}
	if b.released.CompareAndSwap(false, true) {
		b.pool.free <- b
	}
}
