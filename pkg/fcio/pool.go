package fcio

import (
	"context"
	"fmt"
	"log/slog"
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
	mmapped  bool // arena came from mmap (Close unmaps); false for heap fallback
}

// NewPool mmaps an arena of floor(capBytes/slabSize) slabs plus one reserved
// slab kept pre-zeroed for padding/de-sparse writes. Floor (not ceil) keeps
// the arena from ever exceeding --io-buffer. slabSize defaults to
// DefaultSlabSize when 0 and is rounded up to a multiple of slabAlign. An
// optional logger (passed from upstream — variadic so existing callers are
// unaffected) receives a debug line if the mmap arena falls back to heap.
func NewPool(slabSize, capBytes int64, log ...*slog.Logger) *Pool {
	l := slog.New(slog.DiscardHandler)
	if len(log) > 0 && log[0] != nil {
		l = log[0]
	}
	if slabSize <= 0 {
		slabSize = DefaultSlabSize
	}
	slabSize = (slabSize + slabAlign - 1) / slabAlign * slabAlign
	if capBytes < slabSize {
		capBytes = slabSize
	}
	n := int(capBytes / slabSize)
	total := int64(n+1) * slabSize
	mmapped := true
	arena, err := allocArena(total)
	if err != nil {
		// mmap is best-effort (it exists for page alignment); a heap arena
		// keeps the pool functional — direct-tier alignment checks will
		// simply reject its slabs.
		l.Debug("fcio: pool arena mmap failed; heap fallback (direct-tier slabs will be rejected)",
			"bytes", total, "err", err)
		arena = make([]byte, total)
		mmapped = false
	}
	p := &Pool{slabSize: slabSize, arena: arena, free: make(chan *Buf, n), mmapped: mmapped}
	p.zero = &Buf{pool: p, slab: arena[:slabSize:slabSize], fill: int(slabSize), zero: true}
	for i := range n {
		start := int64(i+1) * slabSize
		p.free <- &Buf{pool: p, slab: arena[start : start+slabSize : start+slabSize]}
	}
	return p
}

// Slabs reports the number of circulating (non-zero) slabs — the free-list
// capacity. Callers that stage multiple buffers before releasing any (the
// ReadAll readahead pipeline) bound their in-flight count by this to avoid
// self-deadlock on an under-provisioned pool.
func (p *Pool) Slabs() int { return cap(p.free) }

// Close unmaps the arena. Provided so a long-lived engine can reclaim the
// mapping; no current caller requires it. It is a no-op on a heap-fallback
// arena and after the first call. Bufs must not be used after Close.
func (p *Pool) Close() error {
	if !p.mmapped || p.arena == nil {
		p.arena = nil
		return nil
	}
	arena := p.arena
	p.arena = nil
	if err := freeArena(arena); err != nil {
		return fmt.Errorf("fcio: pool close: %w", err)
	}
	return nil
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

// ZeroBuf returns the pool-owned pre-zeroed slab (full slab length) used as
// the source for padding writes. It is shared, never enters the free list,
// Release on it is a no-op, and its length is never mutated — callers must
// treat it as read-only and slice their own view. Concurrent length mutation
// (the former SetLen-on-shared-buf pattern) is a data race; callers needing a
// sized zero source use ZeroSlice.
func (p *Pool) ZeroBuf() *Buf { return p.zero }

// ZeroSlice returns a fresh read-only Buf of length n (clamped to the slab
// size) backed by the shared pre-zeroed slab. Unlike SetLen on the single
// ZeroBuf, every call gets its own Buf header, so concurrent de-sparse walks
// never race on one mutable fill length. Contents are read-only; Release is a
// no-op. The buffer address is the arena base (slab-aligned) so it is usable
// on the direct tier.
func (p *Pool) ZeroSlice(n int) *Buf {
	if n < 0 {
		n = 0
	}
	if int64(n) > p.slabSize {
		n = int(p.slabSize)
	}
	return &Buf{pool: p, slab: p.zero.slab, fill: n, zero: true}
}

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
