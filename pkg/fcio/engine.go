package fcio

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// IOTier selects the storage/IO tier requested from the engine: fadvise is
// the default, direct is opt-in, plain serves exotic netfs. The zero value
// is TierAuto so a zero Engine config is usable.
type IOTier int

const (
	TierAuto   IOTier = iota // buffered IO + fadvise (default)
	TierDirect               // opt-in O_DIRECT, per-volume probed with fadvise downgrade
	TierPlain                // no direct IO, no fadvise (exotic netfs)
)

func (t IOTier) String() string {
	switch t {
	case TierAuto:
		return "auto"
	case TierDirect:
		return "direct"
	case TierPlain:
		return "plain"
	}
	return "unknown"
}

// FsType is the probed media class of the volume behind a path.
type FsType int

const (
	FsUnknown FsType = iota
	FsSSD
	FsHDD
	FsNetFS
)

func (t FsType) String() string {
	switch t {
	case FsSSD:
		return "ssd"
	case FsHDD:
		return "hdd"
	case FsNetFS:
		return "netfs"
	case FsUnknown:
		return "unknown"
	}
	return "invalid"
}

// CapsCache persists per-volume capability probes. Implemented structurally
// by pkg/store over its kv table; fcio never imports store. Keys look like
// "volcaps:<dev>", values are JSON.
type CapsCache interface {
	GetCaps(ctx context.Context, key string) ([]byte, error) // nil, nil if absent
	PutCaps(ctx context.Context, key string, caps []byte) error
}

// Hints declares the caller's access pattern at Open.
type Hints struct {
	Sequential bool
}

// Engine opens files on the requested IO tier and owns per-volume
// capability decisions (in-memory over the injected CapsCache).
type Engine struct {
	log  *slog.Logger
	caps CapsCache // nil ok: decisions then live only for the process
	mode IOTier

	vols sync.Map // "volcaps:<dev>" -> *volCaps (linux direct tier)

	// pool sources ReadAll scratch when set, so verify/copy reads back-pressure
	// against the same --io-buffer budget as writes. nil ⇒ ReadAll falls back
	// to private page-aligned scratch. Injected via SetPool (cmd wires the
	// engine and pool it created together); the engine never builds its own.
	pool *Pool

	// fallocateFn overrides the fallocate syscall used by Open; nil uses the
	// OS call. Test seam for the ENOSYS degradation to the no-preallocation
	// tier.
	fallocateFn func(fd int, size int64) error
}

// NewEngine builds an engine on the given tier. caps may be nil.
func NewEngine(log *slog.Logger, caps CapsCache, mode IOTier) *Engine {
	return &Engine{log: log, caps: caps, mode: mode}
}

// SetPool injects the shared buffer pool used to source ReadAll scratch, so
// large verify/copy read passes obey the pool cap and back-pressure instead
// of allocating private mappings. Wired once at startup, before any Open.
func (e *Engine) SetPool(p *Pool) { e.pool = p }

func (e *Engine) logDebug(msg string, args ...any) {
	if e.log != nil {
		e.log.Debug(msg, args...)
	}
}

// Sequential-read pipeline depths by media class: the direct tier bypasses
// kernel readahead, so reads run an application-level readahead ring; on the
// fadvise tier the ring complements FADV_SEQUENTIAL.
const (
	readDepthSSD     = 4
	readDepthHDD     = 2
	readDepthNetFS   = 2
	readDepthUnknown = 2

	// readChunkSize is one pipeline slot; on the direct tier it is rounded
	// up to a multiple of the volume alignment.
	readChunkSize int64 = 1 << 20
)

func depthForFS(fs FsType) int {
	switch fs {
	case FsSSD:
		return readDepthSSD
	case FsHDD:
		return readDepthHDD
	case FsNetFS:
		return readDepthNetFS
	default:
		return readDepthUnknown
	}
}

// readOp is one in-flight pipeline read.
type readOp struct {
	off     int64
	buf     []byte
	release func()
	n       int
	err     error
	done    chan struct{}
	direct  bool
}

// ReadAll streams f from offset 0 to EOF, invoking fn strictly in offset
// order. fn must consume p synchronously — the buffer is reused after fn
// returns. Only the direct tier runs the application-level readahead pipeline
// (it bypasses kernel readahead); the fadvise/plain tiers run a simple
// sequential loop, since FADV_SEQUENTIAL already primes the kernel readahead
// window. Scratch is sourced from the engine's Pool when one is injected (so
// a 40GiB hash pass obeys the --io-buffer cap and back-pressure), else from
// private page-aligned scratch.
func (e *Engine) ReadAll(ctx context.Context, f *File, fn func(p []byte, off int64) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fi, err := f.f.Stat()
	if err != nil {
		return fmt.Errorf("fcio: stat %s: %w", f.path, err)
	}
	size := fi.Size()
	if size == 0 {
		return nil
	}
	f.declareSequential()
	if f.tier == tierDirect {
		return e.readAllDirect(ctx, f, size, fn)
	}
	return e.readAllSequential(ctx, f, size, fn)
}

// readAllSequential is the fadvise/plain path: one buffer in flight, read in
// order via the buffered fd, and (fadvise tier) a trailing DONTNEED behind
// the consumed offset so the read pass does not pollute the page cache.
func (e *Engine) readAllSequential(ctx context.Context, f *File, size int64, fn func(p []byte, off int64) error) error {
	chunk := readChunkSize
	var buf []byte
	if e.pool != nil {
		b, err := e.pool.Get(ctx)
		if err != nil {
			return err
		}
		defer b.Release()
		if e.pool.SlabSize() < chunk {
			chunk = e.pool.SlabSize()
		}
		b.SetLen(int(chunk))
		buf = b.Data()
	} else {
		buf = make([]byte, chunk)
	}
	for off := int64(0); off < size; {
		if err := ctx.Err(); err != nil {
			return err
		}
		length := chunk
		if rem := size - off; rem < length {
			length = rem
		}
		n, err := f.readChunk(buf[:length], off, false)
		if err != nil {
			return fmt.Errorf("fcio: read %s @%d: %w", f.path, off, err)
		}
		if err := fn(buf[:n], off); err != nil {
			return err
		}
		// Evict what we just consumed (no-op off the fadvise tier), keeping a
		// large sequential read pass from thrashing the working set.
		f.DontNeed(off, int64(n))
		off += int64(n)
	}
	f.maxInflight.Store(1)
	return nil
}

// readAllDirect is the direct-tier readahead pipeline: up to
// depthForFS(media) reads in flight, delivered in offset order. The aligned
// body is read through the O_DIRECT fd, the unaligned EOF tail through the
// buffered fd.
func (e *Engine) readAllDirect(ctx context.Context, f *File, size int64, fn func(p []byte, off int64) error) error {
	depth := depthForFS(f.fsType)
	chunk := (readChunkSize + f.align - 1) / f.align * f.align
	bodyEnd := size / f.align * f.align
	if e.pool != nil {
		// Bound in-flight buffers by the pool so the pipeline can never hold
		// more slabs than exist (self-deadlock), and keep a chunk within one
		// slab while staying alignment-multiple.
		if s := e.pool.Slabs(); depth > s {
			depth = s
		}
		if slab := e.pool.SlabSize(); chunk > slab {
			chunk = slab / f.align * f.align
		}
	}
	getScratch := func(n int) ([]byte, func(), error) {
		if e.pool != nil {
			b, err := e.pool.Get(ctx)
			if err != nil {
				return nil, nil, err
			}
			b.SetLen(n)
			return b.Data(), b.Release, nil
		}
		return mmapScratch(n)
	}
	var inflight []*readOp
	maxInflight := 0
	next := int64(0)
	drain := func() {
		for _, o := range inflight {
			<-o.done
			o.release()
		}
	}
	for next < size || len(inflight) > 0 {
		for next < size && len(inflight) < depth {
			if err := ctx.Err(); err != nil {
				drain()
				return err
			}
			off := next
			length := chunk
			if rem := size - off; rem < length {
				length = rem
			}
			useDirect := off+length <= bodyEnd
			buf, release, aerr := getScratch(int(length))
			if aerr != nil {
				drain()
				return fmt.Errorf("fcio: read scratch: %w", aerr)
			}
			o := &readOp{off: off, buf: buf, release: release, done: make(chan struct{}), direct: useDirect}
			go func() {
				o.n, o.err = f.readChunk(o.buf, o.off, o.direct)
				close(o.done)
			}()
			inflight = append(inflight, o)
			next += length
		}
		if len(inflight) > maxInflight {
			maxInflight = len(inflight)
		}
		cur := inflight[0]
		select {
		case <-cur.done:
		case <-ctx.Done():
			drain()
			return ctx.Err()
		}
		inflight = inflight[1:]
		if cur.err != nil {
			cur.release()
			drain()
			return fmt.Errorf("fcio: read %s @%d: %w", f.path, cur.off, cur.err)
		}
		if err := fn(cur.buf[:cur.n], cur.off); err != nil {
			cur.release()
			drain()
			return err
		}
		cur.release()
	}
	f.maxInflight.Store(int32(maxInflight))
	return nil
}
