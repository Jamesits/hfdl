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

	// fallocateFn overrides the fallocate syscall used by Open; nil uses the
	// OS call. Test seam for the ENOSYS degradation to the no-preallocation
	// tier.
	fallocateFn func(fd int, size int64) error
}

// NewEngine builds an engine on the given tier. caps may be nil.
func NewEngine(log *slog.Logger, caps CapsCache, mode IOTier) *Engine {
	return &Engine{log: log, caps: caps, mode: mode}
}

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

// ReadAll streams f from offset 0 to EOF through a readahead pipeline: up to
// depthForFS(f media class) reads in flight, fn invoked strictly in offset
// order. fn must consume p synchronously — the buffer is reused after fn
// returns. Buffers are self-allocated (page-aligned scratch, so the direct
// tier is safe) because Pool ownership stays with the caller.
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
	depth := depthForFS(f.fsType)
	chunk := readChunkSize
	direct := f.tier == tierDirect
	var bodyEnd int64
	if direct {
		// Aligned body goes through the O_DIRECT fd; the unaligned EOF tail
		// is read through the buffered fd.
		chunk = (chunk + f.align - 1) / f.align * f.align
		bodyEnd = size / f.align * f.align
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
			useDirect := direct && off+length <= bodyEnd
			buf, release, aerr := mmapScratch(int(length))
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
