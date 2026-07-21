package transfer

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/logging"
	"github.com/jamesits/hfdl/pkg/otel"
	"github.com/jamesits/hfdl/pkg/stats"
	"github.com/jamesits/hfdl/pkg/throttle"
)

// Defaults mirroring the CLI/env knobs: --checkpoint-interval,
// HF_HUB_DOWNLOAD_TIMEOUT, --stall-timeout/--stall-min-bytes, and the
// per-file connection count.
const (
	defaultCheckpointInterval = 30 * time.Second
	defaultHeaderTimeout      = 10 * time.Second // HF_HUB_DOWNLOAD_TIMEOUT
	defaultStallWindow        = 15 * time.Second
	defaultStallMinBytes      = 32 << 10
	defaultConns              = 4

	minBlockSize = 4 << 20
	maxBlockSize = 64 << 20

	// writeAlign is the alignment the flush path partitions writes around:
	// aligned full slabs go through fcio.File.WriteAt (direct-tier capable),
	// head/tail fragments through WriteUnaligned. Matches fcio's slabAlign.
	writeAlign = 4096

	eventsCap        = 1024
	leaseRePoll      = 100 * time.Millisecond
	otelScope        = "hfdl.transfer"
	maxFallbackTries = 8
)

// FileTask is one file's download request. The caller (sched) has already
// loaded durable progress and re-chunked the missing ranges into the
// Leaser's blocks; transfer downloads what it leases.
type FileTask struct {
	FileID int64
	Path   string
	Size   int64
	BlobID string      // verify target + response-identity check
	Source BlockSource // nil → httpSource built from the fields below

	Repo, SHA string
	Upstreams []*Upstream
	Policy    config.UpstreamPolicy

	BlockSize     int64         // 0 → adaptive: clamp(pow2(size/conns), 4MiB, 64MiB)
	Conns         int           // <= 0 → defaultConns
	StallWindow   time.Duration // <= 0 → 15s
	StallMinBytes int64         // <= 0 → 32KiB

	Leaser BlockLeaser

	// Progress is the opaque prior-progress blob from store.LoadProgress
	// (nil on fresh files). It seeds the in-memory IntervalSet so every
	// checkpoint blob stays the full cumulative fsynced set — SaveProgress
	// is plain replace. A corrupt seed degrades to empty + a warning (sched
	// already resets the file on corrupt load).
	Progress []byte

	Sequential bool // io-mode=sequential: Tier C in-order commits
}

// Config wires process-wide dependencies into the Downloader. All fields
// come from upstream; nil fields get safe defaults (prov is never nil in
// production but defaults to otel.Noop anyway).
type Config struct {
	Log       *slog.Logger
	HTTP      *http.Client
	Bandwidth *throttle.Bucket
	Stats     *stats.Registry
	Engine    *fcio.Engine
	Pool      *fcio.Pool

	CheckpointInterval, HeaderTimeout time.Duration

	Prov *otel.Providers
}

// Downloader is the process-wide download engine: one per process, one
// fileDownload per Run.
type Downloader struct {
	cfg    Config
	log    *slog.Logger
	tracer trace.Tracer
	events chan Event

	mu    sync.Mutex
	files map[int64]*fileDownload
}

// NewDownloader builds a Downloader, filling safe defaults for nil fields.
func NewDownloader(cfg Config) *Downloader {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{}
	}
	if cfg.Bandwidth == nil {
		cfg.Bandwidth = throttle.NewBucket(0, 0) // unlimited
	}
	if cfg.Stats == nil {
		cfg.Stats = stats.New()
	}
	if cfg.Pool == nil {
		cfg.Pool = fcio.NewPool(0, 0)
	}
	if cfg.CheckpointInterval <= 0 {
		cfg.CheckpointInterval = defaultCheckpointInterval
	}
	if cfg.HeaderTimeout <= 0 {
		cfg.HeaderTimeout = defaultHeaderTimeout
	}
	if cfg.Prov == nil {
		cfg.Prov = otel.Noop()
	}
	return &Downloader{
		cfg:    cfg,
		log:    logging.Component(cfg.Log, "transfer"),
		tracer: cfg.Prov.Tracer(otelScope),
		events: make(chan Event, eventsCap),
		files:  make(map[int64]*fileDownload),
	}
}

// Events returns the buffered lifecycle stream (block start/done/retry,
// stall, checkpoint, requeued, rangeless). It is never closed; consumers
// drain for the process lifetime.
func (d *Downloader) Events() <-chan Event { return d.events }

// SetParallelism hot-updates the worker count of a running file: increase
// spawns workers leasing pending blocks; decrease signals excess workers to
// drain — each flushes its RAM buffer through fcio, records the flushed
// range in the in-memory IntervalSet (persisted at the next checkpoint),
// closes the body, and exits. Unknown fileIDs are ignored.
func (d *Downloader) SetParallelism(fileID int64, n int) {
	if n < 0 {
		n = 0
	}
	d.mu.Lock()
	fd := d.files[fileID]
	d.mu.Unlock()
	if fd == nil {
		return
	}
	fd.setParallelism(n)
}

// applyDefaults returns a copy of t with adaptive/zero fields resolved.
func applyDefaults(t *FileTask) FileTask {
	out := *t
	if out.Conns <= 0 {
		out.Conns = defaultConns
	}
	if out.BlockSize <= 0 {
		out.BlockSize = adaptiveBlockSize(out.Size, out.Conns)
	}
	if out.StallWindow <= 0 {
		out.StallWindow = defaultStallWindow
	}
	if out.StallMinBytes <= 0 {
		out.StallMinBytes = defaultStallMinBytes
	}
	return out
}

// adaptiveBlockSize is the FastCopy GenOvlSize analog: round the per-conn
// share up to a power of two, clamped to [4MiB, 64MiB].
func adaptiveBlockSize(size int64, conns int) int64 {
	if conns < 1 {
		conns = 1
	}
	v := size / int64(conns)
	if v < minBlockSize {
		return minBlockSize
	}
	p := int64(minBlockSize)
	for p < v && p < maxBlockSize {
		p <<= 1
	}
	return min(p, maxBlockSize)
}

// fileDownload is one Run: workers, buffering, stall monitor, checkpoints
// and the fallback switch for a single file.
type fileDownload struct {
	d    *Downloader
	task FileTask // defaults applied
	sink *fcio.File
	log  *slog.Logger

	src      BlockSource
	srcLabel string      // "http" | "xet" (span attribute)
	hs       *httpSource // non-nil when src is the built-in http source

	detached context.Context // WithoutCancel(run ctx): leaser/checkpoint calls
	// must reach the store even during shutdown.

	workersCtx    context.Context
	cancelWorkers context.CancelFunc

	spanCtx  context.Context
	fileSpan trace.Span

	intervals *intervalTracker
	ckpt      *checkpointer
	stall     *stallMonitor

	wg        sync.WaitGroup
	workersMu sync.Mutex
	workers   map[*workerState]struct{}

	inflight atomic.Int32
	wake     chan struct{}

	fallbackNeeded atomic.Bool

	errOnce sync.Once
	runErr  error

	attemptsMu sync.Mutex
	attempts   map[int64]int

	// Tier C in-order commit watermark (Sequential only): every byte below
	// seqCommit is flushed, and flushes happen strictly at the watermark.
	seqMu      sync.Mutex
	seqCond    *sync.Cond
	seqStarted bool
	seqCommit  int64

	flushHook atomic.Pointer[func(off, n int64)] // test seam: observe commit order
}

// Run downloads t into sink, checkpointing durable progress through the
// injected sink. It returns nil when every block completed (or the
// single-stream fallback finished), ctx.Err() on cancellation, a
// *ResetFileError when the file must be reset (416/size mismatch), or a
// terminal error.
func (d *Downloader) Run(ctx context.Context, t *FileTask, sink *fcio.File, progress ProgressSink) (err error) {
	if t == nil {
		return fmt.Errorf("transfer: nil FileTask")
	}
	if sink == nil {
		return fmt.Errorf("transfer: nil sink")
	}
	if t.Leaser == nil {
		return fmt.Errorf("transfer: nil BlockLeaser")
	}
	if t.Size < 0 {
		return fmt.Errorf("transfer: negative size %d", t.Size)
	}
	if t.Source == nil && len(t.Upstreams) == 0 {
		return fmt.Errorf("transfer: no BlockSource and no upstreams")
	}
	if progress == nil {
		progress = func(context.Context, int64, []byte) error { return nil }
	}

	task := applyDefaults(t)
	fd := &fileDownload{
		d:         d,
		task:      task,
		sink:      sink,
		log:       d.log,
		detached:  context.WithoutCancel(ctx),
		srcLabel:  "xet",
		intervals: &intervalTracker{set: IntervalSet{size: task.Size}},
		stall:     newStallMonitor(task.StallWindow, task.StallMinBytes),
		workers:   make(map[*workerState]struct{}),
		wake:      make(chan struct{}, 1),
		attempts:  make(map[int64]int),
	}
	fd.seqCond = sync.NewCond(&fd.seqMu)
	fd.workersCtx, fd.cancelWorkers = context.WithCancel(ctx)
	defer fd.cancelWorkers()

	// Seed from the prior durable blob so checkpoint snapshots stay the
	// full cumulative fsynced set across restarts (SaveProgress is replace).
	if len(task.Progress) > 0 {
		var seed IntervalSet
		if serr := seed.UnmarshalBinary(task.Progress); serr != nil || seed.size != task.Size {
			fd.log.Warn("prior progress blob unusable, starting with an empty set",
				"file_id", task.FileID, "err", serr)
		} else {
			fd.intervals.set = seed
		}
	}

	fd.src = task.Source
	if fd.src == nil {
		var seed [32]byte
		if _, serr := rand.Read(seed[:]); serr != nil {
			return fmt.Errorf("transfer: seed rng: %w", serr)
		}
		fd.hs = newHTTPSource(d.log, d.cfg.HTTP, &task, d.cfg.HeaderTimeout, seed)
		fd.src = fd.hs
		fd.srcLabel = "http"
	}

	fd.ckpt = &checkpointer{
		tracker: fd.intervals,
		fsyncFn: sink.Fsync,
		persistFn: func(pctx context.Context, blob []byte) error {
			return progress(pctx, task.FileID, blob)
		},
		onPersist: func(fsynced int64) {
			d.emit(fd.detached, Event{FileID: task.FileID, Kind: EventCheckpoint, Bytes: fsynced})
			fd.fileSpan.AddEvent("checkpoint", trace.WithAttributes(
				attribute.Int64("hfdl.fsynced_bytes", fsynced)))
		},
	}

	d.mu.Lock()
	d.files[task.FileID] = fd
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.files, task.FileID)
		d.mu.Unlock()
	}()

	d.cfg.Stats.SetFileTotal(task.FileID, task.Size)
	defer d.cfg.Stats.RemoveFile(task.FileID)

	fd.spanCtx, fd.fileSpan = d.tracer.Start(ctx, "transfer.file", trace.WithAttributes(
		attribute.Int64("hfdl.file_id", task.FileID),
		attribute.String("hfdl.path", task.Path),
		attribute.Int64("hfdl.size", task.Size),
		attribute.String("hfdl.blob_id", task.BlobID),
		attribute.String("hfdl.source", fd.srcLabel),
		attribute.Int("hfdl.conns", task.Conns),
		attribute.Int64("hfdl.block_size", task.BlockSize),
	))
	defer func() {
		if err != nil {
			fd.fileSpan.RecordError(err)
			fd.fileSpan.SetStatus(codes.Error, err.Error())
		} else {
			fd.fileSpan.SetStatus(codes.Ok, "")
		}
		fd.fileSpan.End()
	}()

	// Checkpoint ticker: per active file every CheckpointInterval.
	go func() {
		tick := time.NewTicker(d.cfg.CheckpointInterval)
		defer tick.Stop()
		for {
			select {
			case <-fd.workersCtx.Done():
				return
			case <-tick.C:
				if cerr := fd.ckpt.checkpoint(fd.detached, false); cerr != nil {
					fd.fail(cerr)
					return
				}
			}
		}
	}()
	// Tier C: wake watermark waiters on shutdown/drain.
	if task.Sequential {
		go func() {
			<-fd.workersCtx.Done()
			fd.seqMu.Lock()
			fd.seqCond.Broadcast()
			fd.seqMu.Unlock()
		}()
	}

	fd.setParallelism(task.Conns)
	fd.wg.Wait()

	if fd.fallbackNeeded.Load() && fd.runErr == nil && ctx.Err() == nil {
		fd.fileSpan.AddEvent("single-stream fallback")
		if ferr := fd.runFallback(ctx); ferr != nil {
			fd.fail(ferr)
		} else {
			fd.intervals.add(0, task.Size)
		}
	}

	// Final checkpoint: file completion / pause / shutdown — flush → fsync →
	// persist, with a live context even when the run ctx is canceled.
	ckErr := fd.ckpt.checkpoint(fd.detached, true)

	switch {
	case fd.runErr != nil:
		err = fd.runErr
	case ctx.Err() != nil:
		err = ctx.Err()
	default:
		err = ckErr
	}
	return err
}

// fail records the first terminal error and stops all workers.
func (fd *fileDownload) fail(err error) {
	fd.errOnce.Do(func() {
		fd.runErr = err
		fd.cancelWorkers()
		fd.seqMu.Lock()
		fd.seqCond.Broadcast()
		fd.seqMu.Unlock()
	})
}

// notifyWake nudges one idle worker to re-poll the leaser (coalesced).
func (fd *fileDownload) notifyWake() {
	select {
	case fd.wake <- struct{}{}:
	default:
	}
}

// setParallelism reconciles the live worker set with n.
func (fd *fileDownload) setParallelism(n int) {
	fd.workersMu.Lock()
	cur := len(fd.workers)
	for i := cur; i < n; i++ {
		ws := &workerState{}
		ws.ctx, ws.cancel = context.WithCancel(fd.workersCtx)
		fd.workers[ws] = struct{}{}
		fd.wg.Add(1)
		go fd.worker(ws)
	}
	if n < cur {
		k := cur - n
		for ws := range fd.workers {
			if k == 0 {
				break
			}
			ws.drain.Store(true)
			ws.cancel() // interrupt a blocked Lease or read: drain path flushes
			k--
		}
	}
	fd.workersMu.Unlock()
	if fd.fileSpan != nil {
		fd.fileSpan.AddEvent("parallelism change", trace.WithAttributes(
			attribute.Int("hfdl.conns", n)))
	}
	fd.log.Debug("parallelism changed", "file_id", fd.task.FileID, "conns", n)
}

// removeWorker deregisters an exited worker.
func (fd *fileDownload) removeWorker(ws *workerState) {
	fd.workersMu.Lock()
	delete(fd.workers, ws)
	fd.workersMu.Unlock()
}
