package transfer

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/otel"
	"github.com/jamesits/hfdl/pkg/stats"
	"github.com/jamesits/hfdl/pkg/throttle"
)

// memLeaser is an in-memory BlockLeaser. Requeued blocks become available
// after their backoff; Lease polls briefly so backed-off blocks are picked
// up without the run ending early.
type memLeaser struct {
	mu        sync.Mutex
	pending   []memBlock
	completed []Block
	requeues  []requeueRec
}

type memBlock struct {
	b     Block
	avail time.Time
}

type requeueRec struct {
	b       Block
	backoff time.Duration
	cause   error
}

func newMemLeaser(size, blockSize int64) *memLeaser {
	return memLeaserFromIntervals([]Interval{{0, size}}, blockSize)
}

// memLeaserFromIntervals chunks each interval into blockSize blocks (the
// resume re-chunking flow).
func memLeaserFromIntervals(ivs []Interval, blockSize int64) *memLeaser {
	l := &memLeaser{}
	id := int64(1)
	idx := 0
	for _, iv := range ivs {
		for off := iv.Start; off < iv.End; off += blockSize {
			end := min(off+blockSize, iv.End)
			l.pending = append(l.pending, memBlock{b: Block{ID: id, Idx: idx, Offset: off, Length: end - off}})
			id++
			idx++
		}
	}
	return l
}

func (l *memLeaser) Lease(ctx context.Context, fileID int64) (Block, bool, error) {
	for {
		l.mu.Lock()
		if len(l.pending) == 0 {
			l.mu.Unlock()
			return Block{}, false, nil
		}
		now := time.Now()
		earliest := time.Time{}
		for i, mb := range l.pending {
			if !mb.avail.After(now) {
				b := mb.b
				l.pending = append(l.pending[:i], l.pending[i+1:]...)
				l.mu.Unlock()
				return b, true, nil
			}
			if earliest.IsZero() || mb.avail.Before(earliest) {
				earliest = mb.avail
			}
		}
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return Block{}, false, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (l *memLeaser) Complete(ctx context.Context, b Block) error {
	l.mu.Lock()
	l.completed = append(l.completed, b)
	l.mu.Unlock()
	return nil
}

func (l *memLeaser) Requeue(ctx context.Context, b Block, backoff time.Duration, cause error) error {
	l.mu.Lock()
	l.pending = append(l.pending, memBlock{b: b, avail: time.Now().Add(backoff)})
	l.requeues = append(l.requeues, requeueRec{b: b, backoff: backoff, cause: cause})
	l.mu.Unlock()
	return nil
}

func (l *memLeaser) completedCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.completed)
}

func (l *memLeaser) requeueCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.requeues)
}

// fixture is an httptest-backed resolve server with strict range handling,
// request logging, and hooks for pathological behaviors.
type fixture struct {
	t       *testing.T
	content []byte
	etag    string

	// hooks, evaluated per request before the default handler
	beforeRange func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool // true = handled
	beforeFull  func(w http.ResponseWriter, r *http.Request, n int) bool

	mu         sync.Mutex
	requests   []fixtureReq
	inFlight   int
	maxInFly   int
	serveDelay time.Duration
}

type fixtureReq struct {
	rangeHdr string
	off, end int64 // -1 when unranged
	status   int
}

func newContent(seed uint64, size int) []byte {
	var s [32]byte
	for i := range 4 {
		s[i*8] = byte(seed >> (i * 8))
	}
	r := rand.New(rand.NewChaCha8(s))
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(r.Uint64())
	}
	return out
}

func newFixture(t *testing.T, content []byte, etag string) *fixture {
	f := &fixture{t: t, content: content, etag: etag}
	return f
}

func (f *fixture) start() *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(f.handle))
	f.t.Cleanup(srv.Close)
	return srv
}

func (f *fixture) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxInFly {
		f.maxInFly = f.inFlight
	}
	delay := f.serveDelay
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()
	if delay > 0 {
		time.Sleep(delay)
	}
	f.mu.Lock()
	n := len(f.requests)
	f.mu.Unlock()

	rh := r.Header.Get("Range")
	if rh == "" {
		f.mu.Lock()
		f.requests = append(f.requests, fixtureReq{rangeHdr: "", off: -1, end: -1, status: http.StatusOK})
		f.mu.Unlock()
		if f.beforeFull != nil && f.beforeFull(w, r, n) {
			return
		}
		w.Header().Set("ETag", f.etag)
		w.Header().Set("Content-Length", strconv.Itoa(len(f.content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(f.content)
		return
	}
	var off, end int64
	if _, err := fmt.Sscanf(rh, "bytes=%d-%d", &off, &end); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if off < 0 || end >= int64(len(f.content)) || off > end {
		f.mu.Lock()
		f.requests = append(f.requests, fixtureReq{rangeHdr: rh, off: off, end: end, status: http.StatusRequestedRangeNotSatisfiable})
		f.mu.Unlock()
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	f.mu.Lock()
	f.requests = append(f.requests, fixtureReq{rangeHdr: rh, off: off, end: end, status: http.StatusPartialContent})
	f.mu.Unlock()
	if f.beforeRange != nil && f.beforeRange(w, r, off, end, n) {
		return
	}
	w.Header().Set("ETag", f.etag)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, end, len(f.content)))
	w.Header().Set("Content-Length", strconv.FormatInt(end-off+1, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(f.content[off : end+1])
}

func (f *fixture) rangedRequests() []fixtureReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fixtureReq, 0, len(f.requests))
	for _, q := range f.requests {
		if q.off >= 0 {
			out = append(out, q)
		}
	}
	return out
}

func (f *fixture) requestsSnapshot() []fixtureReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fixtureReq(nil), f.requests...)
}

func (f *fixture) maxInFlight() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxInFly
}

// eventLog drains Downloader.Events for assertions.
type eventLog struct {
	mu  sync.Mutex
	evs []Event
}

func (e *eventLog) collect(ch <-chan Event, stop <-chan struct{}) {
	for {
		select {
		case ev := <-ch:
			e.mu.Lock()
			e.evs = append(e.evs, ev)
			e.mu.Unlock()
		case <-stop:
			return
		}
	}
}

func (e *eventLog) count(kind EventKind) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, ev := range e.evs {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}

func (e *eventLog) find(kind EventKind) (Event, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ev := range e.evs {
		if ev.Kind == kind {
			return ev, true
		}
	}
	return Event{}, false
}

// testDownloader builds a Downloader with small slabs and no checkpoint
// ticker (tests control checkpoints explicitly).
func testDownloader(t *testing.T) *Downloader {
	t.Helper()
	return NewDownloader(Config{
		Log:                slog.New(slog.DiscardHandler),
		HTTP:               &http.Client{},
		Bandwidth:          throttle.NewBucket(0, 0),
		Stats:              stats.New(),
		Pool:               fcio.NewPool(64<<10, 4<<20),
		CheckpointInterval: time.Hour,
		HeaderTimeout:      2 * time.Second,
		Prov:               otel.Noop(),
	})
}

// openSink creates a plain-tier cache file of the given size.
func openSink(t *testing.T, size int64) *fcio.File {
	t.Helper()
	e := fcio.NewEngine(slog.New(slog.DiscardHandler), nil, fcio.TierPlain)
	f, err := e.Open(t.Context(), filepath.Join(t.TempDir(), "blob"), size, fcio.Hints{})
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// readSink reads the whole cache file back for content comparison.
func readSink(t *testing.T, f *fcio.File) []byte {
	t.Helper()
	b, err := os.ReadFile(f.Path())
	if err != nil {
		t.Fatalf("read sink %s: %v", f.Path(), err)
	}
	return b
}

// progressLog records ProgressSink blobs.
type progressLog struct {
	mu    sync.Mutex
	blobs [][]byte
}

func (p *progressLog) sink() ProgressSink {
	return func(ctx context.Context, fileID int64, blob []byte) error {
		p.mu.Lock()
		p.blobs = append(p.blobs, append([]byte(nil), blob...))
		p.mu.Unlock()
		return nil
	}
}

// lastBlob returns the newest blob, or nil when none landed yet.
func (p *progressLog) lastBlob() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.blobs) == 0 {
		return nil
	}
	return p.blobs[len(p.blobs)-1]
}

func (p *progressLog) last(t *testing.T) *IntervalSet {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.blobs) == 0 {
		t.Fatal("no progress blobs")
	}
	var s IntervalSet
	if err := s.UnmarshalBinary(p.blobs[len(p.blobs)-1]); err != nil {
		t.Fatalf("decode last blob: %v", err)
	}
	return &s
}

// waitFor polls cond until true or the timeout elapses.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// taskFor builds a minimal FileTask against one fixture server.
func taskFor(leaser *memLeaser, size int64, upstreams ...string) *FileTask {
	ups := make([]*Upstream, len(upstreams))
	for i, u := range upstreams {
		ups[i] = &Upstream{Endpoint: u}
	}
	return &FileTask{
		FileID:      1,
		Path:        "org/repo/file.bin",
		Size:        size,
		BlobID:      "test-etag",
		Repo:        "org/repo",
		SHA:         "abc123",
		Upstreams:   ups,
		BlockSize:   128 << 10,
		Conns:       2,
		StallWindow: 5 * time.Second,
		Leaser:      leaser,
	}
}
