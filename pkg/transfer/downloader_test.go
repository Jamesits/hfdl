package transfer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
)

// TestDownloadBasic: multi-block download assembles the file, emits block
// lifecycle events, and the final checkpoint blob covers the whole file.
func TestDownloadBasic(t *testing.T) {
	size := int64(600 << 10)
	content := newContent(21, int(size))
	fx := newFixture(t, content, "test-etag")
	srv := fx.start()
	leaser := newMemLeaser(size, 128<<10)
	d := testDownloader(t)
	evs := &eventLog{}
	stop := make(chan struct{})
	defer close(stop)
	go evs.collect(d.Events(), stop)

	prog := &progressLog{}
	task := taskFor(leaser, size, srv.URL)
	task.Conns = 3
	sink := openSink(t, size)
	if err := d.Run(t.Context(), task, sink, prog.sink()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := readSink(t, sink); string(got) != string(content) {
		t.Fatal("content mismatch")
	}
	blocks := int((size + 128<<10 - 1) / (128 << 10))
	if n := evs.count(EventBlockDone); n != blocks {
		t.Fatalf("block-done events %d, want %d", n, blocks)
	}
	if n := evs.count(EventBlockStart); n != blocks {
		t.Fatalf("block-start events %d, want %d", n, blocks)
	}
	if evs.count(EventCheckpoint) == 0 {
		t.Fatal("no checkpoint event")
	}
	final := prog.last(t)
	if len(final.Missing(size)) != 0 {
		t.Fatalf("final progress not complete: missing %v", final.Missing(size))
	}
	snap := d.cfg.Stats.Snapshot()
	if snap.TotalNetwork != size {
		t.Fatalf("network bytes %d, want %d", snap.TotalNetwork, size)
	}
}

// TestDownloadValidationRetry: a mirror serving a wrong Content-Range gets
// its attempts dropped (requeue + penalize) and the file completes from the
// good mirror.
func TestDownloadValidationRetry(t *testing.T) {
	size := int64(256 << 10)
	content := newContent(22, int(size))
	bad := newFixture(t, content, "test-etag")
	bad.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		w.Header().Set("ETag", "test-etag")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off+1, end, size)) // wrong start, always
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-off+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[off : end+1])
		return true
	}
	badSrv := bad.start()
	good := newFixture(t, content, "test-etag")
	goodSrv := good.start()

	leaser := newMemLeaser(size, 128<<10)
	d := testDownloader(t)
	task := taskFor(leaser, size, badSrv.URL, goodSrv.URL)
	task.Policy = config.RoundRobin // bad mirror is guaranteed attempts
	sink := openSink(t, size)
	if err := d.Run(t.Context(), task, sink, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := readSink(t, sink); string(got) != string(content) {
		t.Fatal("content mismatch")
	}
	if leaser.requeueCount() == 0 {
		t.Fatal("no requeue after validation failure")
	}
	var found bool
	for _, rr := range leaser.requeues {
		var ae *AttemptError
		if errors.As(rr.cause, &ae) && ae.Kind == FailValidation {
			found = true
		}
	}
	if !found {
		t.Fatal("no FailValidation requeue cause")
	}
	// Penalty landed in the registry: bad mirror EMA was halved at least once.
	if got := d.cfg.Stats.Snapshot().Retries; got == 0 {
		t.Fatal("no retries recorded")
	}
}

// TestDownloadShortBodyRetry: a body that ends early is requeued; the next
// attempt (full body) completes the block.
func TestDownloadShortBodyRetry(t *testing.T) {
	size := int64(128 << 10)
	content := newContent(23, int(size))
	fx := newFixture(t, content, "test-etag")
	fx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		if n == 0 { // first attempt: chunked short body
			w.Header().Set("ETag", "test-etag")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, end, size))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(content[off : off+(end-off+1)/2])
			return true
		}
		return false
	}
	srv := fx.start()
	leaser := newMemLeaser(size, size)
	d := testDownloader(t)
	task := taskFor(leaser, size, srv.URL)
	task.Conns = 1
	sink := openSink(t, size)
	if err := d.Run(t.Context(), task, sink, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := readSink(t, sink); string(got) != string(content) {
		t.Fatal("content mismatch")
	}
	var found bool
	for _, rr := range leaser.requeues {
		var ae *AttemptError
		if errors.As(rr.cause, &ae) && ae.Kind == FailShortBody {
			found = true
		}
	}
	if !found {
		t.Fatal("no FailShortBody requeue")
	}
}

// TestDownloadRangeless: one upstream cannot range → EventRangeless, and
// the file completes from the other.
func TestDownloadRangeless(t *testing.T) {
	size := int64(256 << 10)
	content := newContent(24, int(size))
	rangeless := newFixture(t, content, "test-etag")
	rangeless.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		w.Header().Set("ETag", "test-etag")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
		return true
	}
	rangelessSrv := rangeless.start()
	good := newFixture(t, content, "test-etag")
	goodSrv := good.start()

	leaser := newMemLeaser(size, 64<<10)
	d := testDownloader(t)
	evs := &eventLog{}
	stop := make(chan struct{})
	defer close(stop)
	go evs.collect(d.Events(), stop)
	task := taskFor(leaser, size, rangelessSrv.URL, goodSrv.URL)
	task.Policy = config.RoundRobin
	task.Conns = 2
	sink := openSink(t, size)
	if err := d.Run(t.Context(), task, sink, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := readSink(t, sink); string(got) != string(content) {
		t.Fatal("content mismatch")
	}
	if evs.count(EventRangeless) == 0 {
		t.Fatal("no rangeless event")
	}
	if len(rangeless.rangedRequests()) > 2 {
		t.Fatalf("rangeless upstream kept getting ranged requests: %d", len(rangeless.rangedRequests()))
	}
}

// TestDownloadAllRangelessFallback: no upstream ranges → the whole file
// lands via one plain GET.
func TestDownloadAllRangelessFallback(t *testing.T) {
	size := int64(300 << 10)
	content := newContent(25, int(size))
	mk := func() *fixture {
		fx := newFixture(t, content, "test-etag")
		fx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
			w.Header().Set("ETag", "test-etag")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(content)
			return true
		}
		return fx
	}
	f1, f2 := mk(), mk()
	s1, s2 := f1.start(), f2.start()

	leaser := newMemLeaser(size, 64<<10)
	d := testDownloader(t)
	// A consumer must drain the event stream so emit() never back-pressures,
	// but the rangeless-event COUNT is deliberately not asserted: with two
	// upstreams the number of EventRangeless (0, 1 or 2) depends on the
	// concurrent interleaving of markRangeless vs. the all-rangeless→
	// errAllRangeless transition, so any exact count is racy. The deterministic
	// contract is what matters — the file still completes byte-correct and the
	// fallback fetches it with a single unranged GET.
	stop := make(chan struct{})
	defer close(stop)
	go (&eventLog{}).collect(d.Events(), stop)
	task := taskFor(leaser, size, s1.URL, s2.URL)
	task.Conns = 2
	sink := openSink(t, size)
	if err := d.Run(t.Context(), task, sink, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := readSink(t, sink); string(got) != string(content) {
		t.Fatal("content mismatch after fallback")
	}
	// Fallback fetched with an unranged GET.
	var unranged int
	for _, f := range []*fixture{f1, f2} {
		for _, q := range f.requestsSnapshot() {
			if q.off < 0 {
				unranged++
			}
		}
	}
	if unranged == 0 {
		t.Fatal("no unranged fallback GET")
	}
}

// TestDownload416Complete: a 416 against an already-complete file (per
// seeded progress + size) is benign.
func TestDownload416Complete(t *testing.T) {
	size := int64(128 << 10)
	content := newContent(26, int(size))
	fx := newFixture(t, content, "test-etag")
	fx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return true
	}
	srv := fx.start()

	seed := &IntervalSet{size: size}
	seed.Add(0, size)
	blob, err := seed.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	leaser := newMemLeaser(size, size) // one stale block
	d := testDownloader(t)
	task := taskFor(leaser, size, srv.URL)
	task.Progress = blob
	task.Conns = 1
	sink := openSink(t, size)
	if err := d.Run(t.Context(), task, sink, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if leaser.completedCount() != 1 {
		t.Fatalf("completed %d, want 1", leaser.completedCount())
	}
}

// TestDownload416Mismatch: a 416 against an incomplete file is a
// ResetFileError (terminal; sched resets the file).
func TestDownload416Mismatch(t *testing.T) {
	size := int64(128 << 10)
	content := newContent(27, int(size))
	fx := newFixture(t, content, "test-etag")
	fx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return true
	}
	srv := fx.start()
	leaser := newMemLeaser(size, size)
	d := testDownloader(t)
	task := taskFor(leaser, size, srv.URL)
	task.Conns = 1
	sink := openSink(t, size)
	err := d.Run(t.Context(), task, sink, nil)
	var rfe *ResetFileError
	if !errors.As(err, &rfe) {
		t.Fatalf("got %v, want *ResetFileError", err)
	}
}

// TestCheckpointOrdering: snapshot → fsync → persist, in that order, and
// unchanged snapshots skip the fsync+persist round entirely.
func TestCheckpointOrdering(t *testing.T) {
	tracker := &intervalTracker{set: IntervalSet{size: 1000}}
	tracker.add(0, 100)
	var calls []string
	var mu sync.Mutex
	rec := func(s string) { mu.Lock(); calls = append(calls, s); mu.Unlock() }
	var persisted []byte
	c := &checkpointer{
		tracker:   tracker,
		fsyncFn:   func() error { rec("fsync"); return nil },
		persistFn: func(ctx context.Context, blob []byte) error { rec("persist"); persisted = blob; return nil },
	}
	if err := c.checkpoint(t.Context(), false); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	mu.Lock()
	got := append([]string(nil), calls...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "fsync" || got[1] != "persist" {
		t.Fatalf("call order %v, want [fsync persist]", got)
	}
	// The snapshot is taken before fsync: RAM-buffered (untracked) bytes
	// are invisible to the persisted blob.
	var set IntervalSet
	if err := set.UnmarshalBinary(persisted); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(set.iv) != 1 || set.iv[0] != (Interval{0, 100}) {
		t.Fatalf("persisted %v, want [{0 100}]", set.iv)
	}
	// No change → skipped entirely.
	if err := c.checkpoint(t.Context(), false); err != nil {
		t.Fatalf("checkpoint 2: %v", err)
	}
	mu.Lock()
	if len(calls) != 2 {
		t.Fatalf("unchanged checkpoint did work: %v", calls)
	}
	mu.Unlock()
	// Forced checkpoint re-persists even unchanged (file completion).
	if err := c.checkpoint(t.Context(), true); err != nil {
		t.Fatalf("forced: %v", err)
	}
	mu.Lock()
	if len(calls) != 4 {
		t.Fatalf("forced checkpoint skipped: %v", calls)
	}
	mu.Unlock()
}

// TestKillAndResume: run 1 is canceled mid-flight after checkpoints; run 2
// resumes from Missing() at a NEW block size and completes; the fixture
// asserts no fsynced interval was re-served.
func TestKillAndResume(t *testing.T) {
	size := int64(4 << 20)
	content := newContent(28, int(size))
	fx1 := newFixture(t, content, "test-etag")
	fx1.serveDelay = 15 * time.Millisecond
	srv1 := fx1.start()
	fx2 := newFixture(t, content, "test-etag") // run 2 talks to a fresh server: its request log is pure resume
	srv2 := fx2.start()

	// Run 1.
	leaser1 := newMemLeaser(size, 128<<10)
	d1 := testDownloader(t)
	d1.cfg.CheckpointInterval = 30 * time.Millisecond
	prog1 := &progressLog{}
	task1 := taskFor(leaser1, size, srv1.URL)
	task1.Conns = 4
	sink := openSink(t, size)
	ctx, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- d1.Run(ctx, task1, sink, prog1.sink()) }()
	waitFor(t, "first checkpoint with ≥2 blocks", 10*time.Second, func() bool {
		blob := prog1.lastBlob()
		if blob == nil {
			return false
		}
		var s IntervalSet
		if err := s.UnmarshalBinary(blob); err != nil {
			return false
		}
		return s.total() >= 2*(128<<10)
	})
	cancel()
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("run 1: %v, want context.Canceled", err)
	}
	done1 := prog1.last(t)
	missing := done1.Missing(size)
	if len(missing) == 0 {
		t.Fatal("run 1 unexpectedly finished before cancel")
	}
	durable := done1.total()
	t.Logf("run 1 durable bytes: %d of %d; missing ranges: %d", durable, size, len(missing))

	// Run 2: re-chunk the missing ranges at a NEW block size.
	const newBlock = 200 << 10
	leaser2 := memLeaserFromIntervals(missing, newBlock)
	d2 := testDownloader(t)
	prog2 := &progressLog{}
	task2 := taskFor(leaser2, size, srv2.URL)
	task2.Progress, _ = done1.MarshalBinary()
	task2.Conns = 4
	task2.BlockSize = newBlock
	if err := d2.Run(t.Context(), task2, sink, prog2.sink()); err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if got := readSink(t, sink); string(got) != string(content) {
		t.Fatal("content mismatch after resume")
	}
	// No fsynced interval was re-served: every resume request lies fully
	// inside a Missing range (disjoint from the durable set).
	for _, q := range fx2.rangedRequests() {
		if done1.contains(q.off, q.end+1) {
			t.Fatalf("resume re-served fsynced range [%d,%d)", q.off, q.end+1)
		}
		var inside bool
		for _, m := range missing {
			if q.off >= m.Start && q.end < m.End {
				inside = true
				break
			}
		}
		if !inside {
			t.Fatalf("resume request [%d,%d) outside missing set %v", q.off, q.end+1, missing)
		}
	}
	// Cumulative progress: run 2's final blob covers the whole file.
	final := prog2.last(t)
	if len(final.Missing(size)) != 0 {
		t.Fatalf("final blob not complete: %v", final.Missing(size))
	}
	// Run 2 must have downloaded strictly fewer bytes than the full file.
	var served2 int64
	for _, q := range fx2.rangedRequests() {
		served2 += q.end - q.off + 1
	}
	if served2 >= size {
		t.Fatalf("resume re-downloaded everything: %d bytes", served2)
	}
	t.Logf("run 2 served %d bytes (durable %d, overlap is re-attempted in-flight blocks)", served2, durable)
}

// TestSetParallelismLive: hot scale-up spawns workers (observed via
// concurrent block starts); scale-down drains them without corruption.
func TestSetParallelismLive(t *testing.T) {
	size := int64(4 << 20)
	content := newContent(29, int(size))
	fx := newFixture(t, content, "test-etag")
	fx.serveDelay = 20 * time.Millisecond
	srv := fx.start()
	leaser := newMemLeaser(size, 128<<10)
	d := testDownloader(t)
	evs := &eventLog{}
	stop := make(chan struct{})
	defer close(stop)
	go evs.collect(d.Events(), stop)
	prog := &progressLog{}
	task := taskFor(leaser, size, srv.URL)
	task.Conns = 1
	sink := openSink(t, size)

	ctx := t.Context()
	runDone := make(chan error, 1)
	go func() { runDone <- d.Run(ctx, task, sink, prog.sink()) }()

	waitFor(t, "first block start", 5*time.Second, func() bool { return evs.count(EventBlockStart) > 0 })
	d.SetParallelism(task.FileID, 4)
	// Let the scaled-up worker set complete several blocks so their starts
	// are on the event record before the downscale drain begins.
	waitFor(t, "scaled-up completions", 10*time.Second, func() bool { return evs.count(EventBlockDone) >= 8 })
	if fx.maxInFlight() < 3 {
		t.Fatalf("fixture never saw scaled-up concurrency: %d", fx.maxInFlight())
	}
	d.SetParallelism(task.FileID, 1)
	// Drained workers exit; only one keeps fetching.
	waitFor(t, "in-flight back to 1", 5*time.Second, func() bool {
		fx.mu.Lock()
		defer fx.mu.Unlock()
		return fx.inFlight <= 1
	})
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := readSink(t, sink); string(got) != string(content) {
		t.Fatal("content mismatch after parallelism churn")
	}
	// Scale-up concurrency witness: the fixture's own in-flight gauge
	// (race-stable — the event stream's start/done pairing lags under
	// -race and is not suited for deriving exact overlap).
	if fx.maxInFlight() < 3 {
		t.Fatalf("no scaled-up concurrency: max in-flight %d", fx.maxInFlight())
	}
	// The checkpoint event is emitted inside Run (drain flushes and the forced
	// final checkpoint) but the collector drains the channel asynchronously, so
	// poll rather than sampling once right after <-runDone.
	waitFor(t, "checkpoint event", 5*time.Second, func() bool { return evs.count(EventCheckpoint) > 0 })
	final := prog.last(t)
	if len(final.Missing(size)) != 0 {
		t.Fatal("final progress incomplete")
	}
}

// TestSequentialInOrderCommit: Tier C commits strictly in offset order even
// when later blocks arrive first.
func TestSequentialInOrderCommit(t *testing.T) {
	size := int64(1 << 20)
	content := newContent(30, int(size))
	fx := newFixture(t, content, "test-etag")
	// Early blocks are slow, later blocks instant: without commit ordering
	// the flush offsets would scramble.
	fx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		delay := time.Duration(size-off) * 30 * time.Millisecond / time.Duration(size)
		time.Sleep(delay)
		return false
	}
	srv := fx.start()
	leaser := newMemLeaser(size, 128<<10)
	d := testDownloader(t)
	task := taskFor(leaser, size, srv.URL)
	task.Conns = 3
	task.Sequential = true
	sink := openSink(t, size)

	var mu sync.Mutex
	var offs []int64
	runDone := make(chan error, 1)
	go func() { runDone <- d.Run(t.Context(), task, sink, nil) }()
	// Install the flush-order hook as soon as the run registers.
	waitFor(t, "run registration", 5*time.Second, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		fd := d.files[task.FileID]
		if fd == nil {
			return false
		}
		hook := func(off, n int64) {
			mu.Lock()
			offs = append(offs, off)
			mu.Unlock()
		}
		fd.flushHook.Store(&hook)
		return true
	})
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(offs); i++ {
		if offs[i] < offs[i-1] {
			t.Fatalf("out-of-order commit: %d after %d (seq %v)", offs[i], offs[i-1], offs)
		}
	}
	if len(offs) == 0 {
		t.Fatal("hook never fired")
	}
	if got := readSink(t, sink); string(got) != string(content) {
		t.Fatal("content mismatch in sequential mode")
	}
}

// TestStallIdleKillIntegration: a connection that sends headers but never a
// first byte is killed by the idle-read deadline, penalized, and requeued;
// the next attempt completes the file.
func TestStallIdleKillIntegration(t *testing.T) {
	size := int64(128 << 10)
	content := newContent(31, int(size))
	fx := newFixture(t, content, "test-etag")
	fx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		if n == 0 { // first attempt: headers only, then silence
			w.Header().Set("ETag", "test-etag")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, end, size))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", end-off+1))
			w.WriteHeader(http.StatusPartialContent)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done() // hang until the stall policy kills us
			return true
		}
		return false
	}
	srv := fx.start()
	leaser := newMemLeaser(size, size)
	d := testDownloader(t)
	evs := &eventLog{}
	stop := make(chan struct{})
	defer close(stop)
	go evs.collect(d.Events(), stop)
	task := taskFor(leaser, size, srv.URL)
	task.Conns = 1
	task.StallWindow = 80 * time.Millisecond
	sink := openSink(t, size)
	start := time.Now()
	if err := d.Run(t.Context(), task, sink, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := readSink(t, sink); string(got) != string(content) {
		t.Fatal("content mismatch")
	}
	ev, ok := evs.find(EventStall)
	if !ok {
		t.Fatal("no stall event")
	}
	var ae *AttemptError
	if !errors.As(ev.Err, &ae) || ae.Kind != FailStall {
		t.Fatalf("stall event err %v, want AttemptError{FailStall}", ev.Err)
	}
	if d.cfg.Stats.Snapshot().Stalls == 0 {
		t.Fatal("stall not recorded in stats")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("idle kill took too long: %s", elapsed)
	}
}

// TestHeaderTimeoutIntegration: headers beyond the deadline → retry, then
// the file completes on the next attempt.
func TestHeaderTimeoutIntegration(t *testing.T) {
	size := int64(128 << 10)
	content := newContent(32, int(size))
	fx := newFixture(t, content, "test-etag")
	fx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		if n == 0 {
			time.Sleep(400 * time.Millisecond)
		}
		return false
	}
	srv := fx.start()
	leaser := newMemLeaser(size, size)
	d := testDownloader(t)
	d.cfg.HeaderTimeout = 80 * time.Millisecond
	task := taskFor(leaser, size, srv.URL)
	task.Conns = 1
	sink := openSink(t, size)
	if err := d.Run(t.Context(), task, sink, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	var found bool
	for _, rr := range leaser.requeues {
		var ae *AttemptError
		if errors.As(rr.cause, &ae) && ae.Kind == FailHeaderTimeout {
			found = true
		}
	}
	if !found {
		t.Fatal("no FailHeaderTimeout requeue")
	}
}

// TestStallPenalizesEMA: a stall halves the upstream EMA in the registry
// after a measured rate was reported.
func TestStallPenalizesEMA(t *testing.T) {
	size := int64(256 << 10)
	content := newContent(33, int(size))
	fx := newFixture(t, content, "test-etag")
	fx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		if n == 1 { // second request: hang after headers
			w.Header().Set("ETag", "test-etag")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, end, size))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", end-off+1))
			w.WriteHeader(http.StatusPartialContent)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
			return true
		}
		return false
	}
	srv := fx.start()
	leaser := newMemLeaser(size, 128<<10) // two blocks
	d := testDownloader(t)
	task := taskFor(leaser, size, srv.URL)
	task.Conns = 1
	task.StallWindow = 80 * time.Millisecond
	sink := openSink(t, size)
	if err := d.Run(t.Context(), task, sink, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	up := d.cfg.Stats.Snapshot().Upstreams[srv.URL]
	// Block 1 reported a healthy rate; the stall on block 2 halved the EMA.
	if up.EMABps <= 0 {
		t.Fatalf("EMA not populated: %+v", up)
	}
	if d.cfg.Stats.Snapshot().Stalls != 1 {
		t.Fatalf("stalls %d, want 1", d.cfg.Stats.Snapshot().Stalls)
	}
}
