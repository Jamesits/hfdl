package logging

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeSink collects rows; gate (when non-nil) blocks InsertLogs until closed,
// simulating a wedged DB; err makes every batch fail.
type fakeSink struct {
	mu   sync.Mutex
	rows []LogRow
	gate chan struct{}
	err  error
}

func (f *fakeSink) InsertLogs(ctx context.Context, rows []LogRow) error {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.rows = append(f.rows, rows...)
	return nil
}

func (f *fakeSink) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}

func (f *fakeSink) msgs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.rows))
	for i, r := range f.rows {
		out[i] = r.Msg
	}
	return out
}

func TestAttachDBReplaysPreAttachInOrder(t *testing.T) {
	ring := NewRing(100)
	h := NewHandler(slog.LevelDebug, nil, ring)
	log := slog.New(h)

	log.Info("pre-1")
	log.Info("pre-2")
	log.Info("pre-3")

	sink := &fakeSink{}
	h.AttachDB(t.Context(), sink)

	waitFor(t, "pre-attach replay", func() bool { return sink.len() == 3 })
	got := sink.msgs()
	for i, want := range []string{"pre-1", "pre-2", "pre-3"} {
		if got[i] != want {
			t.Fatalf("replay[%d] = %q, want %q (order must be preserved)", i, got[i], want)
		}
	}

	log.Info("post-1")
	log.Info("post-2")
	waitFor(t, "post-attach rows", func() bool { return sink.len() == 5 })
	got = sink.msgs()
	if got[3] != "post-1" || got[4] != "post-2" {
		t.Fatalf("post-attach rows = %v, replay must not duplicate or reorder", got)
	}

	if err := h.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDBBurstNeverBlocksHandle(t *testing.T) {
	ring := NewRing(8)
	h := NewHandler(slog.LevelDebug, nil, ring)
	log := slog.New(h)

	// The sink wedges until released: the queue fills and every later record
	// must be dropped, not block.
	sink := &fakeSink{gate: make(chan struct{})}
	h.AttachDB(t.Context(), sink)

	const n = 10000
	start := time.Now()
	for i := 0; i < n; i++ {
		log.Info("burst", "i", i)
	}
	elapsed := time.Since(start)
	// A blocking design would serialize on the wedged sink almost
	// immediately; 5s is a generous bound for 10k channel sends.
	if elapsed > 5*time.Second {
		t.Fatalf("10k Handle calls took %v — Handle must not block on sink IO", elapsed)
	}
	if got := h.DropCount(); got == 0 {
		t.Fatalf("DropCount = 0, want drops from the full queue behind a wedged sink")
	}

	close(sink.gate)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if sink.len() == 0 {
		t.Fatalf("sink got no rows after release")
	}
}

func TestDBSinkErrorDropsBatch(t *testing.T) {
	ring := NewRing(8)
	h := NewHandler(slog.LevelDebug, nil, ring)
	log := slog.New(h)

	sink := &fakeSink{err: errors.New("db down")}
	h.AttachDB(t.Context(), sink)

	// One full batch is enough to trigger a failing flush.
	for i := 0; i < dbBatchSize; i++ {
		log.Info("m")
	}
	waitFor(t, "drop counter", func() bool { return h.DropCount() >= 1 })
	if sink.len() != 0 {
		t.Fatalf("erroring sink recorded %d rows", sink.len())
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCloseFlushesOutstandingBatch(t *testing.T) {
	ring := NewRing(8)
	h := NewHandler(slog.LevelDebug, nil, ring)
	slog.New(h).Info("lonesome")

	sink := &fakeSink{}
	h.AttachDB(t.Context(), sink)

	// No waiting for the 250ms ticker: Close must drain and flush the
	// outstanding single-row batch before returning.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := sink.msgs(); len(got) != 1 || got[0] != "lonesome" {
		t.Fatalf("sink rows after Close = %v, want [lonesome]", got)
	}
}

func TestCloseWithoutAttach(t *testing.T) {
	h := NewHandler(slog.LevelDebug, nil, NewRing(4))
	if err := h.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := h.Close(t.Context()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
