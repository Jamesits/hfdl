package logging

import (
	"context"
	"log/slog"
	"sync"
	"testing"
)

// captureHandler records every record it sees.
type captureHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (c *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (c *captureHandler) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, r.Clone())
	return nil
}
func (c *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return c }
func (c *captureHandler) WithGroup(_ string) slog.Handler      { return c }

func (c *captureHandler) msgs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.recs))
	for i, r := range c.recs {
		out[i] = r.Message
	}
	return out
}

func TestAttachSlogReplaysRingBeforeNewRecords(t *testing.T) {
	ring := NewRing(100)
	h := NewHandler(slog.LevelDebug, nil, ring)
	log := slog.New(h)

	log.Info("pre-1")
	log.Info("pre-2")

	cap := &captureHandler{}
	h.AttachSlog(cap)

	// Replay is deferred to the first Handle carrying a context.
	if got := cap.msgs(); len(got) != 0 {
		t.Fatalf("capture got %v before any post-attach record", got)
	}

	log.Info("post-1")

	got := cap.msgs()
	want := []string{"pre-1", "pre-2", "post-1"}
	if len(got) != len(want) {
		t.Fatalf("capture = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("capture[%d] = %q, want %q — replay must precede new records", i, got[i], want[i])
		}
	}

	log.Info("post-2")
	if got := cap.msgs(); len(got) != 4 || got[3] != "post-2" {
		t.Fatalf("fan-out after replay broken: %v", got)
	}
}

func TestAttachSlogReplayOnCloseWhenIdle(t *testing.T) {
	ring := NewRing(100)
	h := NewHandler(slog.LevelDebug, nil, ring)
	log := slog.New(h)
	log.Info("only-pre")

	cap := &captureHandler{}
	h.AttachSlog(cap)

	// No records after attach: Close still performs the replay.
	if err := h.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := cap.msgs(); len(got) != 1 || got[0] != "only-pre" {
		t.Fatalf("capture after Close = %v, want [only-pre]", got)
	}
}

func TestAttachSlogRespectsFloor(t *testing.T) {
	ring := NewRing(100)
	h := NewHandler(slog.LevelWarn, nil, ring)
	log := slog.New(h)

	cap := &captureHandler{}
	h.AttachSlog(cap)

	log.Info("below-floor")
	log.Warn("at-floor")

	got := cap.msgs()
	if len(got) != 1 || got[0] != "at-floor" {
		t.Fatalf("capture = %v — root floor must apply to extra sinks too", got)
	}
}
