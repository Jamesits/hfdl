package logging

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
)

// extraSink is an additionally attached slog.Handler (the otelslog OTLP
// bridge). Because AttachSlog receives no context, the pre-attach ring
// replay is deferred to the first Handle (or Close) that carries one;
// replayMu makes exactly one goroutine perform it, and the replayed flag
// keeps every later record strictly after the replayed ones. upTo is the
// dedup horizon: replay delivers ring records with Seq <= upTo, and Handle
// delivers live records only when Seq > upTo, so a record in flight during
// attach is delivered exactly once (never both replayed and handled live).
type extraSink struct {
	h        slog.Handler
	upTo     uint64 // ring Seq horizon captured at attach time
	replayMu sync.Mutex
	replayed bool
}

// AttachSlog fans out to another slog.Handler. Records already in the ring
// at attach time are replayed to it (in order, before any later record) on
// the first subsequent Handle or on Close. A nil handler is ignored.
func (h *Handler) AttachSlog(other slog.Handler) {
	if other == nil {
		return
	}
	c := h.core
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.extras = append(c.extras, &extraSink{h: other, upTo: c.ring.lastSeq()})
}

func (x *extraSink) replay(ctx context.Context, ring *Ring) {
	x.replayMu.Lock()
	defer x.replayMu.Unlock()
	if x.replayed {
		return
	}
	x.replayed = true
	for _, rec := range ring.All() {
		if rec.Seq > x.upTo {
			continue
		}
		if !x.h.Enabled(ctx, rec.Level) {
			continue
		}
		_ = x.h.Handle(ctx, recordToSlog(rec))
	}
}

// recordToSlog reconstructs a slog.Record from a ring entry for replay. The
// compact Attrs JSON is expanded back into top-level attrs; nested objects
// become slog groups via slog.Any. Attr ordering inside the JSON is not
// preserved on this path — acceptable for a best-effort bootstrap replay.
func recordToSlog(rec Record) slog.Record {
	r := slog.NewRecord(rec.Time, rec.Level, rec.Msg, 0)
	if rec.Source != "" {
		r.AddAttrs(slog.String(SourceKey, rec.Source))
	}
	if rec.Attrs != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(rec.Attrs), &m); err == nil {
			for k, v := range m {
				r.AddAttrs(slog.Any(k, v))
			}
		}
	}
	return r
}
