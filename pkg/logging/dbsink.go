package logging

import (
	"context"
	"time"
)

const (
	// dbFlushInterval / dbBatchSize are the flush triggers: a batch goes out
	// every 250ms or once it holds 256 rows, whichever comes first.
	dbFlushInterval = 250 * time.Millisecond
	dbBatchSize     = 256
	// dbQueueCap bounds how far Handle can run ahead of a slow sink before
	// rows are dropped (and counted) instead of blocking the caller.
	dbQueueCap = 4096
)

// LogRow is one logs-table row as inserted by the DB sink.
type LogRow struct {
	Time   time.Time
	Level  int
	Source string
	Msg    string
	Attrs  string
}

// LogSink is implemented by store; cmd injects it, logging never imports
// store.
type LogSink interface {
	InsertLogs(ctx context.Context, rows []LogRow) error
}

// AttachDB starts the async batched DB sink: a single background goroutine
// first replays the current ring contents (pre-attach bootstrap records),
// then flushes queued rows every 250ms or 256 records. Best-effort — a sink
// error drops the batch and bumps DropCount. Records logged between the ring
// snapshot and queue activation cannot be duplicated or reordered because
// the snapshot and the queue handoff happen under the same lock. A second
// AttachDB (or one after Close) is a no-op.
func (h *Handler) AttachDB(ctx context.Context, s LogSink) {
	c := h.core
	c.mu.Lock()
	if c.dbQueue != nil || c.closed {
		c.mu.Unlock()
		return
	}
	replay := c.ring.All()
	q := make(chan LogRow, dbQueueCap)
	stop := make(chan struct{})
	done := make(chan struct{})
	c.dbQueue = q
	c.dbStop = stop
	c.dbDone = done
	c.mu.Unlock()

	rows := make([]LogRow, len(replay))
	for i, rec := range replay {
		rows[i] = LogRow{Time: rec.Time, Level: int(rec.Level), Source: rec.Source, Msg: rec.Msg, Attrs: rec.Attrs}
	}
	go c.dbLoop(ctx, s, rows, q, stop, done)
}

func (c *core) dbLoop(ctx context.Context, s LogSink, replay []LogRow, q <-chan LogRow, stop, done chan struct{}) {
	defer close(done)

	flush := func(batch []LogRow) {
		if len(batch) == 0 {
			return
		}
		if err := s.InsertLogs(ctx, batch); err != nil {
			c.drops.Add(1)
		}
	}

	for i := 0; i < len(replay); i += dbBatchSize {
		flush(replay[i:min(i+dbBatchSize, len(replay))])
	}

	ticker := time.NewTicker(dbFlushInterval)
	defer ticker.Stop()

	batch := make([]LogRow, 0, dbBatchSize)
	flushBatch := func() {
		flush(batch)
		batch = batch[:0]
	}
	// drain flushes whatever is still queued after shutdown was requested,
	// then reports done.
	drain := func() {
		for {
			select {
			case row := <-q:
				batch = append(batch, row)
				if len(batch) >= dbBatchSize {
					flushBatch()
				}
			default:
				flushBatch()
				return
			}
		}
	}

	for {
		select {
		case row := <-q:
			batch = append(batch, row)
			if len(batch) >= dbBatchSize {
				flushBatch()
			}
		case <-ticker.C:
			flushBatch()
		case <-stop:
			drain()
			return
		case <-ctx.Done():
			drain()
			return
		}
	}
}

// DropCount reports how many DB units were dropped: one per failed batch,
// one per record that found the queue full.
func (h *Handler) DropCount() int64 {
	return h.core.drops.Load()
}

// Close flushes the outstanding DB batch and stops the background goroutine,
// waiting for it to finish or for ctx to expire. It also replays any extra
// slog handler that never saw a record after being attached. Safe to call
// more than once and without any sink attached.
func (h *Handler) Close(ctx context.Context) error {
	c := h.core
	c.mu.Lock()
	extras := append([]*extraSink(nil), c.extras...)
	stop, done := c.dbStop, c.dbDone
	c.dbQueue, c.dbStop, c.dbDone = nil, nil, nil
	if stop != nil {
		close(stop)
	}
	c.closed = true
	c.mu.Unlock()

	for _, x := range extras {
		x.replay(ctx, c.ring)
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
