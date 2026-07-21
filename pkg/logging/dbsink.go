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
// error drops the batch (each row counted) and bumps DropCount. No record is
// duplicated across the replay snapshot and the live queue: under the same
// c.mu that publishes the queue, AttachDB records the max Seq present in the
// snapshot (dbSince); Handle enqueues a record only when its Seq exceeds that
// horizon, so a record captured by the replay is never also queued. Handle
// writes the ring before taking c.mu, so a record in flight during attach is
// either seen by the snapshot (Seq <= horizon → replay only) or enqueued
// (Seq > horizon → queue only), never both. A second AttachDB (or one after
// Close) is a no-op.
func (h *Handler) AttachDB(ctx context.Context, s LogSink) {
	c := h.core
	c.mu.Lock()
	if c.dbQueue != nil || c.closed {
		c.mu.Unlock()
		return
	}
	replay := c.ring.All()
	c.dbSince = 0
	if n := len(replay); n > 0 {
		c.dbSince = replay[n-1].Seq // newest snapshot record is never evicted
	}
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
			// Record-level accounting: a failed batch loses every row in it,
			// not one "unit".
			c.drops.Add(int64(len(batch)))
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
			// Close already retired the queue under c.mu before closing stop,
			// so no new record can be enqueued past what drain sees.
			drain()
			return
		case <-ctx.Done():
			// Retire the queue first so Handle stops enqueuing (counts drops)
			// while drain flushes whatever is already buffered — otherwise
			// post-cancel records vanish into an unread channel.
			c.retireDBQueue()
			drain()
			return
		}
	}
}

// retireDBQueue marks the DB queue closed under c.mu: subsequent Handle sends
// see a nil queue plus dbClosed and count a drop rather than enqueue into a
// queue the sink loop no longer drains. Idempotent; safe alongside Close.
func (c *core) retireDBQueue() {
	c.mu.Lock()
	c.dbQueue = nil
	c.dbClosed = true
	c.mu.Unlock()
}

// DropCount reports how many log records were dropped from the DB sink:
// every row of a failed batch, each record that found the queue full, and
// each record logged after the sink was retired (ctx-cancel or Close).
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
	// Retiring the queue under the same lock Handle sends under closes the
	// log-vs-Close race: a record either won the lock and is buffered for the
	// sink loop's final drain, or loses it and is a counted drop.
	c.dbClosed = true
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
