package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/jamesits/hfdl/pkg/fcio"
)

// ProgressSink receives durable checkpoint snapshots (fsynced bytes only);
// injected by sched, which forwards to store.SaveProgress. Primitive types
// only: no transfer↔store import.
type ProgressSink func(ctx context.Context, fileID int64, blob []byte) error

// errDrain aborts an in-flight attempt because the worker is being scaled
// down; the partial buffer is flushed and the block requeued for a peer.
var errDrain = errors.New("transfer: worker draining")

// workerState is one download goroutine. drain+cancel implement hot
// downscale: cancel interrupts any blocked Lease/read, and the drain flag
// routes the abort to the flush-and-exit path.
type workerState struct {
	drain  atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
}

// worker loops lease → attempt until the leaser runs dry (ok=false with no
// in-flight blocks), it is drained, or the run ends.
func (fd *fileDownload) worker(ws *workerState) {
	defer fd.wg.Done()
	defer fd.removeWorker(ws)
	defer ws.cancel()
	for {
		if ws.ctx.Err() != nil {
			return
		}
		leaseStart := time.Now()
		b, ok, err := fd.task.Leaser.Lease(ws.ctx, fd.task.FileID)
		if err != nil {
			if ws.ctx.Err() != nil {
				return // drain / shutdown / fallback
			}
			fd.fail(fmt.Errorf("transfer: lease file %d: %w", fd.task.FileID, err))
			return
		}
		if !ok {
			// Done only when nothing is in flight either: a peer may still
			// requeue its block, which a later Lease must pick up.
			if fd.inflight.Load() == 0 {
				return
			}
			select {
			case <-fd.wake:
			case <-time.After(leaseRePoll):
			case <-ws.ctx.Done():
			}
			continue
		}
		fd.seqBaseline(b.Offset)
		fd.inflight.Add(1)
		fd.executeBlock(ws, b, time.Since(leaseStart))
		fd.inflight.Add(-1)
		fd.notifyWake()
	}
}

// executeBlock runs one block attempt through its full lifecycle: open,
// stream, flush, then Complete or Requeue (or switch the run to the
// single-stream fallback / fail it terminally).
func (fd *fileDownload) executeBlock(ws *workerState, b Block, queueWait time.Duration) {
	fd.attemptsMu.Lock()
	fd.attempts[b.ID]++
	attempt := fd.attempts[b.ID]
	fd.attemptsMu.Unlock()

	attemptCtx, cancel := context.WithCancel(ws.ctx)
	defer cancel()

	_, span := fd.d.tracer.Start(fd.spanCtx, "transfer.block", trace.WithAttributes(
		attribute.Int("hfdl.block.idx", b.Idx),
		attribute.Int64("hfdl.block.offset", b.Offset),
		attribute.Int64("hfdl.block.length", b.Length),
		attribute.Int64("hfdl.block.queue_wait_ms", queueWait.Milliseconds()),
	))
	started := time.Now()

	body, err := fd.src.Open(attemptCtx, b.Offset, b.Length)
	if err != nil {
		fd.handleOpenError(ws, b, attempt, err, span)
		return
	}
	defer body.Close()

	// Arm the stall monitor only now that the body is open: the idle-read
	// deadline (layer 2) counts from body byte 0, not from before the request.
	// The header deadline in httpSource.do owns the pre-body connect/redirect/
	// slow-start phase; overlapping the two would double-count it and could
	// idle-kill a connection still waiting on legitimate response headers.
	conn := fd.stall.connStart(cancel)
	defer fd.stall.connEnd(conn)

	upstream := upstreamOf(body)
	span.SetAttributes(attribute.String("hfdl.upstream", upstream))
	st := fd.d.cfg.Stats
	st.ConnStart(fd.task.FileID, upstream)
	defer st.ConnEnd(fd.task.FileID, upstream)
	// Emit with the run-scoped ctx, not ws.ctx: a downscale drain cancels
	// ws.ctx, and dropping the start while the paired requeue/done is
	// delivered (detached) would corrupt consumers' in-flight accounting.
	fd.d.emit(fd.workersCtx, Event{FileID: fd.task.FileID, BlockID: b.ID, Kind: EventBlockStart, Upstream: upstream})

	read, serr := fd.streamBlock(ws, attemptCtx, conn, b, body, upstream)
	elapsed := time.Since(started)

	switch {
	case serr == nil:
		// Success: report the measured per-block rate, complete, move on.
		if elapsed > 0 && upstream != "" {
			fd.reportRate(upstream, float64(b.Length)/elapsed.Seconds())
		}
		if cerr := fd.task.Leaser.Complete(fd.detached, b); cerr != nil {
			fd.fail(fmt.Errorf("transfer: complete block %d: %w", b.ID, cerr))
			span.RecordError(cerr)
			span.SetStatus(codes.Error, cerr.Error())
			span.End()
			return
		}
		span.SetAttributes(attribute.Int64("hfdl.block.bytes", b.Length))
		span.SetStatus(codes.Ok, "")
		span.End()
		fd.d.emit(fd.detached, Event{FileID: fd.task.FileID, BlockID: b.ID, Kind: EventBlockDone, Bytes: b.Length, Upstream: upstream})

	case errors.Is(serr, errDrain):
		// Downscale drain: partial buffer already flushed by streamBlock;
		// hand the block to a peer and checkpoint the flushed range.
		fd.requeue(b, 0, errDrain)
		span.AddEvent("requeue", trace.WithAttributes(attribute.String("reason", "drain")))
		span.SetStatus(codes.Ok, "")
		span.End()
		if cerr := fd.ckpt.checkpoint(fd.detached, false); cerr != nil {
			fd.fail(cerr)
		}

	default:
		fd.handleStreamError(ws, b, attempt, serr, read, upstream, span, conn)
	}
}

// handleOpenError classifies failures before any body byte was consumed. The
// stall monitor is not yet armed here (it arms only after Open returns a body),
// so there is no connTrack to reconcile.
func (fd *fileDownload) handleOpenError(ws *workerState, b Block, attempt int, err error, span trace.Span) {
	defer span.End()

	// Fallback / drain / shutdown cancellation of the attempt context.
	if ws.ctx.Err() != nil || fd.workersCtx.Err() != nil {
		fd.requeue(b, 0, err)
		span.AddEvent("requeue", trace.WithAttributes(attribute.String("reason", "run ended")))
		span.SetStatus(codes.Ok, "")
		return
	}

	if errors.Is(err, errAllRangeless) {
		// No upstream can range this object: abandon the block schedule.
		// Requeue first so no block lease dangles, then stop the workers.
		fd.requeue(b, 0, err)
		span.AddEvent("rangeless: all upstreams")
		span.SetStatus(codes.Ok, "")
		fd.startFallback()
		return
	}

	var rns *rangeNotSatisfiableError
	if errors.As(err, &rns) {
		span.AddEvent("416")
		if fd.intervals.coversAll(fd.task.Size) {
			// Benign: the file is already complete per size + IntervalSet.
			if cerr := fd.task.Leaser.Complete(fd.detached, b); cerr != nil {
				fd.fail(fmt.Errorf("transfer: complete block %d: %w", b.ID, cerr))
			}
			span.SetStatus(codes.Ok, "")
			fd.d.emit(fd.detached, Event{FileID: fd.task.FileID, BlockID: b.ID, Kind: EventBlockDone, Upstream: rns.upstream})
			return
		}
		rfe := &ResetFileError{Reason: fmt.Sprintf("416 on incomplete file (size %d)", fd.task.Size)}
		span.RecordError(rfe)
		span.SetStatus(codes.Error, rfe.Error())
		fd.fail(rfe)
		return
	}

	var term *TerminalHTTPError
	if errors.As(err, &term) {
		span.RecordError(term)
		span.SetStatus(codes.Error, term.Error())
		fd.fail(term)
		return
	}

	var ae *AttemptError
	if errors.As(err, &ae) {
		switch ae.Kind {
		case FailRangeless:
			// 200 instead of 206: mark + requeue elsewhere, no EMA penalty.
			span.AddEvent("validation failure", trace.WithAttributes(
				attribute.String("detail", "200 to a ranged request: rangeless mark")))
			fd.retryBlock(b, attempt, ae, span)
			fd.d.emit(fd.detached, Event{FileID: fd.task.FileID, BlockID: b.ID, Kind: EventRangeless, Upstream: ae.Upstream, Err: ae})
		case FailNoHealthy:
			// Every upstream cooling down/blacklisted: wait it out.
			fd.requeue(b, retryBackoff(attempt), ae)
			span.AddEvent("requeue", trace.WithAttributes(
				attribute.String("reason", ae.Err.Error()),
				attribute.String("backoff", retryBackoff(attempt).String())))
			span.SetStatus(codes.Ok, "")
		default:
			// Net/header/status/validation: penalize and retry elsewhere.
			if ae.Kind == FailValidation {
				span.AddEvent("validation failure", trace.WithAttributes(
					attribute.String("detail", ae.Err.Error())))
			}
			fd.penalize(ae.Upstream)
			fd.d.cfg.Stats.AddRetry(ae.Upstream)
			fd.retryBlock(b, attempt, ae, span)
			fd.d.emit(fd.detached, Event{FileID: fd.task.FileID, BlockID: b.ID, Kind: EventBlockRetry, Upstream: ae.Upstream, Err: ae})
		}
		return
	}

	// Unclassified: treat as retriable network failure.
	fd.retryBlock(b, attempt, err, span)
	fd.d.emit(fd.detached, Event{FileID: fd.task.FileID, BlockID: b.ID, Kind: EventBlockRetry, Err: err})
}

// handleStreamError classifies failures after streaming began. The partial
// buffer was already flushed by streamBlock.
func (fd *fileDownload) handleStreamError(ws *workerState, b Block, attempt int, serr error, read int64, upstream string, span trace.Span, conn *connTrack) {
	defer span.End()

	if ws.drain.Load() {
		// Downscale drain (interrupt mid-read): partial buffer already
		// flushed; requeue for a peer and checkpoint the flushed range.
		fd.requeue(b, 0, errDrain)
		span.AddEvent("requeue", trace.WithAttributes(attribute.String("reason", "drain")))
		span.SetStatus(codes.Ok, "")
		if cerr := fd.ckpt.checkpoint(fd.detached, false); cerr != nil {
			fd.fail(cerr)
		}
		return
	}

	if reason := conn.killReason(); reason != stallNone {
		// Stall kill (layers 2–4): flush happened, penalize, requeue.
		fd.penalize(upstream)
		fd.d.cfg.Stats.AddStall(upstream)
		fd.retryBlock(b, attempt, serr, span)
		fd.d.emit(fd.detached, Event{FileID: fd.task.FileID, BlockID: b.ID, Kind: EventStall, Bytes: read, Upstream: upstream, Err: serr})
		fd.fileSpan.AddEvent("stall kill", trace.WithAttributes(
			attribute.String("hfdl.upstream", upstream),
			attribute.String("reason", reason.String())))
		return
	}

	if fd.workersCtx.Err() != nil || fd.fallbackNeeded.Load() {
		// Shutdown/fallback interruption: quiet requeue, no penalty.
		fd.requeue(b, 0, serr)
		span.AddEvent("requeue", trace.WithAttributes(attribute.String("reason", "run ended")))
		span.SetStatus(codes.Ok, "")
		return
	}

	// Mid-body transport failure or length mismatch.
	fd.penalize(upstream)
	fd.d.cfg.Stats.AddRetry(upstream)
	fd.retryBlock(b, attempt, serr, span)
	fd.d.emit(fd.detached, Event{FileID: fd.task.FileID, BlockID: b.ID, Kind: EventBlockRetry, Bytes: read, Upstream: upstream, Err: serr})
}

// retryBlock requeues with backoff and records the retry convention on the
// span: retried failures end Ok with a retry event; Error is terminal only.
func (fd *fileDownload) retryBlock(b Block, attempt int, cause error, span trace.Span) {
	bo := retryBackoff(attempt)
	fd.requeue(b, bo, cause)
	span.AddEvent("retry", trace.WithAttributes(
		attribute.String("reason", cause.Error()),
		attribute.String("backoff", bo.String())))
	span.SetStatus(codes.Ok, "")
}

// requeue hands a block back to the leaser (detached ctx: the store must
// hear about it even during shutdown) and emits EventRequeued.
func (fd *fileDownload) requeue(b Block, backoff time.Duration, cause error) {
	if err := fd.task.Leaser.Requeue(fd.detached, b, backoff, cause); err != nil {
		fd.log.Warn("requeue failed", "file_id", fd.task.FileID, "block_id", b.ID, "err", err)
	}
	fd.d.emit(fd.detached, Event{FileID: fd.task.FileID, BlockID: b.ID, Kind: EventRequeued, Err: cause})
}

// startFallback switches the run to single-stream mode, once.
func (fd *fileDownload) startFallback() {
	if fd.fallbackNeeded.CompareAndSwap(false, true) {
		fd.log.Info("no upstream supports ranges; single-stream fallback", "file_id", fd.task.FileID)
		fd.cancelWorkers()
	}
}

// retryBackoff is the per-item exponential backoff (±20% jitter, capped) the
// leaser applies via blocks.available_at.
func retryBackoff(attempt int) time.Duration {
	const base = 500 * time.Millisecond
	const max = 30 * time.Second
	d := base << min(attempt, 6)
	if d > max {
		d = max
	}
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

// reportRate folds a measured block rate into the per-file EMA view and the
// global registry (per-upstream and per-file EMA both).
func (fd *fileDownload) reportRate(upstream string, bps float64) {
	if fd.hs != nil {
		fd.hs.reportRate(upstream, bps)
	}
	fd.d.cfg.Stats.ReportUpstreamRate(upstream, bps)
	fd.d.cfg.Stats.ReportFileRate(fd.task.FileID, bps)
}

// penalize halves the upstream's EMA (both views) and temporarily blacklists
// it for this file so the next block goes elsewhere — stall or error.
func (fd *fileDownload) penalize(upstream string) {
	if upstream == "" {
		return
	}
	if fd.hs != nil {
		fd.hs.penalize(upstream)
		fd.hs.blacklist(upstream)
	}
	fd.d.cfg.Stats.PenalizeUpstream(upstream)
}

// streamBlock reads the body into pool slabs and flushes through fcio.
// Exactly b.Length bytes must arrive; the buffer invariant is: buf holds
// file range [base, base+fill), never crossing the block boundary. On
// drain/stall/shutdown abort it flushes the partial buffer (zero-padding
// forward within the block interior, never recorded as progress) and
// returns the cause.
func (fd *fileDownload) streamBlock(ws *workerState, ctx context.Context, conn *connTrack, b Block, body io.Reader, upstream string) (int64, error) {
	pool := fd.d.cfg.Pool
	slabSize := pool.SlabSize()
	st := fd.d.cfg.Stats

	base := b.Offset
	// Tier C: a re-attempted block may already have a committed prefix
	// (flushed ranges are the truth); discard it from the stream.
	if fd.task.Sequential {
		if skip := fd.intervals.prefixCovered(b.Offset, b.End()); skip > 0 {
			if skip >= b.Length {
				return b.Length, nil
			}
			if _, err := io.CopyN(io.Discard, body, skip); err != nil {
				return 0, fd.classifyReadErr(conn, err)
			}
			if werr := fd.d.cfg.Bandwidth.Wait(ctx, skip); werr != nil {
				return 0, fd.classifyReadErr(conn, werr)
			}
			conn.addBytes(skip)
			st.AddNetwork(skip)
			st.AddFile(fd.task.FileID, skip)
			if upstream != "" {
				st.AddUpstream(upstream, skip)
			}
			base += skip
		}
	}

	buf, err := pool.Get(ctx)
	if err != nil {
		return 0, err
	}
	defer buf.Release()
	expect := b.End() - base // bytes this attempt must deliver after any skip
	space := buf.Data()[:slabSize]
	fill := 0
	var read int64 // bytes consumed from the body (excl. skipped prefix)

	flushAbort := func() {
		// Partial flush on downscale/pause: write the buffered fragment,
		// then zero-pad forward to alignment — only within this block's own
		// unwritten interior, never recorded as progress (overwritten on
		// resume).
		if fill == 0 {
			return
		}
		if werr := fd.gatedWrite(space[:fill], base); werr != nil {
			fd.log.Warn("abort flush failed", "file_id", fd.task.FileID, "err", werr)
			return
		}
		end := base + int64(fill)
		if pad := alignUp(end) - end; pad > 0 && end+pad <= b.End() {
			zero := pool.ZeroBuf().Data()
			if zerr := fd.sink.WriteUnaligned(zero[:pad], end); zerr != nil {
				fd.log.Warn("zero-pad failed", "file_id", fd.task.FileID, "err", zerr)
			}
		}
	}

	for {
		if fill == len(space) {
			if ferr := fd.flushFullSlab(buf, &base, &fill, space); ferr != nil {
				return read, ferr
			}
		}
		if ws.drain.Load() {
			flushAbort()
			return read, errDrain
		}
		n, rerr := body.Read(space[fill:])
		if n > 0 {
			if werr := fd.d.cfg.Bandwidth.Wait(ctx, int64(n)); werr != nil {
				flushAbort()
				return read, fd.classifyReadErr(conn, werr)
			}
			conn.addBytes(int64(n))
			st.AddNetwork(int64(n))
			st.AddFile(fd.task.FileID, int64(n))
			if upstream != "" {
				st.AddUpstream(upstream, int64(n))
			}
			fill += n
			read += int64(n)
			if base+int64(fill) > b.End() {
				// More bytes than requested: identity/length violation.
				return read, &AttemptError{Upstream: upstream, Kind: FailValidation,
					Err: fmt.Errorf("body longer than requested %d bytes", b.Length)}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			// Mid-body abort: flush whatever partial progress is buffered
			// (stall kills included) so it survives to the next
			// checkpoint.
			flushAbort()
			return read, fd.classifyReadErr(conn, rerr)
		}
	}

	if read != expect {
		// EOF before the requested length.
		flushAbort()
		return read, &AttemptError{Upstream: upstream, Kind: FailShortBody,
			Err: fmt.Errorf("body ended at %d bytes, want %d", read, expect)}
	}
	// Tail fragment: never write across the block boundary.
	if fill > 0 {
		if terr := fd.gatedWrite(space[:fill], base); terr != nil {
			return read, terr
		}
	}
	return read, nil
}

// flushFullSlab flushes a full slab: aligned bases go through WriteAt
// (direct-tier capable); unaligned bases first shed their head fragment
// through WriteUnaligned so the slab remainder realigns.
func (fd *fileDownload) flushFullSlab(buf *fcio.Buf, base *int64, fill *int, space []byte) error {
	if *base%writeAlign == 0 {
		buf.SetLen(*fill)
		if err := fd.gatedWriteBuf(buf, *base); err != nil {
			return err
		}
		*base += int64(*fill)
		*fill = 0
		buf.SetLen(0)
		return nil
	}
	head := int(alignUp(*base) - *base)
	if err := fd.gatedWrite(space[:head], *base); err != nil {
		return err
	}
	copy(space, space[head:*fill])
	*base += int64(head)
	*fill -= head
	return nil
}

// gatedWrite writes p at off through the (Tier C: watermark-ordered) commit
// gate and records the flushed range. Fragments always take the buffered fd.
func (fd *fileDownload) gatedWrite(p []byte, off int64) error {
	if len(p) == 0 {
		return nil
	}
	if err := fd.seqWait(off); err != nil {
		return err
	}
	if err := fd.sink.WriteUnaligned(p, off); err != nil {
		return fmt.Errorf("transfer: write %s @%d: %w", fd.task.Path, off, err)
	}
	fd.committed(off, int64(len(p)))
	return nil
}

// gatedWriteBuf is gatedWrite for a full aligned slab (WriteAt path).
func (fd *fileDownload) gatedWriteBuf(buf *fcio.Buf, off int64) error {
	if err := fd.seqWait(off); err != nil {
		return err
	}
	if err := fd.sink.WriteAt(buf, off); err != nil {
		return fmt.Errorf("transfer: write %s @%d: %w", fd.task.Path, off, err)
	}
	fd.committed(off, int64(len(buf.Data())))
	return nil
}

// committed records a flushed range: in-memory IntervalSet (persisted at
// the next checkpoint), the test hook, and the Tier C watermark advance.
//
// Order matters (Sequential): the hook must fire before seqAdvance. The
// underlying writes are already gated in offset order by seqWait, but
// seqAdvance releases the next block's seqWait — so advancing first would let
// the woken worker record its own offset through the hook before this one
// does, surfacing a spurious out-of-order observation. Recording first pins
// the hook order to the (correct) write order the watermark enforces.
func (fd *fileDownload) committed(off, n int64) {
	fd.intervals.add(off, off+n)
	if h := fd.flushHook.Load(); h != nil {
		(*h)(off, n)
	}
	if fd.task.Sequential {
		fd.seqAdvance(off + n)
	}
}

// classifyReadErr maps a body-read failure onto the attempt taxonomy: stall
// kills (monitor canceled the attempt ctx) become FailStall; everything else
// is a transport failure.
func (fd *fileDownload) classifyReadErr(conn *connTrack, err error) error {
	if reason := conn.killReason(); reason != stallNone {
		return &AttemptError{Kind: FailStall, Err: fmt.Errorf("stall policy: %s: %w", reason, err)}
	}
	return err
}

// seqWait blocks until the watermark reaches off (Tier C in-order commit).
// Offsets at or below the watermark pass through (re-attempt overlap is
// byte-identical).
func (fd *fileDownload) seqWait(off int64) error {
	if !fd.task.Sequential {
		return nil
	}
	fd.seqMu.Lock()
	defer fd.seqMu.Unlock()
	for {
		if off <= fd.seqCommit {
			return nil
		}
		if fd.workersCtx.Err() != nil {
			return fd.workersCtx.Err()
		}
		fd.seqCond.Wait()
	}
}

// seqBaseline pins the Tier C watermark to the first leased block's
// offset. The leaser hands blocks out in offset order (documented
// assumption), so the first lease is the schedule's minimum.
func (fd *fileDownload) seqBaseline(off int64) {
	if !fd.task.Sequential {
		return
	}
	fd.seqMu.Lock()
	if !fd.seqStarted {
		fd.seqStarted = true
		fd.seqCommit = off
	}
	fd.seqMu.Unlock()
}

// seqAdvance moves the watermark after a committed flush.
func (fd *fileDownload) seqAdvance(end int64) {
	fd.seqMu.Lock()
	if end > fd.seqCommit {
		fd.seqCommit = end
	}
	fd.seqCond.Broadcast()
	fd.seqMu.Unlock()
}

// openFallback opens the whole-file single-stream body for the documented
// rangeless fallback. Only the built-in http source has a rangeless fallback
// (openWhole); a nil hs means the source can always range (xet), so the
// fallback path must never have been entered.
func (fd *fileDownload) openFallback(ctx context.Context) (io.ReadCloser, error) {
	if fd.hs == nil {
		return nil, fmt.Errorf("transfer: single-stream fallback requires the http source")
	}
	return fd.hs.openWhole(ctx)
}

// runFallback executes the documented single-stream fallback: no upstream
// can range the object, so the whole file is fetched as one GET over one
// connection. Mid-file resume is unsupported — any failure restarts the
// stream from byte 0, and no checkpoint persists until the stream
// completes.
func (fd *fileDownload) runFallback(ctx context.Context) error {
	st := fd.d.cfg.Stats
	var lastErr error
	for attempt := 1; attempt <= maxFallbackTries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		upstream, n, err := fd.fallbackStream(ctx)
		if err == nil {
			fd.log.Info("single-stream fallback complete", "file_id", fd.task.FileID, "bytes", n)
			return nil
		}
		lastErr = err
		var ae *AttemptError
		if errors.As(err, &ae) && ae.Kind == FailStall {
			st.AddStall(upstream)
			fd.d.emit(fd.detached, Event{FileID: fd.task.FileID, Kind: EventStall, Bytes: n, Upstream: upstream, Err: err})
		} else {
			st.AddRetry(upstream)
			fd.d.emit(fd.detached, Event{FileID: fd.task.FileID, Kind: EventBlockRetry, Bytes: n, Upstream: upstream, Err: err})
		}
		fd.penalize(upstream)
		var term *TerminalHTTPError
		if errors.As(err, &term) {
			return term
		}
		select {
		case <-time.After(retryBackoff(attempt)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("transfer: fallback failed after %d attempts: %w", maxFallbackTries, lastErr)
}

// fallbackStream performs one whole-file streaming attempt from offset 0.
func (fd *fileDownload) fallbackStream(ctx context.Context) (string, int64, error) {
	pool := fd.d.cfg.Pool
	slabSize := pool.SlabSize()
	st := fd.d.cfg.Stats

	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn := fd.stall.connStart(cancel)
	defer fd.stall.connEnd(conn)

	body, err := fd.openFallback(attemptCtx)
	if err != nil {
		return "", 0, err
	}
	defer body.Close()
	upstream := upstreamOf(body)
	st.ConnStart(fd.task.FileID, upstream)
	defer st.ConnEnd(fd.task.FileID, upstream)

	buf, err := pool.Get(attemptCtx)
	if err != nil {
		return upstream, 0, err
	}
	defer buf.Release()
	space := buf.Data()[:slabSize]
	fill := 0
	base := int64(0)
	var read int64
	flush := func(final bool) error {
		if fill == 0 {
			return nil
		}
		if !final && base%writeAlign == 0 && int64(fill) == slabSize {
			buf.SetLen(fill)
			if err := fd.sink.WriteAt(buf, base); err != nil {
				return err
			}
		} else {
			if err := fd.sink.WriteUnaligned(space[:fill], base); err != nil {
				return err
			}
		}
		if h := fd.flushHook.Load(); h != nil {
			(*h)(base, int64(fill))
		}
		base += int64(fill)
		fill = 0
		buf.SetLen(0)
		return nil
	}
	for {
		if fill == len(space) {
			if err := flush(false); err != nil {
				return upstream, read, err
			}
		}
		n, rerr := body.Read(space[fill:])
		if n > 0 {
			if werr := fd.d.cfg.Bandwidth.Wait(attemptCtx, int64(n)); werr != nil {
				return upstream, read, fd.classifyReadErr(conn, werr)
			}
			conn.addBytes(int64(n))
			st.AddNetwork(int64(n))
			st.AddFile(fd.task.FileID, int64(n))
			if upstream != "" {
				st.AddUpstream(upstream, int64(n))
			}
			fill += n
			read += int64(n)
			if read > fd.task.Size {
				return upstream, read, &AttemptError{Upstream: upstream, Kind: FailValidation,
					Err: fmt.Errorf("fallback body longer than size %d", fd.task.Size)}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return upstream, read, fd.classifyReadErr(conn, rerr)
		}
	}
	if err := flush(true); err != nil {
		return upstream, read, err
	}
	if read != fd.task.Size {
		return upstream, read, &AttemptError{Upstream: upstream, Kind: FailShortBody,
			Err: fmt.Errorf("fallback body ended at %d bytes, want %d", read, fd.task.Size)}
	}
	return upstream, read, nil
}

// alignUp rounds off up to the next writeAlign boundary.
func alignUp(off int64) int64 {
	return (off + writeAlign - 1) / writeAlign * writeAlign
}
