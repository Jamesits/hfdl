package sched

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/jamesits/hfdl/pkg/transfer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// eventPump consumes the downloader's lifecycle stream for the whole run:
// stall/retry OTel counters with the upstream attribute, upstream
// cooldown side effects for 429/503 causes, and ENOSPC detection. Stalls
// and retries are already tallied in the stats registry by transfer itself;
// this pump owns the OTel counters and the durable upstream table.
func (m *Manager) eventPump(ctx context.Context) {
	defer m.wg.Done()
	events := m.cfg.Downloader.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-events:
			m.handleEvent(ctx, ev)
		}
	}
}

func (m *Manager) handleEvent(ctx context.Context, ev transfer.Event) {
	switch ev.Kind {
	case transfer.EventBlockDone:
		// Network bytes on the shared hfdl.download.bytes counter (kind=network);
		// the disk queue tallies kind=salvage separately.
		if ev.Bytes > 0 {
			m.salvageBytes.Add(ctx, ev.Bytes, networkAttr)
		}
	case transfer.EventStall:
		m.stalls.Add(ctx, 1, upstreamAttr(ev.Upstream))
		m.log.Info("stall kill",
			"file_id", ev.FileID, "block_id", ev.BlockID,
			"upstream", ev.Upstream, "err", ev.Err)
		m.noteIOError(ctx, ev.Err)
	case transfer.EventBlockRetry:
		m.blockRetries.Add(ctx, 1, upstreamAttr(ev.Upstream))
		m.log.Debug("block retry",
			"file_id", ev.FileID, "block_id", ev.BlockID,
			"upstream", ev.Upstream, "err", ev.Err)
		m.noteIOError(ctx, ev.Err)
		m.maybeCooldownUpstream(ctx, ev)
	case transfer.EventRequeued:
		m.log.Debug("block requeued",
			"file_id", ev.FileID, "block_id", ev.BlockID,
			"upstream", ev.Upstream, "err", ev.Err)
		m.noteIOError(ctx, ev.Err)
		m.maybeCooldownUpstream(ctx, ev)
	}
}

// maybeCooldownUpstream persists a download-side 429/503 as an upstream-level
// cooldown (distinct from the endpoint_cooldowns api/cas gates). It reads the
// typed *transfer.AttemptError status and Retry-After rather than string-
// matching the error text — a 5xx that happened to contain "503" in a URL, or
// a 429 whose text was reworded, are no longer mis-/under-classified — and it
// honors the server's Retry-After when present.
func (m *Manager) maybeCooldownUpstream(ctx context.Context, ev transfer.Event) {
	if ev.Err == nil || ev.Upstream == "" {
		return
	}
	var ae *transfer.AttemptError
	if !errors.As(ev.Err, &ae) {
		return
	}
	if ae.StatusCode != http.StatusTooManyRequests && ae.StatusCode != http.StatusServiceUnavailable {
		return
	}
	d := ae.RetryAfter
	if d <= 0 {
		d = upstreamCooldown
	}
	until := m.nowFn().Add(d)
	if err := m.st.UpdateUpstream(ctx, ev.Upstream, 0, false, &until, nil); err != nil {
		m.log.Debug("persist upstream cooldown failed", "upstream", ev.Upstream, "err", err)
	}
}

// upstreamCooldown mirrors transfer's per-file park duration so the durable
// gate and the in-memory one agree across restarts.
const upstreamCooldown = 30 * time.Second

func upstreamAttr(upstream string) metric.AddOption {
	if upstream == "" {
		upstream = "unknown"
	}
	return metric.WithAttributes(attribute.String("upstream", upstream))
}

// persistUpstreamEMA folds the registry's per-upstream EMA view into the
// durable table so the EMA survives restarts.
func (m *Manager) persistUpstreamEMA(ctx context.Context) {
	snap := m.cfg.Stats.Snapshot()
	for name, u := range snap.Upstreams {
		if err := m.st.UpdateUpstream(ctx, name, u.EMABps, true, nil, nil); err != nil {
			m.log.Debug("persist upstream ema failed", "upstream", name, "err", err)
		}
	}
}
