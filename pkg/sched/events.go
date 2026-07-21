package sched

import (
	"context"
	"strings"
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

// maybeCooldownUpstream persists a download-side 429/503 as an
// upstream-level cooldown (distinct from the endpoint_cooldowns api/cas
// gates).
func (m *Manager) maybeCooldownUpstream(ctx context.Context, ev transfer.Event) {
	if ev.Err == nil || ev.Upstream == "" {
		return
	}
	msg := ev.Err.Error()
	var is429, is503 bool
	for _, tok := range []string{"429", "Too Many Requests"} {
		if strings.Contains(msg, tok) {
			is429 = true
		}
	}
	for _, tok := range []string{"503", "Service Unavailable"} {
		if strings.Contains(msg, tok) {
			is503 = true
		}
	}
	if !is429 && !is503 {
		return
	}
	until := m.nowFn().Add(upstreamCooldown)
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
