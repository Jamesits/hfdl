package main

import (
	"github.com/jamesits/hfdl/pkg/otel"
	"github.com/jamesits/hfdl/pkg/sched"
	"github.com/jamesits/hfdl/pkg/stats"
	"github.com/jamesits/hfdl/pkg/throttle"
)

// queueNames labels the four sched queues for hfdl.queue.* metrics
// (index order is pinned by the sched contract).
var queueNames = [4]string{"meta", "download", "disk", "install"}

// metricsSource adapts the runtime state onto otel.MetricsSource. The
// manager snapshot covers rates/queues/duty/cooldowns; the buckets, duty
// limiter and stats registry cover what it does not duplicate (utilization,
// waiter counts, per-upstream speeds and connections) — every otel.Metrics
// field is sourced. otel.Setup runs before the manager exists, so all
// dependencies are dereferenced lazily at collect time and are nil-safe.
type metricsSource struct {
	app *wireApp
}

// Collect produces one plain-value snapshot per collection tick. Missing
// components (telemetry collecting before wiring completes) yield zeroes.
func (m *metricsSource) Collect() otel.Metrics {
	if m.app == nil {
		return otel.Metrics{}
	}
	var snap *sched.Stats
	if m.app.manager != nil {
		snap = m.app.manager.Snapshot()
	}
	return adaptMetrics(snap, m.app.bandwidth, m.app.api, m.app.duty, m.app.reg)
}

// adaptMetrics maps every field of otel.Metrics. Sync counters (bytes, api
// requests, stalls, retries, verify failures, files completed/failed) are
// owned by the packages themselves and are not part of this adapter.
func adaptMetrics(s *sched.Stats, bandwidth, api *throttle.Bucket, duty *throttle.DutyLimiter, reg *stats.Registry) otel.Metrics {
	out := otel.Metrics{
		QueueDepth:    map[string]int64{},
		QueueInFlight: map[string]int64{},
	}
	if s != nil {
		out.DownloadSpeed = s.GlobalRate
		out.APIRate = s.APIRate
		for i, q := range s.Queues {
			out.QueueDepth[queueNames[i]] = int64(q.Depth)
			out.QueueInFlight[queueNames[i]] = int64(q.InFlight)
		}
		if s.DutyMedia != "" {
			out.DiskDutyLevel = map[string]int64{s.DutyMedia: int64(s.DutyLevel)}
			out.DiskDutyActiveRatio = map[string]float64{s.DutyMedia: s.DutyActiveRatio}
		}
		for _, c := range s.Cooldowns {
			out.Cooldowns = append(out.Cooldowns, otel.CooldownSample{
				Target:  c.Target,
				Kind:    c.Kind,
				Seconds: c.Remaining.Seconds(),
			})
		}
	}
	if bandwidth != nil {
		bs := bandwidth.Stats()
		out.BandwidthUtilization = bs.Utilization
		out.ThrottleWaiters = map[string]int64{"bandwidth": int64(bs.Waiters)}
	}
	if api != nil {
		as := api.Stats()
		if out.ThrottleWaiters == nil {
			out.ThrottleWaiters = map[string]int64{}
		}
		out.ThrottleWaiters["api"] = int64(as.Waiters)
	}
	if duty != nil {
		ds := duty.Stats()
		media := ds.Media.String()
		out.DiskDutyLevel = map[string]int64{media: int64(ds.Level)}
		out.DiskDutyActiveRatio = map[string]float64{media: ds.ActiveRatio}
	}
	if reg != nil {
		rs := reg.Snapshot()
		out.Connections = int64(rs.Conns)
		if len(rs.Upstreams) > 0 {
			out.UpstreamSpeed = map[string]float64{}
			out.UpstreamConnections = map[string]int64{}
			for up, u := range rs.Upstreams {
				out.UpstreamSpeed[up] = u.EMABps
				out.UpstreamConnections[up] = int64(u.Conns)
			}
		}
	}
	return out
}
