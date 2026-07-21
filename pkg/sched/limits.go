package sched

import (
	"context"
	"encoding/json"

	"github.com/jamesits/hfdl/pkg/config"
)

// SetLimits hot-updates every limit surface: bandwidth and API
// buckets, the disk duty level, per-active-file connection counts, and the
// operator pause. The settings persist to the store kv table so a restart
// keeps them.
//
// Pause contract: MaxBandwidthBps == 0 && DiskActivePct == 0 (cmd's pause
// toggle) pauses the download and install queues; while paused the
// bandwidth bucket rate is left untouched because a 0 rate means
// *unlimited* to throttle.Bucket — the pause gate is what blocks download
// waits. Any later SetLimits with a non-pause combination resumes.
func (m *Manager) SetLimits(l config.Limits) {
	paused := l.MaxBandwidthBps == 0 && l.DiskActivePct == 0

	m.limitsMu.Lock()
	m.limits = l
	m.limitsMu.Unlock()

	if m.cfg.Bandwidth != nil && l.MaxBandwidthBps > 0 {
		m.cfg.Bandwidth.SetRate(l.MaxBandwidthBps, m.cfg.Bandwidth.Stats().Burst)
	}
	if m.cfg.API != nil && l.APIIOPS > 0 {
		burst := l.APIBurst
		if burst < 1 {
			burst = 1
		}
		m.cfg.API.SetRate(l.APIIOPS, burst)
	}
	if m.cfg.Duty != nil && l.DiskActivePct > 0 {
		// Level is the disk active percentage, passed through directly.
		m.cfg.Duty.SetLevel(l.DiskActivePct)
	}

	// Hot connection-count update across the active download set: the
	// per-file conns cap applies to files already in 'downloading'.
	if l.Conns > 0 && m.cfg.Downloader != nil {
		m.activeMu.Lock()
		for fid := range m.active {
			m.cfg.Downloader.SetParallelism(fid, l.Conns)
		}
		m.activeMu.Unlock()
	}

	m.pause.set(paused)
	if !paused {
		wake(m.wakeDownload)
		wake(m.wakeInstall)
	}

	// Best-effort persistence; the kv row is a convenience, never on the
	// critical path. Uses the manager's detached run ctx when available.
	if m.st != nil {
		if ctx := m.detachedCtx(); ctx != nil {
			if blob, err := json.Marshal(l); err == nil {
				if serr := m.st.SetKV(ctx, kvLimitsKey, string(blob)); serr != nil {
					m.log.Debug("persist limits failed", "err", serr)
				}
			}
		}
	}
}

// loadPersistedLimits restores the operator's last settings from the kv
// table. Called
// once at Run start — the first point a caller ctx exists.
func (m *Manager) loadPersistedLimits(ctx context.Context) {
	if m.st == nil {
		return
	}
	raw, err := m.st.GetKV(ctx, kvLimitsKey)
	if err != nil || raw == "" {
		return
	}
	var l config.Limits
	if err := json.Unmarshal([]byte(raw), &l); err != nil {
		m.log.Warn("ignoring corrupt persisted limits", "err", err)
		return
	}
	m.log.Info("restored persisted limits", "limits", l)
	m.SetLimits(l)
}

// detachedCtx returns a ctx that outlives run cancellation for cleanup
// writes (nil before Run starts).
func (m *Manager) detachedCtx() context.Context {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	return m.detached
}
