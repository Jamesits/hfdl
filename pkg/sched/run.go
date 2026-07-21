package sched

import (
	"context"
	"errors"
	"time"

	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/throttle"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Run starts every worker pool and the event pump, then blocks until the
// queues drain: all jobs terminal (done|error). It returns the aggregate of
// terminal errors (errors.Join) after the remaining work drained, or
// ctx.Err() when cancelled — in-flight files checkpoint and requeue on the
// way out (final checkpoints persist via the detached ctx).
func (m *Manager) Run(ctx context.Context) error {
	m.runMu.Lock()
	if m.running {
		m.runMu.Unlock()
		return errors.New("sched: Run already in progress")
	}
	m.running = true
	runCtx, cancel := context.WithCancel(ctx)
	m.runCancel = cancel
	m.detached = context.WithoutCancel(ctx)
	m.runMu.Unlock()

	finish := func(err error) error {
		cancel()
		m.wg.Wait()
		m.runMu.Lock()
		m.running = false
		m.runCancel = nil
		m.runMu.Unlock()
		return err
	}

	m.loadPersistedLimits(ctx)

	// Disk-queue depth and duty media follow the cache volume's fs class.
	fs, err := fcio.ProbeFs(ctx, m.cfg.Cache.Root())
	if err != nil {
		fs = fcio.FsUnknown
	}
	if m.cfg.Duty != nil {
		m.cfg.Duty.SetMedia(fsToMedia(fs))
	}

	// Upstreams: one row per configured endpoint (EMA/gates persisted).
	for ep := range m.cfg.Clients {
		if err := m.st.UpsertUpstream(ctx, ep); err != nil {
			return finish(err)
		}
	}

	// Startup recovery, then periodic (every lease interval).
	var rec store.RecoveryStats
	if err := m.storeCall(ctx, func() error {
		var rerr error
		rec, rerr = m.st.Recover(ctx, m.nowFn())
		return rerr
	}); err != nil {
		return finish(err)
	} else if rec != (store.RecoveryStats{}) {
		m.log.Info("startup recovery requeued rows",
			"repos", rec.Repos, "files", rec.Files, "verifying", rec.Verifying,
			"salvaging", rec.Salvaging, "blocks", rec.Blocks,
			"job_files", rec.JobFiles, "references", rec.References)
	}

	// Worker pools. DryRun lists only: no payload moves.
	m.wg.Add(metaWorkers)
	for range metaWorkers {
		go m.metaWorker(runCtx)
	}
	if !m.cfg.DryRun {
		m.wg.Add(1)
		go m.downloadOrchestrator(runCtx)

		diskOverride := m.currentLimits().DiskWorkers
		diskN := diskWorkerCount(fs, diskOverride)
		if hardCap := maxDiskWorkers(); diskOverride > hardCap {
			m.log.Warn("disk-workers clamped to keep a CPU free for network/meta/install",
				"requested", diskOverride, "workers", diskN, "cap", hardCap)
		}
		m.log.Debug("disk queue depth", "workers", diskN, "fs", fs, "override", diskOverride)
		m.wg.Add(diskN)
		for range diskN {
			go m.diskWorker(runCtx)
		}

		// Cheap pool: cache-mode symlink/hardlink installs (metadata ops, no
		// duty gate). Copy pool: local-dir reflink/copy installs — kept
		// separate so a long copy can't starve cheap symlinks; copies serialize
		// per (src,dst) volume pair and pace under the DutyLimiter inside the
		// installer.
		m.wg.Add(installCheapWorkers)
		for range installCheapWorkers {
			go m.installWorker(runCtx, store.DestModeCache)
		}
		m.wg.Add(installCopyWorkers)
		for range installCopyWorkers {
			go m.installWorker(runCtx, store.DestModeLocalDir)
		}

		m.wg.Add(1)
		go m.eventPump(runCtx)
	}

	// Periodic Recover + upstream EMA persistence.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		recoverTick := time.NewTicker(m.recoverInterval)
		emaTick := time.NewTicker(10 * time.Second)
		defer recoverTick.Stop()
		defer emaTick.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-recoverTick.C:
				var rec store.RecoveryStats
				rerr := m.storeCall(runCtx, func() error {
					var err error
					rec, err = m.st.Recover(runCtx, m.nowFn())
					return err
				})
				if rerr == nil && rec != (store.RecoveryStats{}) {
					m.log.Info("periodic recovery requeued rows",
						"repos", rec.Repos, "files", rec.Files,
						"blocks", rec.Blocks, "job_files", rec.JobFiles)
				}
				wake(m.wakeMeta)
				wake(m.wakeDownload)
				wake(m.wakeDisk)
				wake(m.wakeInstall)
			case <-emaTick.C:
				m.persistUpstreamEMA(runCtx)
			}
		}
	}()

	// Drain loop: all jobs terminal (done|error).
	for {
		select {
		case <-ctx.Done():
			return finish(ctx.Err())
		default:
		}
		drained, err := m.drained(ctx)
		if err != nil {
			m.log.Warn("drain probe failed", "err", err)
		} else if drained {
			m.endJobSpans(ctx)
			m.persistUpstreamEMA(m.detachedCtxOr(ctx))
			return finish(m.joinedErrors())
		}
		m.endJobSpans(ctx)
		select {
		case <-ctx.Done():
			return finish(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// drained reports whether every job row is terminal. Fresh counts, not the
// TUI cache: drain must not lag behind the last completion.
func (m *Manager) drained(ctx context.Context) (bool, error) {
	var queued, running int64
	row := m.st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FILTER (WHERE status = 'queued'), COUNT(*) FILTER (WHERE status = 'running') FROM jobs")
	if err := row.Scan(&queued, &running); err != nil {
		return false, err
	}
	return queued == 0 && running == 0, nil
}

// endJobSpans closes sched.job spans for jobs that went terminal since the
// last sweep, attaching final totals.
func (m *Manager) endJobSpans(ctx context.Context) {
	m.spansMu.Lock()
	open := make(map[int64]trace.Span, len(m.spans))
	for id, sp := range m.spans {
		open[id] = sp
	}
	m.spansMu.Unlock()
	if len(open) == 0 {
		return
	}
	for id, sp := range open {
		var status string
		err := m.st.DB().QueryRowContext(ctx, "SELECT status FROM jobs WHERE id = ?", id).Scan(&status)
		if err != nil || (status != string(store.JobDone) && status != string(store.JobError)) {
			continue
		}
		done, total, _ := m.st.JobProgress(ctx, id)
		sp.SetAttributes(
			attribute.Int("hfdl.files_done", done),
			attribute.Int("hfdl.files_total", total),
			attribute.String("hfdl.job_status", status),
		)
		sp.End()
		m.spansMu.Lock()
		delete(m.spans, id)
		m.spansMu.Unlock()
	}
}

// fsToMedia maps the probed fs class onto throttle's media enum: sched
// owns the mapping so throttle never imports fcio.
func fsToMedia(fs fcio.FsType) throttle.MediaClass {
	switch fs {
	case fcio.FsSSD:
		return throttle.MediaSSD
	case fcio.FsHDD:
		return throttle.MediaHDD
	case fcio.FsNetFS:
		return throttle.MediaNetFS
	default:
		return throttle.MediaUnknown
	}
}
