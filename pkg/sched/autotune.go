package sched

import (
	"context"
	"sort"
	"time"

	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/tune"
)

const (
	// tuneTick is the controller sampling cadence.
	tuneTick = time.Second
	// blockPivotConns is the connection count the adaptive block size is
	// computed against. Chunking happens once at file start while the live
	// per-file connection count floats with the budget, so a fixed pivot
	// keeps block layout independent of the controller's momentary state.
	blockPivotConns = 8
)

// connBudget returns the live controller budget clamped to
// [1, Limits.MaxWorkers]. This is the rebalance target and may include an
// in-flight probe; admission must use committedBudget.
func (m *Manager) connBudget() int {
	limits := m.currentLimits()
	b := int(m.budget.Load())
	return min(max(b, 1), max(limits.MaxWorkers, 1))
}

// committedBudget returns the last accepted (non-probe) budget clamped to
// [1, Limits.MaxWorkers]. Admission keys off it so a speculative probe can
// never admit a file that a revert cannot evict.
func (m *Manager) committedBudget() int {
	limits := m.currentLimits()
	b := int(m.committed.Load())
	return min(max(b, 1), max(limits.MaxWorkers, 1))
}

// usableConns bounds a file's useful parallelism: one connection per
// remaining block (a worker with no block left to lease only idles), at
// least one (a live file always keeps a connection), at most lim.
// remaining is the scheduler's own counter — admission-time missing bytes
// minus durably completed blocks (noteBlockDone) — never the stats
// registry's wire-byte progress: wire bytes count in-flight and retried
// reads, which would open admission headroom while the blocks are still
// leased and their workers still hold connections, oversubscribing the
// global cap; and the registry's Total is the full file size, which would
// snap a resumed file's near-finished tail back to "needs everything".
// Caller holds activeMu.
func usableConns(af *activeFile, lim int) int {
	lim = max(lim, 1)
	if af.blockSize <= 0 {
		return lim
	}
	// Overflow-safe ceiling division: remaining+blockSize-1 could wrap for
	// huge remaining values and collapse the result to the floor of 1.
	blocks := af.remaining / af.blockSize
	if af.remaining%af.blockSize != 0 {
		blocks++
	}
	return int(min(max(blocks, 1), int64(lim)))
}

// spareConnsLocked returns how much of budget the active set cannot put to
// work (each file capped at its usable parallelism). Positive headroom is
// what admits the next file: deepen the files already downloading first,
// widen only with connections they cannot use. Caller holds activeMu.
func (m *Manager) spareConnsLocked(budget int) int {
	used := 0
	for _, af := range m.active {
		used += usableConns(af, budget)
	}
	return budget - used
}

// spareConns is the activeMu-taking wrapper around spareConnsLocked,
// evaluated against the committed budget (a speculative probe must not
// admit files a revert cannot evict).
func (m *Manager) spareConns() int {
	budget := m.committedBudget()
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	return m.spareConnsLocked(budget)
}

// noteBlockDone shrinks fileID's remaining-work counter after a block
// durably completes (blockLeaser.Complete). This is the single source of
// truth behind usableConns/spareConns.
func (m *Manager) noteBlockDone(fileID, n int64) {
	m.activeMu.Lock()
	if af, ok := m.active[fileID]; ok {
		af.remaining = max(af.remaining-n, 0)
	}
	m.activeMu.Unlock()
}

// admitFile atomically re-checks admission headroom and inserts the file
// into the active set, then grants its connection share by re-splitting the
// whole budget over the grown set (shrinking the existing files first).
// Returns the new file's share for the initial FileTask.Conns, or ok=false
// when the headroom vanished since the caller's pre-check (tune revert,
// SetLimits clamp, or a concurrent admission between the orchestrator's
// check and this insertion) or the file is already active. Check and insert
// share one allocMu critical section: a headroom re-check outside it could
// over-admit against a concurrently lowered budget.
func (m *Manager) admitFile(f *store.File, blockSize, remaining int64, cancel context.CancelFunc) (int, bool) {
	m.allocMu.Lock()
	defer m.allocMu.Unlock()
	budget := m.committedBudget()
	m.activeMu.Lock()
	if _, dup := m.active[f.ID]; dup || m.spareConnsLocked(budget) < 1 {
		m.activeMu.Unlock()
		return 0, false
	}
	m.admitSeq++
	m.active[f.ID] = &activeFile{
		file:      *f,
		seq:       m.admitSeq,
		blockSize: blockSize,
		remaining: remaining,
		cancel:    cancel,
	}
	m.activeMu.Unlock()
	m.rebalanceLocked(m.connBudget())
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	return m.active[f.ID].conns, true
}

// assignedConns sums the per-file connection assignments of the active set.
func (m *Manager) assignedConns() int {
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	total := 0
	for _, af := range m.active {
		total += af.conns
	}
	return total
}

// tuneLoop drives the adaptive connection controller: once per tick it
// samples the global rate, bandwidth utilization and stall kills, feeds the
// tune.Controller, publishes the resulting budget (admission reads it), and
// rebalances the budget across the active files.
func (m *Manager) tuneLoop(ctx context.Context) {
	defer m.wg.Done()
	ctrl := tune.New(tune.Config{})
	t := time.NewTicker(m.tuneEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if m.cfg.Downloader == nil || m.cfg.Stats == nil {
			continue
		}
		var util float64
		if m.cfg.Bandwidth != nil {
			util = m.cfg.Bandwidth.Stats().Utilization
		}
		reg := m.cfg.Stats.Snapshot()

		// One allocation critical section per tick: observe → publish →
		// rebalance. limits are read inside it so a concurrent SetLimits
		// clamp (also under allocMu) can never be overwritten by a stale
		// controller value.
		m.allocMu.Lock()
		limits := m.currentLimits()
		sample := tune.Sample{
			Rate:        reg.GlobalRate,
			Utilization: util,
			Assigned:    m.assignedConns(),
			Kills:       reg.Stalls,
		}
		budget := ctrl.Observe(m.nowFn(), max(limits.MaxWorkers, 1), sample)
		committed := ctrl.Committed()
		prev := m.budget.Swap(int64(budget))
		prevCommitted := m.committed.Swap(int64(committed))
		if int64(budget) != prev {
			m.log.Debug("connection budget changed", "budget", budget, "prev", prev,
				"committed", committed,
				"rate_bps", int64(sample.Rate), "utilization", util)
		}
		m.rebalanceLocked(budget)
		m.allocMu.Unlock()

		m.activeMu.Lock()
		activeCount := len(m.active)
		m.activeMu.Unlock()
		if committed > int(prevCommitted) || (activeCount > 0 && m.spareConns() > 0) {
			// Headroom for more files: nudge the orchestrator. Either the
			// committed budget rose (a probe deepens existing files only) or
			// the active set can no longer absorb it — files near completion
			// free connections mid-flight, so the next file can pipeline in
			// without waiting for the current one to fully finish.
			wake(m.wakeDownload)
		}
	}
}

// rebalance is the allocMu-taking wrapper around rebalanceLocked for callers
// outside the tune loop (tests).
func (m *Manager) rebalance(budget int) {
	m.allocMu.Lock()
	defer m.allocMu.Unlock()
	m.rebalanceLocked(budget)
}

// rebalanceLocked distributes budget connections across the active files,
// oldest admission first: the file already downloading is deepened up to its
// usable parallelism before any budget reaches a newer file, so the current
// file finishes fastest while newer files ramp up only with connections the
// older ones cannot use. Every live file keeps at least one connection even
// when the budget momentarily dips below the active count (a revert or a
// lowered MaxWorkers). Budget the whole set cannot absorb stays unassigned:
// that headroom is what admits the next file, and the controller's
// saturation check keeps it from probing on top of connections that cannot
// do work. Caller holds allocMu.
func (m *Manager) rebalanceLocked(budget int) {
	type target struct {
		fileID int64
		seq    uint64
		usable int
		conns  int
	}
	var files []target

	m.activeMu.Lock()
	for fid, af := range m.active {
		files = append(files, target{fileID: fid, seq: af.seq, usable: usableConns(af, budget)})
	}
	m.activeMu.Unlock()
	if len(files) == 0 {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].seq < files[j].seq })

	left := budget
	for i := range files {
		reserve := len(files) - i - 1 // every newer file keeps one connection
		files[i].conns = max(min(files[i].usable, left-reserve), 1)
		left -= files[i].conns
	}

	// Apply outside activeMu: SetParallelism takes downloader locks and may
	// spawn/drain workers; holding sched's map lock across it is unnecessary
	// (allocMu already serializes allocators). The call is deliberately
	// unconditional, not diffed against af.conns: an update sent before the
	// downloader registered the file is silently dropped, so re-asserting
	// the target every tick is what heals that divergence.
	for _, f := range files {
		m.activeMu.Lock()
		if af, ok := m.active[f.fileID]; ok {
			af.conns = f.conns
		}
		m.activeMu.Unlock()
		if m.setParallelism != nil {
			m.setParallelism(f.fileID, f.conns)
		}
	}
}
