package sched

import (
	"testing"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/transfer"
)

// budgetManager builds a bare Manager with a fake active set, enough for the
// pure budget/rebalance arithmetic (SetParallelism on unknown file IDs is a
// no-op in transfer). Fake files are 64KiB-blocked with (i+1)MiB remaining,
// so each can absorb at least 16 connections; seq follows insertion order.
func budgetManager(t *testing.T, maxWorkers int, activeConns ...int) *Manager {
	t.Helper()
	m := NewManager(ManagerConfig{
		Downloader: transfer.NewDownloader(transfer.Config{}),
		Limits:     config.Limits{MaxWorkers: maxWorkers, APIIOPS: 1, DiskActivePct: 100},
	})
	for i, c := range activeConns {
		fid := int64(i + 1)
		m.admitSeq++
		m.active[fid] = &activeFile{
			file:      store.File{ID: fid, Size: int64(i+1) << 20},
			seq:       m.admitSeq,
			blockSize: 64 << 10,
			remaining: int64(i+1) << 20,
			conns:     c,
		}
	}
	return m
}

func TestConnBudgetStartsAtOneAndClamps(t *testing.T) {
	m := budgetManager(t, 16)
	if got := m.connBudget(); got != 1 {
		t.Fatalf("initial budget = %d, want 1 (slow start)", got)
	}
	m.budget.Store(64)
	if got := m.connBudget(); got != 16 {
		t.Fatalf("budget = %d, want clamp to MaxWorkers=16", got)
	}
	m.budget.Store(0)
	if got := m.connBudget(); got != 1 {
		t.Fatalf("budget = %d, want floor 1", got)
	}
}

func TestSetLimitsClampsBudget(t *testing.T) {
	m := budgetManager(t, 16)
	m.budget.Store(16)
	m.committed.Store(16)
	l := m.currentLimits()
	l.MaxWorkers = 4
	m.SetLimits(l)
	if got := m.budget.Load(); got != 4 {
		t.Fatalf("budget after lowering MaxWorkers = %d, want 4", got)
	}
	if got := m.committed.Load(); got != 4 {
		t.Fatalf("committed after lowering MaxWorkers = %d, want 4", got)
	}
	if got := m.connBudget(); got != 4 {
		t.Fatalf("connBudget = %d, want 4", got)
	}
	if got := m.committedBudget(); got != 4 {
		t.Fatalf("committedBudget = %d, want 4", got)
	}
}

// TestAdmitFileGrantsFullBudget: the first admitted file receives the
// entire live budget (its usable parallelism permitting) so it finishes as
// fast as possible.
func TestAdmitFileGrantsFullBudget(t *testing.T) {
	m := budgetManager(t, 16)
	m.budget.Store(8)
	m.committed.Store(8)
	got, ok := m.admitFile(&store.File{ID: 1, Size: 4 << 20}, 64<<10, 4<<20, func() {})
	if !ok {
		t.Fatal("admission denied into an empty active set")
	}
	if got != 8 || m.active[1].conns != 8 {
		t.Fatalf("admitFile = %d, active share = %d, want the full budget 8",
			got, m.active[1].conns)
	}
}

// TestAdmitFileDeepensBeforeWidening: admitFile denies a second file while
// the active one can absorb the whole committed budget (deepen first), a
// speculative probe (budget > committed) never opens admission, duplicates
// are denied, and admission opens exactly when the active file's remaining
// blocks no longer cover the budget — the tail frees connections for the
// next file before the current one finishes.
func TestAdmitFileDeepensBeforeWidening(t *testing.T) {
	m := budgetManager(t, 16, 1) // one active file: 1MiB remaining, 64KiB blocks (usable 16)
	if _, ok := m.admitFile(&store.File{ID: 2, Size: 1 << 20}, 64<<10, 1<<20, func() {}); ok {
		t.Fatal("admission allowed above the committed budget")
	}
	m.budget.Store(4) // in-flight probe: rebalance target up, committed unchanged
	if _, ok := m.admitFile(&store.File{ID: 2, Size: 1 << 20}, 64<<10, 1<<20, func() {}); ok {
		t.Fatal("a speculative probe opened admission")
	}
	m.committed.Store(16)
	m.budget.Store(16)
	if _, ok := m.admitFile(&store.File{ID: 2, Size: 1 << 20}, 64<<10, 1<<20, func() {}); ok {
		t.Fatal("second file admitted while the first absorbs the whole budget")
	}
	if _, ok := m.admitFile(&store.File{ID: 1, Size: 1 << 20}, 64<<10, 1<<20, func() {}); ok {
		t.Fatal("duplicate admission allowed")
	}
	// The first file is nearly done: 2 blocks left → usable 2, spare 14.
	m.activeMu.Lock()
	m.active[1].remaining = 128 << 10
	m.activeMu.Unlock()
	conns, ok := m.admitFile(&store.File{ID: 2, Size: 8 << 20}, 64<<10, 8<<20, func() {})
	if !ok {
		t.Fatal("admission denied despite spare budget at the first file's tail")
	}
	if m.active[1].conns != 2 {
		t.Fatalf("older file has %d conns, want 2 (its remaining blocks)", m.active[1].conns)
	}
	if conns != 14 {
		t.Fatalf("new file granted %d conns, want the spare 14", conns)
	}
}

// TestSpareConns: headroom is the committed budget minus what the active
// set can absorb, and admission of new files keys off the committed budget
// even while a probe raises the live one.
func TestSpareConns(t *testing.T) {
	m := budgetManager(t, 16)
	m.committed.Store(8)
	if got := m.spareConns(); got != 8 {
		t.Fatalf("spare with empty active set = %d, want the whole budget 8", got)
	}
	m.admitSeq++
	m.active[1] = &activeFile{
		file: store.File{ID: 1, Size: 1 << 20}, seq: m.admitSeq,
		blockSize: 64 << 10, remaining: 3 * (64 << 10), // 3 blocks left
	}
	if got := m.spareConns(); got != 5 {
		t.Fatalf("spare = %d, want 8-3=5", got)
	}
	m.budget.Store(16) // probe must not widen admission headroom
	if got := m.spareConns(); got != 5 {
		t.Fatalf("spare during probe = %d, want committed-based 5", got)
	}
}

// TestNoteBlockDoneOpensHeadroom: only durable block completion shrinks a
// file's remaining-work counter and opens admission headroom — never
// wire-byte progress, which counts in-flight and retried reads whose
// workers still hold connections (using it would oversubscribe the global
// connection cap and, for resumed files, the registry's full-size Total
// would inflate remaining back to the whole file).
func TestNoteBlockDoneOpensHeadroom(t *testing.T) {
	m := budgetManager(t, 16)
	m.committed.Store(8)
	const blockSize = 64 << 10
	m.admitSeq++
	m.active[1] = &activeFile{
		file: store.File{ID: 1, Size: 1 << 30}, seq: m.admitSeq,
		blockSize: blockSize, remaining: 8 * blockSize, // resumed: 8 blocks left of a 1GiB file
	}
	if got := m.spareConns(); got != 0 {
		t.Fatalf("spare = %d, want 0 while all 8 blocks are outstanding", got)
	}
	m.noteBlockDone(1, 3*blockSize)
	if got := m.spareConns(); got != 3 {
		t.Fatalf("spare after 3 completed blocks = %d, want 3", got)
	}
	m.noteBlockDone(1, 100*blockSize) // over-completion clamps at zero
	if got := m.spareConns(); got != 7 {
		t.Fatalf("spare after full completion = %d, want 7 (file keeps 1)", got)
	}
	m.noteBlockDone(2, blockSize) // unknown file: no-op, no panic
}

// TestRebalancePublishesToDownloader: every rebalance re-asserts the final
// per-file share to the downloader unconditionally (self-healing for updates
// dropped before file registration), and the published values match the
// scheduler's own active-set accounting.
func TestRebalancePublishesToDownloader(t *testing.T) {
	m := budgetManager(t, 16, 1, 1, 1)
	published := make(map[int64]int)
	m.setParallelism = func(fileID int64, n int) { published[fileID] = n }

	m.rebalance(8)
	if len(published) != 3 {
		t.Fatalf("published to %d files, want all 3", len(published))
	}
	for fid, af := range m.active {
		if published[fid] != af.conns {
			t.Fatalf("file %d: published %d, active set says %d", fid, published[fid], af.conns)
		}
	}

	// A no-op rebalance (same budget) still re-asserts every share.
	published = make(map[int64]int)
	m.rebalance(8)
	if len(published) != 3 {
		t.Fatalf("re-assert published to %d files, want all 3", len(published))
	}
}

// TestRebalanceInvariants: budget flows to the oldest admitted file first
// (deepen before widen) with one connection reserved per newer file, every
// file keeps at least one connection even when the budget dips below the
// active count, a file never gets more connections than remaining blocks,
// and budget the set cannot absorb stays unassigned.
func TestRebalanceInvariants(t *testing.T) {
	m := budgetManager(t, 16, 1, 1, 1)
	m.rebalance(8)
	total := 0
	for _, af := range m.active {
		if af.conns < 1 {
			t.Fatalf("file got %d conns, want >= 1", af.conns)
		}
		total += af.conns
	}
	if total != 8 {
		t.Fatalf("assigned total = %d, want budget 8", total)
	}
	// Oldest first: file 1 absorbs everything but one reserved connection
	// per newer file.
	if m.active[1].conns != 6 || m.active[2].conns != 1 || m.active[3].conns != 1 {
		t.Fatalf("split = %d/%d/%d, want 6/1/1 oldest-first",
			m.active[1].conns, m.active[2].conns, m.active[3].conns)
	}

	// Usable cap: the oldest file has only 2 blocks left, so the surplus
	// cascades to the next-oldest instead.
	m.active[1].remaining = 128 << 10
	m.rebalance(8)
	if m.active[1].conns != 2 || m.active[2].conns != 5 || m.active[3].conns != 1 {
		t.Fatalf("split = %d/%d/%d, want 2/5/1 after the oldest hit its block cap",
			m.active[1].conns, m.active[2].conns, m.active[3].conns)
	}

	// All files near completion: unabsorbable budget stays unassigned so
	// admission (not deeper splits) consumes it.
	m.active[2].remaining = 64 << 10
	m.active[3].remaining = 64 << 10
	m.rebalance(8)
	if m.active[1].conns != 2 || m.active[2].conns != 1 || m.active[3].conns != 1 {
		t.Fatalf("split = %d/%d/%d, want 2/1/1 with the rest unassigned",
			m.active[1].conns, m.active[2].conns, m.active[3].conns)
	}

	// Budget below the active count: everyone keeps 1 (drain-by-attrition).
	m.rebalance(2)
	for fid, af := range m.active {
		if af.conns != 1 {
			t.Fatalf("file %d = %d conns after under-budget rebalance, want 1", fid, af.conns)
		}
	}
}
