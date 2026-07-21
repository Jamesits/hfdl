package sched

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/store"
)

// QueueStat is one queue's TUI row.
type QueueStat struct {
	Depth, InFlight int
	Detail          string
}

// FileProgress is one active download's TUI row.
type FileProgress struct {
	FileID    int64
	Path      string
	Done      int64
	Total     int64
	Conns     int
	Rate      float64
	Upstreams []string
	Status    string
}

// CooldownInfo is one live 429/upstream gate with its remaining time.
type CooldownInfo struct {
	Target, Kind string
	Remaining    time.Duration
}

// Stats is the pinned snapshot consumed by the TUI (4Hz poll) and the OTel
// metric callbacks. Composed from store counts (250ms cache),
// the stats registry, the limiter Stats() and the store cooldown tables.
type Stats struct {
	Running, Paused, ENOSPCPaused bool

	Repo, Revision, CommitSHA, RepoStatus string

	BytesDone, BytesTotal int64
	FilesDone, FilesTotal int
	PendingCount          int
	PendingNext           []string
	GlobalRate            float64
	ETA                   time.Duration // <0 unknown

	Queues [4]QueueStat // 0=meta 1=download 2=disk 3=install
	Active []FileProgress

	Limits                         config.Limits
	BandwidthRate, APIRate         float64
	DutyLevel                      int
	DutyActiveRatio                float64
	DutyMedia                      string
	Cooldowns                      []CooldownInfo
	Stalls, Retries, SalvagedBytes int64
}

// queueTotals is the expensive DB half of the snapshot, refreshed with the
// counts cache.
type queueTotals struct {
	bytesTotal int64
	filesDone  int
	filesTotal int
	bytesDone  int64
	pending    []string
	repoStatus string
	commitSHA  string
}

// Snapshot composes the current Stats. Best-effort: a failing store read
// degrades to the last cached values, never an error — the TUI and OTel
// callbacks must not stall on a busy database.
func (m *Manager) Snapshot() *Stats {
	// Store reads ride the detached run ctx; before the first Run (or in
	// tests that never run) the snapshot carries in-memory data only.
	ctx := m.detachedCtx()
	s := &Stats{
		Limits:       m.currentLimits(),
		Paused:       m.pause.active(),
		ENOSPCPaused: m.enospc.active(),
		ETA:          -1,
	}
	m.runMu.Lock()
	s.Running = m.running
	m.runMu.Unlock()

	var counts map[string]int64
	var totals queueTotals
	if ctx != nil {
		counts = m.cachedCounts(ctx)
		totals = m.cachedTotals(ctx)
	}

	reg := m.cfg.Stats.Snapshot()
	s.GlobalRate = reg.GlobalRate
	s.Stalls = reg.Stalls
	s.Retries = reg.Retries
	s.SalvagedBytes = reg.TotalSalvaged

	s.BytesDone = totals.bytesDone
	s.BytesTotal = totals.bytesTotal
	s.FilesDone = totals.filesDone
	s.FilesTotal = totals.filesTotal
	s.PendingCount = int(counts["files.queued"])
	s.PendingNext = totals.pending

	m.primaryMu.RLock()
	if m.primary != nil {
		s.Repo = m.primary.repo
		s.Revision = m.primary.revision
	}
	m.primaryMu.RUnlock()
	s.CommitSHA = totals.commitSHA
	s.RepoStatus = totals.repoStatus

	// Queues: depths from the cached status counts.
	s.Queues[0] = QueueStat{
		Depth:    int(counts["repos.pending"]),
		InFlight: int(counts["repos.listing"]),
	}
	s.Queues[1] = QueueStat{
		Depth:    int(counts["blocks.pending"]),
		InFlight: int(counts["files.downloading"]),
		Detail:   fmt.Sprintf("%d conn", reg.Conns),
	}
	s.Queues[2] = QueueStat{
		Depth:    int(counts["files.downloaded"] + counts["files.salvaging"] + counts["reference_files.pending"]),
		InFlight: int(counts["files.verifying"] + counts["reference_files.hashing"]),
		Detail:   fmt.Sprintf("v%d s%d", counts["files.verifying"], counts["files.salvaging"]),
	}
	s.Queues[3] = QueueStat{
		Depth:    int(counts["job_files.pending"]),
		InFlight: int(counts["job_files.installing"]),
	}

	// Active downloads: registry per-file counters joined with the
	// orchestrator's active set for paths/status.
	m.activeMu.Lock()
	active := make([]FileProgress, 0, len(m.active))
	for fid, af := range m.active {
		fp := FileProgress{
			FileID: fid,
			Path:   af.file.Path,
			Total:  af.file.Size,
			Status: string(store.FileDownloading),
		}
		if fs, ok := reg.Files[fid]; ok {
			fp.Done = fs.Done
			fp.Conns = fs.Conns
			fp.Rate = fs.Rate
			fp.Upstreams = fs.Upstreams
		}
		active = append(active, fp)
	}
	m.activeMu.Unlock()
	sort.Slice(active, func(i, j int) bool { return active[i].Path < active[j].Path })
	s.Active = active

	// ETA: remaining bytes over the windowed global rate.
	if rem := s.BytesTotal - s.BytesDone; rem > 0 && s.GlobalRate > 0 {
		s.ETA = time.Duration(float64(rem)/s.GlobalRate) * time.Second
	}

	if m.cfg.Bandwidth != nil {
		s.BandwidthRate = m.cfg.Bandwidth.Stats().WindowedRate
	}
	if m.cfg.API != nil {
		s.APIRate = m.cfg.API.Stats().WindowedRate
	}
	if m.cfg.Duty != nil {
		ds := m.cfg.Duty.Stats()
		s.DutyLevel = ds.Level
		s.DutyActiveRatio = ds.ActiveRatio
		s.DutyMedia = ds.Media.String()
	}

	if ctx != nil {
		s.Cooldowns = m.cooldownInfos(ctx)
	}
	return s
}

// cooldownInfos merges endpoint (api/cas) and per-upstream gates with
// remaining time > 0.
func (m *Manager) cooldownInfos(ctx context.Context) []CooldownInfo {
	now := m.nowFn()
	var out []CooldownInfo
	if cds, err := m.st.Cooldowns(ctx); err == nil {
		for _, cd := range cds {
			if rem := cd.Until.Sub(now); rem > 0 {
				out = append(out, CooldownInfo{Target: cd.Endpoint, Kind: cd.Kind, Remaining: rem})
			}
		}
	}
	if ups, err := m.st.UpstreamState(ctx); err == nil {
		for _, u := range ups {
			if u.CooldownUntil != nil {
				if rem := u.CooldownUntil.Sub(now); rem > 0 {
					out = append(out, CooldownInfo{Target: u.Endpoint, Kind: "upstream", Remaining: rem})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Target != out[j].Target {
			return out[i].Target < out[j].Target
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

// cachedCounts returns store.Counts behind the 250ms cache.
func (m *Manager) cachedCounts(ctx context.Context) map[string]int64 {
	m.countsMu.Lock()
	defer m.countsMu.Unlock()
	if m.counts != nil && m.nowFn().Sub(m.countsAt) < countsCacheTTL {
		return m.counts
	}
	counts, err := m.st.Counts(ctx)
	if err != nil {
		if m.counts != nil {
			return m.counts
		}
		return map[string]int64{}
	}
	m.counts = counts
	m.countsAt = m.nowFn()
	return counts
}

// cachedTotals computes the DB-heavy totals behind the same cache window.
func (m *Manager) cachedTotals(ctx context.Context) queueTotals {
	m.countsMu.Lock()
	defer m.countsMu.Unlock()
	if m.nowFn().Sub(m.totalsAt) < countsCacheTTL && m.totalsAt.After(time.Time{}) {
		return m.totals
	}
	t := m.computeTotals(ctx)
	m.totals = t
	m.totalsAt = m.nowFn()
	return t
}

// computeTotals derives byte/file totals over the files referenced by
// non-terminal jobs. Read-only queries on the store handle (store.DB is
// documented for exactly this use).
func (m *Manager) computeTotals(ctx context.Context) queueTotals {
	var t queueTotals
	db := m.st.DB()

	// Totals cover every non-errored job (queued/running/done) so the
	// numbers stay meaningful both during and after a run.
	row := db.QueryRowContext(ctx,
		"SELECT COUNT(*), COALESCE(SUM(sz), 0) FROM ("+
			"SELECT DISTINCT jf.file_id, f.size AS sz FROM job_files jf "+
			"JOIN jobs j ON j.id = jf.job_id AND j.status IN ('queued','running','done') "+
			"JOIN files f ON f.id = jf.file_id)")
	var total int64
	if err := row.Scan(&total, &t.bytesTotal); err == nil {
		t.filesTotal = int(total)
	}

	row = db.QueryRowContext(ctx,
		"SELECT COUNT(*), COALESCE(SUM(sz), 0) FROM ("+
			"SELECT DISTINCT jf.file_id, f.size AS sz FROM job_files jf "+
			"JOIN jobs j ON j.id = jf.job_id AND j.status IN ('queued','running','done') "+
			"JOIN files f ON f.id = jf.file_id "+
			"WHERE f.status IN ('downloaded','verifying','cached'))")
	var done int64
	if err := row.Scan(&done, &t.bytesDone); err == nil {
		t.filesDone = int(done)
	}

	var pending []string
	if err := db.NewSelect().Model((*store.File)(nil)).
		Column("path").
		Where("status = ?", string(store.FileQueued)).
		Order("id").
		Limit(10).
		Scan(ctx, &pending); err == nil {
		t.pending = pending
	}

	m.primaryMu.RLock()
	repoID := int64(0)
	if m.primary != nil {
		repoID = m.primary.repoID
	}
	m.primaryMu.RUnlock()
	if repoID != 0 {
		var status, sha string
		if err := db.QueryRowContext(ctx,
			"SELECT status, COALESCE(commit_sha, '') FROM repos WHERE id = ?", repoID).
			Scan(&status, &sha); err == nil {
			t.repoStatus = status
			t.commitSHA = sha
		}
	}
	return t
}
