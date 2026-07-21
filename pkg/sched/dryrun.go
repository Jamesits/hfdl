package sched

import "github.com/jamesits/hfdl/pkg/store"

// DryRunEntry is one file of the would-download set hf 1.24.0 prints in
// dry-run mode. cmd renders the summary line and the file/size table.
type DryRunEntry struct {
	Path     string // repo path
	DestPath string // would-be install target (job_files.dest_path)
	Size     int64  // raw bytes; cmd humanizes
	Cached   bool   // already fully in cache → hf prints "-" for size
}

// DryRunReport returns the would-download set of the most recently
// submitted job — exactly its selected files, once each — in repo listing
// order (files.id, tree order). The Cached flag follows hf's ground truth:
// the blob store, not the state DB — a fresh DB over a warm cache reports
// Cached=true when blobs/<blob_id> exists. Meaningful after a dry-run Run
// drains; empty before any job/run ctx exists.
func (m *Manager) DryRunReport() []DryRunEntry {
	ctx := m.detachedCtx()
	if ctx == nil {
		return nil
	}
	m.primaryMu.RLock()
	var jobID int64
	if m.primary != nil {
		jobID = m.primary.jobID
	}
	m.primaryMu.RUnlock()
	if jobID == 0 {
		return nil
	}
	// The job's own selection filters the union-assigned job_files rows
	// (CompleteListing pairs every listed file with every job of the repo;
	// the install queue applies the same per-job filter).
	var jobRow store.Job
	if err := m.st.DB().NewSelect().Model(&jobRow).Where("id = ?", jobID).Scan(ctx); err != nil {
		m.log.Warn("dry-run report job query failed", "err", err)
		return nil
	}
	sel := parseJobSelection(&jobRow)

	type row struct {
		Path     string
		DestPath string
		Size     int64
		Status   string
		IsLFS    bool
		SHA256   string
		GitOID   string
	}
	var rows []row
	sqlRows, err := m.st.DB().QueryContext(ctx,
		"SELECT f.path, COALESCE(jf.dest_path, ''), f.size, f.status, f.is_lfs, "+
			"COALESCE(f.sha256, ''), COALESCE(f.git_oid, '') "+
			"FROM job_files jf JOIN files f ON f.id = jf.file_id "+
			"WHERE jf.job_id = ? ORDER BY f.id", jobID)
	if err != nil {
		m.log.Warn("dry-run report query failed", "err", err)
		return nil
	}
	defer func() { _ = sqlRows.Close() }()
	for sqlRows.Next() {
		var r row
		if err := sqlRows.Scan(&r.Path, &r.DestPath, &r.Size, &r.Status, &r.IsLFS, &r.SHA256, &r.GitOID); err != nil {
			m.log.Warn("dry-run report scan failed", "err", err)
			return nil
		}
		if sel.matches(r.Path) {
			rows = append(rows, r)
		}
	}
	if err := sqlRows.Err(); err != nil {
		m.log.Warn("dry-run report rows failed", "err", err)
		return nil
	}

	out := make([]DryRunEntry, 0, len(rows))
	for _, r := range rows {
		cached := r.Status == "cached"
		if !cached {
			// hf checks the blob store, not the state DB.
			blob := r.GitOID
			if r.IsLFS {
				blob = r.SHA256
			}
			if blob != "" {
				_, cached = m.cfg.Cache.HasBlob(blob)
			}
		}
		out = append(out, DryRunEntry{
			Path:     r.Path,
			DestPath: r.DestPath,
			Size:     r.Size,
			Cached:   cached,
		})
	}
	return out
}
