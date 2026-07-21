package sched

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/jamesits/hfdl/pkg/cache"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/store"
)

// treeEntriesFromListing maps the raw commit listing onto tree-cache
// entries (hf's trees/<sha>.json content: every file of the commit).
func treeEntriesFromListing(entries []hfapi.FileEntry) []cache.TreeEntry {
	out := make([]cache.TreeEntry, 0, len(entries))
	for _, e := range entries {
		te := cache.TreeEntry{
			Path:    e.Path,
			Size:    e.Size,
			BlobID:  e.GitOID,
			XetHash: e.XetHash,
		}
		if e.IsLFS {
			te.LFSSHA256 = e.SHA256
			te.LFSSize = e.Size
		}
		out = append(out, te)
	}
	return out
}

// treeEntriesFromFiles maps file rows onto tree-cache entries (fallback
// source when no full listing was persisted).
func treeEntriesFromFiles(files []store.File) []cache.TreeEntry {
	out := make([]cache.TreeEntry, 0, len(files))
	for i := range files {
		f := &files[i]
		te := cache.TreeEntry{
			Path:    f.Path,
			Size:    f.Size,
			BlobID:  f.GitOID,
			XetHash: f.XetHash,
		}
		if f.IsLFS {
			te.LFSSHA256 = f.SHA256
			te.LFSSize = f.Size
		}
		out = append(out, te)
	}
	return out
}

// treeCacheKVKey is the kv row holding the full commit listing (JSON of
// []cache.TreeEntry) so jobs submitted after the listing can still stamp
// the complete tree cache (the files table only holds the union-filtered
// download set).
func treeCacheKVKey(repoID int64) string { return fmt.Sprintf("treecache:%d", repoID) }

// writeTreeCaches stamps the commit tree cache for every job destination
// at listing time: cache-mode model dirs and local-dirs. The full listing
// is also persisted to kv for the completion-time fallback. Failures are
// logged, never listing-fatal.
func (m *Manager) writeTreeCaches(ctx context.Context, jobs []store.Job, r *store.Repo, entries []cache.TreeEntry, log *slog.Logger) {
	if len(entries) == 0 || r.CommitSHA == "" {
		return
	}
	if blob, err := json.Marshal(entries); err == nil {
		if serr := m.st.SetKV(ctx, treeCacheKVKey(r.ID), string(blob)); serr != nil {
			log.Warn("persist tree listing failed", "err", serr)
		}
	}
	for i := range jobs {
		j := &jobs[i]
		var err error
		if j.DestMode == store.DestModeLocalDir {
			err = m.cfg.Installer.WriteTreeCache(ctx, j.DestDir, r.CommitSHA, entries)
		} else {
			err = m.cfg.Installer.WriteModelTreeCache(ctx, j.DestDir, r.Type, r.Name, r.CommitSHA, entries)
		}
		if err != nil {
			log.Warn("tree cache write failed", "job_id", j.ID, "mode", j.DestMode, "err", err)
		}
	}
}

// maybeWriteTreeCache is the completion-time tree-cache fallback: the
// listing-time write carries the COMPLETE commit listing and wins; this
// IfAbsent call only backfills restart/offline runs where no listing
// happened (best source then is the repo's file rows). A stamp failure is
// logged, never job-fatal: the payload is already installed.
func (m *Manager) maybeWriteTreeCache(ctx context.Context, j *store.Job, r *store.Repo) {
	done, total, err := m.st.JobProgress(ctx, j.ID)
	if err != nil || done != total || total == 0 {
		return
	}
	// Prefer the persisted full commit listing; the union-filtered file
	// rows are the degraded source when it is absent.
	var entries []cache.TreeEntry
	if raw, kerr := m.st.GetKV(ctx, treeCacheKVKey(j.RepoID)); kerr == nil && raw != "" {
		_ = json.Unmarshal([]byte(raw), &entries)
	}
	if len(entries) == 0 {
		files, ferr := m.filesForRepo(ctx, j.RepoID)
		if ferr != nil {
			m.log.Warn("tree cache: list repo files failed", "job_id", j.ID, "err", ferr)
			return
		}
		entries = treeEntriesFromFiles(files)
	}
	for {
		var err error
		var written bool
		if j.DestMode == store.DestModeLocalDir {
			written, err = m.cfg.Installer.WriteTreeCacheIfAbsent(ctx, j.DestDir, r.CommitSHA, entries)
		} else {
			// Cache mode: j.DestDir is the HF cache root.
			written, err = m.cfg.Installer.WriteModelTreeCacheIfAbsent(ctx, j.DestDir, r.Type, r.Name, r.CommitSHA, entries)
		}
		if err == nil {
			if written {
				m.log.Debug("tree cache written (fallback)", "job_id", j.ID, "dir", j.DestDir, "mode", j.DestMode, "files", len(entries))
			}
			return
		}
		if !m.noteIOError(ctx, err) {
			m.log.Warn("tree cache write failed", "job_id", j.ID, "err", err)
			return
		}
		if werr := m.waitResumable(ctx); werr != nil {
			return
		}
	}
}

// installWorker runs the install queue: lease one pending job_files
// row whose file is cached, materialize it (cache-mode symlink, or
// local-dir reflink/copy), complete the lease. The cheap pool is metadata
// ops (symlinks); copies are serialized per (src,dst) volume pair by the
// installer's own VolumeSet locking.
func (m *Manager) installWorker(ctx context.Context) {
	defer m.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := m.waitResumable(ctx); err != nil {
			return
		}
		jf, f, j, r, tok, err := m.st.LeaseInstall(ctx, m.nowFn())
		if errors.Is(err, store.ErrNoWork) {
			if !m.waitForWork(ctx, m.wakeInstall) {
				return
			}
			continue
		}
		if err != nil {
			m.log.Error("lease install failed", "err", err)
			if !m.waitForWork(ctx, m.wakeInstall) {
				return
			}
			continue
		}
		m.processInstall(ctx, jf, f, j, r, tok)
	}
}

// processInstall runs one leased job_files row.
func (m *Manager) processInstall(ctx context.Context, jf *store.JobFile, f *store.File, j *store.Job, r *store.Repo, tok store.LeaseToken) {
	// A repo's listing is shared by all its jobs (union filter); the
	// per-job selection decides what this job actually installs.
	if !parseJobSelection(j).matches(f.Path) {
		if err := m.storeCall(ctx, func() error { return m.st.CompleteInstall(ctx, j.ID, f.ID, tok, nil) }); err != nil {
			m.log.Warn("complete skipped install failed", "job_id", j.ID, "file_id", f.ID, "err", err)
		}
		return
	}

	// Offline local-dir without a blob in the cache: the file at the
	// destination IS the serve. A present blob skips this branch and runs
	// the normal install — copying out of the cache is a local,
	// offline-safe operation.
	if m.cfg.Offline && j.DestMode == store.DestModeLocalDir {
		_, hasBlob := m.cfg.Cache.HasBlob(blobID(f))
		if !hasBlob {
			dest, derr := cache.SafeJoin(j.DestDir, f.Path)
			if derr == nil {
				if _, serr := os.Stat(dest); serr == nil {
					if cerr := m.storeCall(ctx, func() error { return m.st.CompleteInstall(ctx, j.ID, f.ID, tok, nil) }); cerr != nil {
						m.log.Warn("complete offline local-dir install failed", "job_id", j.ID, "file_id", f.ID, "err", cerr)
					}
					m.maybeWriteTreeCache(ctx, j, r)
					return
				}
			}
			err := &OfflineError{Op: fmt.Sprintf("serve %s: not in local dir %s", f.Path, j.DestDir)}
			m.log.Error("offline install failed", "job_id", j.ID, "path", f.Path, "err", err)
			if cerr := m.storeCall(ctx, func() error { return m.st.CompleteInstall(ctx, j.ID, f.ID, tok, err) }); cerr != nil {
				m.log.Warn("complete failed offline install failed", "job_id", j.ID, "file_id", f.ID, "err", cerr)
			}
			if ferr := m.storeCall(ctx, func() error { return m.st.FinishJob(ctx, j.ID, err) }); ferr != nil {
				m.log.Debug("finish job after offline install failure", "job_id", j.ID, "err", ferr)
			}
			m.recordError(err)
			return
		}
	}

	req := cache.InstallRequest{
		DestMode:  j.DestMode,
		DestDir:   j.DestDir,
		CacheDir:  j.DestDir, // cache mode: DestDir IS the HF cache root
		RepoType:  r.Type,
		RepoName:  r.Name,
		Revision:  r.Revision,
		CommitSHA: r.CommitSHA,
		RepoPath:  f.Path,
		BlobID:    blobID(f),
		Size:      f.Size,
	}

	// ENOSPC retries after the global pause lifts; every other failure is
	// terminal for the row.
	for {
		path, err := m.cfg.Installer.Install(ctx, req)
		if err == nil {
			if jf.DestPath != "" && jf.DestPath != path {
				m.log.Debug("installer path differs from resolved dest_path",
					"job_id", j.ID, "file_id", f.ID, "resolved", jf.DestPath, "installed", path)
			}
			if cerr := m.storeCall(ctx, func() error { return m.st.CompleteInstall(ctx, j.ID, f.ID, tok, nil) }); cerr != nil {
				m.log.Warn("complete install failed", "job_id", j.ID, "file_id", f.ID, "err", cerr)
			}
			m.log.Debug("installed", "job_id", j.ID, "path", f.Path, "dest", path)
			m.maybeWriteTreeCache(ctx, j, r)
			return
		}
		if m.noteIOError(ctx, err) {
			if werr := m.waitResumable(ctx); werr != nil {
				return // shutdown: Recover requeues the row
			}
			continue
		}
		m.log.Error("install failed", "job_id", j.ID, "path", f.Path, "err", err)
		if cerr := m.storeCall(ctx, func() error { return m.st.CompleteInstall(ctx, j.ID, f.ID, tok, err) }); cerr != nil {
			m.log.Warn("complete failed install failed", "job_id", j.ID, "file_id", f.ID, "err", cerr)
		}
		// hf parity: one failed file fails the job.
		if ferr := m.storeCall(ctx, func() error { return m.st.FinishJob(ctx, j.ID, err) }); ferr != nil {
			m.log.Debug("finish job after install failure", "job_id", j.ID, "err", ferr)
		}
		m.recordError(fmt.Errorf("sched: install %s: %w", f.Path, err))
		return
	}
}
