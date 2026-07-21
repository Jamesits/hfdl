package sched

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamesits/hfdl/pkg/cache"
	"github.com/jamesits/hfdl/pkg/store"
)

// Offline mode (HF_HUB_OFFLINE) is directory-driven like hf's
// try_to_load_from_cache: the cache LAYOUT is the ground truth, not the
// state DB. refs/<rev> pins the commit, snapshots/<sha>/<path> symlinks
// prove the blob and yield its id via the link target. A repo that can be
// neither listed nor served from the layout is a terminal error — offline
// never requeues metadata.

// layoutEntry is one file resolved from the offline cache layout.
type layoutEntry struct {
	path      string // repo path
	blobID    string // from the symlink target basename
	size      int64
	cachePath string // absolute blob path
}

// findInSnapshot resolves one repo path through
// <cacheRoot>/<type>s--org--name/snapshots/<sha>/<path>. Only symlink
// entries are servable: the link target carries the blob id (a regular
// file — a hardlink/copy fallback install — has lost it; hf can't map it
// back either).
func (m *Manager) findInSnapshot(repoType, repoName, sha, repoPath string) (*layoutEntry, error) {
	snap, err := cache.SafeJoin(filepath.Join(
		m.cfg.Cache.Root(), cache.ModelDirName(repoType, repoName), "snapshots", sha), repoPath)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(snap)
	if err != nil {
		return nil, err
	}
	if info.Mode()&fs.ModeSymlink == 0 {
		return nil, fmt.Errorf("sched: offline: %s is not a cache symlink", snap)
	}
	target, err := os.Readlink(snap)
	if err != nil {
		return nil, err
	}
	blobID := filepath.Base(target)
	blobPath := m.cfg.Cache.BlobPath(blobID)
	binfo, err := os.Stat(blobPath)
	if err != nil {
		return nil, fmt.Errorf("sched: offline: blob for %s: %w", repoPath, err)
	}
	return &layoutEntry{path: repoPath, blobID: blobID, size: binfo.Size(), cachePath: blobPath}, nil
}

// readRefsSHA reads the pinned commit of a repo from the cache layout
// (<cacheRoot>/<type>s--org--name/refs/<rev>).
func (m *Manager) readRefsSHA(repoType, repoName, rev string) (string, error) {
	refPath, err := cache.SafeJoin(filepath.Join(
		m.cfg.Cache.Root(), cache.ModelDirName(repoType, repoName), "refs"), rev)
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(refPath)
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(string(b))
	if sha == "" {
		return "", fmt.Errorf("sched: offline: empty refs/%s", rev)
	}
	return sha, nil
}

// walkSnapshot lists every file entry of a cached commit (offline snapshot
// mode with a fresh state DB: the snapshot dir IS the listing).
func (m *Manager) walkSnapshot(repoType, repoName, sha string) ([]layoutEntry, error) {
	root := filepath.Join(m.cfg.Cache.Root(), cache.ModelDirName(repoType, repoName), "snapshots", sha)
	var out []layoutEntry
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return nil
		}
		e, serr := m.findInSnapshot(repoType, repoName, sha, filepath.ToSlash(rel))
		if serr != nil {
			return nil // unservable entry: skip (regular file, dangling link)
		}
		out = append(out, *e)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("sched: offline: walk snapshot: %w", err)
	}
	return out, nil
}

// storeEntry converts a layout entry to a listing row. The blob id length
// distinguishes LFS sha256 (64 hex) from a git blob sha1 (40) — the same
// convention as the tree metadata.
func (e *layoutEntry) storeEntry() store.FileEntry {
	fe := store.FileEntry{Path: e.path, Size: e.size}
	switch len(e.blobID) {
	case 64:
		fe.IsLFS = true
		fe.SHA256 = e.blobID
	default:
		fe.GitOID = e.blobID
	}
	return fe
}

// markCachedOffline moves layout-served files to 'cached' without a hash
// pass (the blob was verified when it was first downloaded; hf trusts the
// cache the same way): queued → downloaded per file, then the standard
// verifying → cached claim ladder, which sets cache_path.
func (m *Manager) markCachedOffline(ctx context.Context, served map[int64]string) error {
	for fileID := range served {
		if err := m.storeCall(ctx, func() error {
			return m.st.TransitionFile(ctx, fileID, "", store.FileQueued, store.FileDownloaded, nil)
		}); err != nil {
			return fmt.Errorf("sched: offline: transition file %d: %w", fileID, err)
		}
	}
	for len(served) > 0 {
		f, tok, err := m.st.LeaseVerify(ctx, m.nowFn())
		if errors.Is(err, store.ErrNoWork) {
			return fmt.Errorf("sched: offline: %d served files not claimable for caching", len(served))
		}
		if err != nil {
			return err
		}
		cachePath, ok := served[f.ID]
		if !ok {
			// Not one of ours (leftover from another flow): put it back.
			if terr := m.st.TransitionFile(ctx, f.ID, tok, store.FileVerifying, store.FileDownloaded, nil); terr != nil {
				return terr
			}
			continue
		}
		if err := m.storeCall(ctx, func() error {
			return m.st.CompleteVerify(ctx, f.ID, tok, cachePath)
		}); err != nil {
			return fmt.Errorf("sched: offline: mark file %d cached: %w", f.ID, err)
		}
		delete(served, f.ID)
		m.log.Info("offline: served from cache layout", "file_id", f.ID, "path", f.Path)
	}
	return nil
}

// processRepoOffline lists a pending repo without any network: the cache
// layout is the listing (hf try_to_load_from_cache). Anything that cannot
// be served from disk is a TERMINAL error — offline never requeues
// metadata.
func (m *Manager) processRepoOffline(ctx context.Context, repo *store.Repo, tok store.LeaseToken, log *slog.Logger) {
	fail := func(err error) {
		if ferr := m.storeCall(ctx, func() error { return m.st.FailRepo(ctx, repo.ID, tok, err) }); ferr != nil {
			log.Warn("fail repo failed", "err", ferr)
		}
		m.recordError(err)
		log.Warn("offline: repo cannot be served", "err", err)
	}

	jobs, err := m.jobsForRepo(ctx, repo.ID)
	if err != nil {
		fail(err)
		return
	}

	// Collect explicit filenames; local-dir jobs serve from the dest dir.
	haveLocalDir := false
	filenamesSeen := map[string]bool{}
	var filenames []string
	for i := range jobs {
		sel := parseJobSelection(&jobs[i])
		if jobs[i].DestMode == store.DestModeLocalDir {
			haveLocalDir = true
		}
		for name := range sel.filenames {
			if !filenamesSeen[name] {
				filenamesSeen[name] = true
				filenames = append(filenames, name)
			}
		}
	}

	// Local-dir jobs have no snapshot layout: files already at the
	// destination are the servable set.
	if haveLocalDir {
		m.processRepoOfflineLocalDir(ctx, repo, tok, jobs, filenames, fail)
		return
	}

	sha, err := m.readRefsSHA(repo.Type, repo.Name, repo.Revision)
	if err != nil {
		fail(&OfflineError{Op: fmt.Sprintf("serve %s@%s: no cached revision (refs/%s missing)",
			repo.Name, repo.Revision, repo.Revision)})
		return
	}

	var entries []layoutEntry
	if len(filenames) > 0 {
		// Explicit files: each must be in the snapshot or the repo fails
		// terminally, naming the first missing path (hf parity).
		for _, name := range filenames {
			e, serr := m.findInSnapshot(repo.Type, repo.Name, sha, name)
			if serr != nil {
				fail(&OfflineError{Op: fmt.Sprintf("serve %s: not in offline cache", name)})
				return
			}
			entries = append(entries, *e)
		}
	} else {
		entries, err = m.walkSnapshot(repo.Type, repo.Name, sha)
		if err != nil {
			fail(&OfflineError{Op: fmt.Sprintf("serve %s@%s: %s", repo.Name, repo.Revision, err)})
			return
		}
		if len(entries) == 0 {
			fail(&OfflineError{Op: fmt.Sprintf("serve %s@%s: nothing in offline cache", repo.Name, repo.Revision)})
			return
		}
	}

	if err := m.storeCall(ctx, func() error { return m.st.SetCommitSHA(ctx, repo.ID, tok, sha) }); err != nil {
		if !errors.Is(err, store.ErrFenced) {
			log.Warn("set commit sha failed", "err", err)
		}
		return
	}
	repo.CommitSHA = sha

	storeEntries := make([]store.FileEntry, 0, len(entries))
	for i := range entries {
		storeEntries = append(storeEntries, entries[i].storeEntry())
	}
	if err := m.storeCall(ctx, func() error {
		return m.st.CompleteListing(ctx, repo.ID, tok, storeEntries)
	}); err != nil {
		if !errors.Is(err, store.ErrFenced) {
			log.Error("offline complete listing failed", "err", err)
		}
		return
	}
	m.repoMu.Lock()
	m.repoByID[repo.ID] = repo
	m.repoMu.Unlock()

	// Mark every synthesized file cached from its layout blob.
	files, err := m.filesForRepo(ctx, repo.ID)
	if err != nil {
		m.recordError(err)
		return
	}
	byPath := make(map[string]*store.File, len(files))
	for i := range files {
		byPath[files[i].Path] = &files[i]
	}
	served := make(map[int64]string, len(entries))
	for i := range entries {
		if f, ok := byPath[entries[i].path]; ok && f.Status == store.FileQueued {
			served[f.ID] = entries[i].cachePath
		}
	}
	if err := m.markCachedOffline(ctx, served); err != nil {
		m.recordError(err)
		log.Warn("offline mark cached failed", "err", err)
		return
	}
	log.Info("offline: repo served from cache layout", "commit_sha", sha, "files", len(entries))
	wake(m.wakeDisk)
	wake(m.wakeInstall)
}

// processRepoOfflineLocalDir serves local-dir jobs from files already at
// their destinations (no snapshot layout exists for local-dir).
func (m *Manager) processRepoOfflineLocalDir(ctx context.Context, repo *store.Repo, tok store.LeaseToken, jobs []store.Job, filenames []string, fail func(error)) {
	if len(filenames) == 0 {
		fail(&OfflineError{Op: fmt.Sprintf("list repo %s@%s in offline mode (local-dir snapshot listing needs the network)",
			repo.Name, repo.Revision)})
		return
	}
	// Every explicitly named file must exist at every local-dir job's
	// destination to be servable.
	type destHit struct {
		path      string
		size      int64
		cachePath string
	}
	hits := make(map[string]*destHit, len(filenames))
	for _, name := range filenames {
		for i := range jobs {
			if jobs[i].DestMode != store.DestModeLocalDir {
				continue
			}
			dest, err := cache.SafeJoin(jobs[i].DestDir, name)
			if err != nil {
				fail(err)
				return
			}
			info, err := os.Stat(dest)
			if err != nil {
				fail(&OfflineError{Op: fmt.Sprintf("serve %s: not in local dir %s", name, jobs[i].DestDir)})
				return
			}
			if hits[name] == nil {
				hits[name] = &destHit{path: name, size: info.Size(), cachePath: dest}
			}
		}
	}

	storeEntries := make([]store.FileEntry, 0, len(hits))
	for _, name := range filenames {
		if h := hits[name]; h != nil {
			storeEntries = append(storeEntries, store.FileEntry{Path: h.path, Size: h.size})
		}
	}
	if err := m.storeCall(ctx, func() error {
		return m.st.CompleteListing(ctx, repo.ID, tok, storeEntries)
	}); err != nil {
		if !errors.Is(err, store.ErrFenced) {
			m.log.Error("offline local-dir listing failed", "err", err)
		}
		return
	}
	files, err := m.filesForRepo(ctx, repo.ID)
	if err != nil {
		m.recordError(err)
		return
	}
	served := make(map[int64]string, len(hits))
	for i := range files {
		if h := hits[files[i].Path]; h != nil && files[i].Status == store.FileQueued {
			served[files[i].ID] = h.cachePath
		}
	}
	if err := m.markCachedOffline(ctx, served); err != nil {
		m.recordError(err)
		return
	}
	wake(m.wakeInstall)
}

// serveJobFileOffline serves one listed-but-uncached file in offline
// mode: from the destination for local-dir jobs, from the snapshot layout
// for cache-mode jobs. false = not servable from disk.
func (m *Manager) serveJobFileOffline(ctx context.Context, j *store.Job, f *store.File, r *store.Repo) (bool, error) {
	if j.DestMode == store.DestModeLocalDir {
		// A blob in the cache makes the file installable offline: mark it
		// cached against the blob and let the normal install copy out.
		if blobPath, ok := m.cfg.Cache.HasBlob(blobID(f)); ok {
			served := map[int64]string{f.ID: blobPath}
			if err := m.markCachedOffline(ctx, served); err != nil {
				return false, err
			}
			return true, nil
		}
		// Otherwise the file already at the destination is the serve.
		dest, err := cache.SafeJoin(j.DestDir, f.Path)
		if err != nil {
			return false, err
		}
		if _, err := os.Stat(dest); err != nil {
			return false, nil
		}
		served := map[int64]string{f.ID: dest}
		if err := m.markCachedOffline(ctx, served); err != nil {
			return false, err
		}
		return true, nil
	}
	return m.serveUncachedOffline(ctx, f, r)
}

// serveUncachedOffline tries the cache layout for one listed-but-uncached
// file (existing state DB, offline re-run): present → served, absent →
// false (caller errors it fast).
func (m *Manager) serveUncachedOffline(ctx context.Context, f *store.File, r *store.Repo) (bool, error) {
	e, err := m.findInSnapshot(r.Type, r.Name, r.CommitSHA, f.Path)
	if err != nil {
		return false, nil
	}
	if e.blobID != blobID(f) && blobID(f) != "" {
		// Layout disagrees with the listing: not servable.
		return false, nil
	}
	served := map[int64]string{f.ID: e.cachePath}
	if err := m.markCachedOffline(ctx, served); err != nil {
		return false, err
	}
	return true, nil
}
