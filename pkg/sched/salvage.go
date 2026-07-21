package sched

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamesits/hfdl/pkg/logging"
	"github.com/jamesits/hfdl/pkg/store"
)

// Reference handling: --reference roots/globs are expanded to
// stat rows at Submit time (no reads; the invalidation key is
// size+mtime_ns+dev+ino). Any path that resolves — after symlink resolution
// — into the hfdl cache or any active job destination is rejected: cache
// blobs are implicit references already, and incomplete/dest files may be
// mid-write. Rejections are logged and skipped, never errors.

// expandReferences expands a job's reference patterns to stat rows.
func (m *Manager) expandReferences(ctx context.Context, j Job) ([]store.ReferenceFile, error) {
	var roots []string
	for _, pattern := range j.References {
		if strings.ContainsAny(pattern, "*?[") {
			matches, err := filepath.Glob(pattern)
			if err != nil {
				return nil, fmt.Errorf("bad reference glob %q: %w", pattern, err)
			}
			roots = append(roots, matches...)
		} else {
			roots = append(roots, pattern)
		}
	}

	var out []store.ReferenceFile
	rejected := 0
	for _, root := range roots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		info, err := os.Stat(root)
		if err != nil {
			m.log.Warn("reference root not accessible, skipping", "path", root, "err", err)
			continue
		}
		if !info.IsDir() {
			if ref, ok := m.statReference(ctx, root, j); ok {
				out = append(out, ref)
			} else {
				rejected++
			}
			continue
		}
		werr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // unreadable subtree: skip, not fatal
			}
			if d.IsDir() {
				return nil
			}
			if !d.Type().IsRegular() {
				// Symlinks are followed explicitly below so the rejection
				// test sees the resolved target.
				if d.Type()&fs.ModeSymlink != 0 {
					if ref, ok := m.statReference(ctx, path, j); ok {
						out = append(out, ref)
					} else {
						rejected++
					}
				}
				return nil
			}
			if ref, ok := m.statReference(ctx, path, j); ok {
				out = append(out, ref)
			} else {
				rejected++
			}
			return nil
		})
		if werr != nil {
			m.log.Warn("reference walk failed, skipping root", "path", root, "err", werr)
		}
	}
	if rejected > 0 {
		m.log.Info("reference paths inside cache/destinations rejected", "count", rejected)
	}
	return out, nil
}

// statReference stats one candidate file (following symlinks) and reports
// whether it survives the cache/destination exclusion. The stored path is
// the symlink-resolved absolute path so dedupe and later opens see the real
// file.
func (m *Manager) statReference(ctx context.Context, path string, j Job) (store.ReferenceFile, bool) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return store.ReferenceFile{}, false
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return store.ReferenceFile{}, false
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() {
		return store.ReferenceFile{}, false
	}
	if m.pathForbidden(resolved, j) {
		m.log.Debug("reference path rejected (inside cache/destination)", "path", resolved)
		return store.ReferenceFile{}, false
	}
	dev, ino := devIno(info)
	return store.ReferenceFile{
		Path:    resolved,
		Size:    info.Size(),
		MtimeNs: info.ModTime().UnixNano(),
		Dev:     dev,
		Ino:     ino,
	}, true
}

// pathForbidden reports whether resolved lies inside the hfdl cache or any
// active job destination (the current job's own destination counts — a file
// must never salvage from its own possibly-partial output).
func (m *Manager) pathForbidden(resolved string, j Job) bool {
	if insideDir(resolved, m.cfg.Cache.Root()) {
		return true
	}
	// The submitting job's destination.
	if dest := jobDestRoot(j); dest != "" && insideDir(resolved, dest) {
		return true
	}
	// Other active jobs' destinations.
	for _, root := range m.activeDestRoots() {
		if insideDir(resolved, root) {
			return true
		}
	}
	return false
}

// jobDestRoot is the filesystem root a job writes into: the local-dir
// target, or the repo's HF-cache directory in cache mode.
func jobDestRoot(j Job) string {
	if j.DestDir == "" {
		return ""
	}
	abs, err := filepath.Abs(j.DestDir)
	if err != nil {
		return ""
	}
	if r, err := filepath.EvalSymlinks(abs); err == nil {
		abs = r
	}
	if j.DestMode == store.DestModeLocalDir {
		return abs
	}
	return filepath.Join(abs, repoCacheDirName(string(j.RepoType), j.Repo))
}

// activeDestRoots tracks destinations of all submitted jobs (registered at
// Submit; single process, in-memory is sufficient).
func (m *Manager) activeDestRoots() []string {
	m.destsMu.Lock()
	defer m.destsMu.Unlock()
	return append([]string(nil), m.dests...)
}

// insideDir reports whether path lies inside dir (both absolute, cleaned).
func insideDir(path, dir string) bool {
	dir = filepath.Clean(dir)
	path = filepath.Clean(path)
	if path == dir {
		return true
	}
	return strings.HasPrefix(path, dir+string(os.PathSeparator))
}

// findSalvageMatch returns a hashed reference matching the file's size and
// sha256, or nil. Whole-file salvage is the only granularity.
func (m *Manager) findSalvageMatch(ctx context.Context, f *store.File) (*store.ReferenceFile, error) {
	var refs []store.ReferenceFile
	if err := m.st.DB().NewSelect().Model(&refs).
		Where("status = ?", string(store.RefHashed)).
		Where("size = ?", f.Size).
		Where("sha256 = ?", f.SHA256).
		Order("id").
		Limit(1).
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("sched: salvage match file %d: %w", f.ID, err)
	}
	if len(refs) == 0 {
		return nil, nil
	}
	return &refs[0], nil
}

// pendingRefSizeMatch reports whether an unhashed reference exists whose
// size matches the file — the download orchestrator defers such files so
// the disk queue can hash first (salvage beats network). 'hashing' counts:
// the file must not start downloading while the hash that could divert it
// to salvage is still in flight.
func (m *Manager) pendingRefSizeMatch(ctx context.Context, size int64) (bool, error) {
	var exists bool
	err := m.st.DB().QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM reference_files WHERE status IN (?, ?) AND size = ?)",
		string(store.RefPending), string(store.RefHashing), size).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("sched: pending reference probe: %w", err)
	}
	return exists, nil
}

// logWouldDownload emits the --dry-run per-file lines (hf parity: one
// "Would download <path>" line per file, with the would-be destination).
func (m *Manager) logWouldDownload(j *store.Job, files []store.File) {
	paths := make([]string, 0, len(files))
	for i := range files {
		dest, err := m.destFor(j, nil, files[i].Path)
		if err != nil {
			dest = files[i].Path
		}
		paths = append(paths, dest)
		m.log.Info("Would download "+dest, "job_id", j.ID, "path", files[i].Path, "size", files[i].Size)
	}
	m.log.Info("dry-run listing complete",
		"job_id", j.ID, "files", len(paths),
		"paths", logging.JSONValue(paths))
}
