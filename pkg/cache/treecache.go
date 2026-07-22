package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// TreeEntry is one file of a commit tree as known to the job. BlobID is the
// git blob sha1 (git_oid — for LFS files that is the pointer file's blob
// sha1); the lfs_* and xet_hash fields are set only when known.
type TreeEntry struct {
	Path      string // path inside the repo
	Size      int64
	BlobID    string // git blob sha1 (git_oid)
	LFSSHA256 string // LFS files only
	LFSSize   int64  // LFS files only
	XetHash   string // when the file is xet-backed
}

// treeCacheVersion is huggingface_hub's trees/<sha>.json format version.
const treeCacheVersion = 1

type treeFileJSON struct {
	Size      int64  `json:"size"`
	BlobID    string `json:"blob_id"`
	LFSSHA256 string `json:"lfs_sha256,omitempty"`
	LFSSize   int64  `json:"lfs_size,omitempty"`
	XetHash   string `json:"xet_hash,omitempty"`
}

type treeCacheJSON struct {
	FormatVersion int                     `json:"format_version"`
	Files         map[string]treeFileJSON `json:"files"`
}

// WriteTreeCache writes <dir>/.cache/huggingface/trees/<commitSHA>.json —
// huggingface_hub's commit tree cache for a local-dir job:
//
//	{"format_version": 1, "files": {"<path>": {"size": N, "blob_id": "...",
//	"lfs_sha256"/"lfs_size"/"xet_hash" when known}}}
//
// entries MUST be the complete commit listing (every file of the commit —
// hf's trees cache feeds try_to_load_from_cache, not just the downloaded
// set); sched passes the full FileEntry list at CompleteListing time. JSON
// is pretty-printed with indent=1 and no trailing newline, matching hf's
// json.dump(indent=1) bytes. Map keys marshal sorted, so the file is
// stable for a given entry set.
func (in *Installer) WriteTreeCache(ctx context.Context, dir, commitSHA string, entries []TreeEntry) error {
	return in.writeTreeCache(ctx, filepath.Join(dir, ".cache", "huggingface"), commitSHA, entries)
}

// WriteTreeCacheIfAbsent is the fallback variant: it writes only when no
// trees/<sha>.json exists yet, so a restart/offline path can backfill the
// cache from DB rows without clobbering the authoritative listing-time
// write. Safe in both orders because a commit sha pins its tree content.
func (in *Installer) WriteTreeCacheIfAbsent(ctx context.Context, dir, commitSHA string, entries []TreeEntry) (written bool, err error) {
	return in.writeTreeCacheIfAbsent(ctx, filepath.Join(dir, ".cache", "huggingface"), commitSHA, entries)
}

// WriteModelTreeCache is the cache-mode counterpart: hf writes the same
// commit tree cache into the model dir,
// <cacheDir>/<type>s--org--name/trees/<commitSHA>.json. sched calls it once
// per cache-mode job after that job's installs complete.
func (in *Installer) WriteModelTreeCache(ctx context.Context, cacheDir, repoType, repoName, commitSHA string, entries []TreeEntry) error {
	if err := validateModel(repoType, repoName); err != nil {
		return err
	}
	return in.writeTreeCache(ctx, filepath.Join(cacheDir, ModelDirName(repoType, repoName)), commitSHA, entries)
}

// WriteModelTreeCacheIfAbsent is the cache-mode fallback; see
// WriteTreeCacheIfAbsent.
func (in *Installer) WriteModelTreeCacheIfAbsent(ctx context.Context, cacheDir, repoType, repoName, commitSHA string, entries []TreeEntry) (written bool, err error) {
	if err := validateModel(repoType, repoName); err != nil {
		return false, err
	}
	return in.writeTreeCacheIfAbsent(ctx, filepath.Join(cacheDir, ModelDirName(repoType, repoName)), commitSHA, entries)
}

func validateModel(repoType, repoName string) error {
	if repoType != "" {
		if err := validateComponent(repoType); err != nil {
			return err
		}
	}
	return validateRepoPath(repoName)
}

// writeTreeCacheIfAbsent reports whether it wrote. The if-absent decision is
// TOCTOU-free: the fully-written temp is hard-linked into place. Link provides
// no-replace publication on POSIX and Windows (NTFS); EEXIST means another
// writer published first.
func (in *Installer) writeTreeCacheIfAbsent(ctx context.Context, parent, commitSHA string, entries []TreeEntry) (bool, error) {
	treePath, err := treeCachePath(parent, commitSHA)
	if err != nil {
		return false, err
	}
	treeDir := filepath.Dir(treePath)
	if err := mkdirAllSync(treeDir, 0o755, in.fsyncDirFn); err != nil {
		return false, fmt.Errorf("cache: create trees dir: %w", err)
	}
	data, err := marshalTreeCache(entries)
	if err != nil {
		return false, err
	}
	tmp, err := createTempFileSync(treeDir, data, 0o644)
	if err != nil {
		return false, err
	}
	defer func() { _ = os.Remove(tmp) }()
	if err := os.Link(tmp, treePath); err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("cache: publish %s: %w", treePath, err)
	}
	return true, in.fsyncDirFn(treeDir)
}

// treeCachePath resolves <parent>/trees/<commitSHA>.json safely.
func treeCachePath(parent, commitSHA string) (string, error) {
	if err := validateCommitSHA(commitSHA); err != nil {
		return "", fmt.Errorf("cache: invalid tree commit: %w", err)
	}
	return SafeJoinContent(filepath.Join(parent, "trees"), commitSHA+".json")
}

// writeTreeCache writes <parent>/trees/<commitSHA>.json in hf's
// format_version 1 shape.
func (in *Installer) writeTreeCache(ctx context.Context, parent, commitSHA string, entries []TreeEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCommitSHA(commitSHA); err != nil {
		return fmt.Errorf("cache: invalid tree commit: %w", err)
	}
	data, err := marshalTreeCache(entries)
	if err != nil {
		return err
	}
	treePath, err := treeCachePath(parent, commitSHA)
	if err != nil {
		return err
	}
	treeDir := filepath.Dir(treePath)
	if err := mkdirAllSync(treeDir, 0o755, in.fsyncDirFn); err != nil {
		return fmt.Errorf("cache: create trees dir: %w", err)
	}
	return atomicWriteFileSync(treePath, data, 0o644, in.fsyncDirFn)
}

func marshalTreeCache(entries []TreeEntry) ([]byte, error) {
	files := make(map[string]treeFileJSON, len(entries))
	for _, e := range entries {
		// Tree entries are repo-controlled; validate each path with the same
		// rules as a join target (non-empty, not absolute, no "..") before it
		// becomes a persisted map key.
		if err := validateRepoPath(e.Path); err != nil {
			return nil, err
		}
		// Repo paths are slash-separated in the JSON regardless of host OS.
		files[filepath.ToSlash(e.Path)] = treeFileJSON{
			Size:      e.Size,
			BlobID:    e.BlobID,
			LFSSHA256: e.LFSSHA256,
			LFSSize:   e.LFSSize,
			XetHash:   e.XetHash,
		}
	}
	// hf uses json.dump(indent=1): pretty-printed with a single-space
	// indent and no trailing newline. An Encoder with SetEscapeHTML(false)
	// also matches Python's literal (non-HTML-escaped) string output for
	// exotic file names; the Encoder's trailing newline is trimmed.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	if err := enc.Encode(treeCacheJSON{FormatVersion: treeCacheVersion, Files: files}); err != nil {
		return nil, fmt.Errorf("cache: marshal tree cache: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
