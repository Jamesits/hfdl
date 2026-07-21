package cache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PathSafetyError rejects a repo-supplied path before any filesystem join
// when it is absolute, contains .., or escapes the destination root via
// symlink.
type PathSafetyError struct {
	Path   string
	Reason string
}

func (e *PathSafetyError) Error() string {
	return fmt.Sprintf("cache: unsafe path %q: %s", e.Path, e.Reason)
}

// SafeJoin joins a repo-supplied relative path p onto root for a
// POINTER-INSTALL target, rejecting absolute paths, any ".." component, and
// ancestor-chain symlink escapes. The leaf itself is deliberately NOT resolved
// because huggingface_hub snapshot entries are relative symlinks into blobs/
// (a sibling of snapshots/) that hfdl creates by design; resolving the leaf
// would reject them by construction. Snapshot symlinks are (re)created via an
// atomic remove-then-symlink that never follows an existing leaf, so a hostile
// pre-placed leaf here cannot redirect a write. Content writers must use
// SafeJoinContent instead.
func SafeJoin(root, p string) (string, error) {
	return safeJoin(root, p, false)
}

// SafeJoinContent joins p onto root for a CONTENT-WRITE target (refs files,
// .metadata stamps, tree-cache JSON, local-dir output, lock files). In
// addition to the ancestor-chain check, the leaf is resolved too, so a
// pre-existing leaf symlink that escapes root is rejected — a hostile symlink
// planted at the write target can never redirect an O_TRUNC write outside the
// destination tree (symlink escape). A leaf symlink that stays inside root is permitted.
func SafeJoinContent(root, p string) (string, error) {
	return safeJoin(root, p, true)
}

// validateRepoPath applies the repo-supplied relative-path rules without
// touching the filesystem: non-empty, not absolute, no ".." component. Used
// both by safeJoin (before the symlink-containment check) and where a repo
// path is recorded rather than joined onto a root (tree-cache map keys).
func validateRepoPath(p string) error {
	if p == "" {
		return &PathSafetyError{Path: p, Reason: "empty path"}
	}
	if filepath.IsAbs(p) || strings.HasPrefix(p, "/") {
		return &PathSafetyError{Path: p, Reason: "absolute path"}
	}
	for _, comp := range strings.Split(filepath.ToSlash(p), "/") {
		if comp == ".." {
			return &PathSafetyError{Path: p, Reason: `contains ".." component`}
		}
	}
	return nil
}

// validateComponent rejects a repo-derived name that must be a single path
// component before it is joined onto a root: empty, ".", "..", absolute, or
// containing a path separator. Used for names that skip SafeJoin — the
// per-repo cache directory (ModelDirName output) and the blob-id lock file.
func validateComponent(name string) error {
	if name == "" || name == "." || name == ".." {
		return &PathSafetyError{Path: name, Reason: "empty or dot component"}
	}
	if filepath.IsAbs(name) || strings.ContainsRune(name, '/') || strings.ContainsRune(name, filepath.Separator) {
		return &PathSafetyError{Path: name, Reason: "not a single path component"}
	}
	return nil
}

func safeJoin(root, p string, resolveLeaf bool) (string, error) {
	if err := validateRepoPath(p); err != nil {
		return "", err
	}
	joined := filepath.Join(root, filepath.FromSlash(p))
	resolvedRoot, err := resolvePath(root)
	if err != nil {
		return "", fmt.Errorf("cache: resolve root %s: %w", root, err)
	}
	// Pointer install: check only the parent chain (the leaf may be a snapshot
	// symlink into blobs/). Content write: resolve the leaf too, so an existing
	// leaf symlink escaping root is caught by the containment check.
	probe := filepath.Dir(joined)
	if resolveLeaf {
		probe = joined
	}
	resolved, err := resolvePath(probe)
	if err != nil {
		return "", fmt.Errorf("cache: resolve %s: %w", probe, err)
	}
	if resolved != resolvedRoot &&
		!strings.HasPrefix(resolved, resolvedRoot+string(filepath.Separator)) {
		return "", &PathSafetyError{Path: p, Reason: "escapes root via symlink"}
	}
	return joined, nil
}

// resolvePath returns p with every symlink in its existing ancestor chain
// resolved, even when p itself does not exist yet: walk up to the deepest
// existing ancestor, EvalSymlinks it, then re-attach the missing tail.
func resolvePath(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	dir := abs
	var tail []string
	for {
		_, err := os.Lstat(dir)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("cache: stat %s: %w", dir, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("cache: no existing ancestor of %s", abs)
		}
		tail = append([]string{filepath.Base(dir)}, tail...)
		dir = parent
	}
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", fmt.Errorf("cache: resolve %s: %w", dir, err)
	}
	if len(tail) > 0 {
		parts := append([]string{resolved}, tail...)
		resolved = filepath.Join(parts...)
	}
	return resolved, nil
}
