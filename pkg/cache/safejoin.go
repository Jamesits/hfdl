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

// SafeJoin joins a repo-supplied relative path p onto root, rejecting
// absolute paths, any ".." component, and symlink escapes. The escape check
// resolves the deepest existing ancestor of the target's *parent* and
// verifies containment inside the resolved root; the leaf itself is never
// resolved because snapshot entries are symlinks into blobs/ by design
// (huggingface_hub layout) and would fail containment by construction.
func SafeJoin(root, p string) (string, error) {
	if p == "" {
		return "", &PathSafetyError{Path: p, Reason: "empty path"}
	}
	if filepath.IsAbs(p) || strings.HasPrefix(p, "/") {
		return "", &PathSafetyError{Path: p, Reason: "absolute path"}
	}
	for _, comp := range strings.Split(filepath.ToSlash(p), "/") {
		if comp == ".." {
			return "", &PathSafetyError{Path: p, Reason: `contains ".." component`}
		}
	}
	joined := filepath.Join(root, filepath.FromSlash(p))
	resolvedRoot, err := resolvePath(root)
	if err != nil {
		return "", fmt.Errorf("cache: resolve root %s: %w", root, err)
	}
	resolvedParent, err := resolvePath(filepath.Dir(joined))
	if err != nil {
		return "", fmt.Errorf("cache: resolve parent of %s: %w", joined, err)
	}
	if resolvedParent != resolvedRoot &&
		!strings.HasPrefix(resolvedParent, resolvedRoot+string(filepath.Separator)) {
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
