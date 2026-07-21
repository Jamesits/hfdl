package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jamesits/hfdl/pkg/fcio"
)

// Store is the content-addressed cache rooted at an HF cache directory.
type Store struct {
	root string // absolute
	e    *fcio.Engine
	log  *slog.Logger

	// fsyncDirFn is the namespace-durability seam (tests count calls).
	fsyncDirFn func(dir string) error
}

// OpenStore creates (mkdir -p) the blob and incomplete-staging directories
// under root and returns the Store. root is made absolute so snapshot
// symlink targets can be computed relative to it.
func OpenStore(ctx context.Context, root string, e *fcio.Engine, log *slog.Logger) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("cache: resolve root %q: %w", root, err)
	}
	for _, dir := range []string{filepath.Join(abs, "blobs"), filepath.Join(abs, ".hfdl", "incomplete")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("cache: create %s: %w", dir, err)
		}
	}
	// huggingface_hub marks every cache root with a cache directory tag;
	// written only when absent, never rewritten (hf parity).
	tag := filepath.Join(abs, "CACHEDIR.TAG")
	if _, err := os.Lstat(tag); errors.Is(err, os.ErrNotExist) {
		if err := writeFileSync(tag, []byte(cachedirTagContent), 0o644); err != nil {
			return nil, err
		}
		if err := fsyncDir(abs); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, fmt.Errorf("cache: stat %s: %w", tag, err)
	}
	return &Store{root: abs, e: e, log: log, fsyncDirFn: fsyncDir}, nil
}

// Root returns the absolute cache root.
func (s *Store) Root() string { return s.root }

// IncompletePath is the staging path of an in-flight download.
func (s *Store) IncompletePath(fileID int64) string {
	return filepath.Join(s.root, ".hfdl", "incomplete", strconv.FormatInt(fileID, 10)+".part")
}

// BlobPath is the published path of a content-addressed blob.
func (s *Store) BlobPath(blobID string) string {
	return filepath.Join(s.root, "blobs", blobID)
}

// HasBlob reports whether blobID is published.
func (s *Store) HasBlob(blobID string) (path string, ok bool) {
	p := s.BlobPath(blobID)
	info, err := os.Stat(p)
	if err != nil || info.IsDir() {
		return "", false
	}
	return p, true
}

// createEmptyFile leaves an empty file at path — the shape of
// huggingface_hub's FileLock artifacts, which survive successful downloads.
func createEmptyFile(path string) error {
	l, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("cache: create %s: %w", path, err)
	}
	if err := l.Close(); err != nil {
		return fmt.Errorf("cache: close %s: %w", path, err)
	}
	return nil
}

// fsyncDir fsyncs a directory so a link/rename inside it survives a crash:
// namespace durability requires fsyncing the parent directory after the
// entry is created.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("cache: open dir %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("cache: fsync dir %s: %w", dir, err)
	}
	return nil
}

// syncFile fsyncs a single (already complete) file.
func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("cache: open %s: %w", path, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("cache: fsync %s: %w", path, err)
	}
	return nil
}

// writeFileSync writes data and fsyncs before returning, so a following
// parent-directory fsync makes the whole step durable.
func writeFileSync(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("cache: create %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close() // error path: the write error is the one that matters
		return fmt.Errorf("cache: write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("cache: fsync %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cache: close %s: %w", path, err)
	}
	return nil
}
