package cache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// BlobConflictError reports a publication race where the already-published
// blob's size differs from the candidate's. Content-addressing makes same
// size ⇒ same blob; a size mismatch is real corruption, not a benign race.
type BlobConflictError struct {
	BlobID string
	Want   int64 // candidate .part size
	Have   int64 // existing blob size
}

func (e *BlobConflictError) Error() string {
	return fmt.Sprintf("cache: blob %s already published with different size (%d != %d)", e.BlobID, e.Have, e.Want)
}

// Publish atomically moves the verified staging file of fileID to
// blobs/<blobID>. link(2) is the portable atomic no-replace rename: the
// blob either appears whole at its final path or the call fails with
// EEXIST, which is tolerated — losing a publication race against identical
// content-addressed bytes is harmless.
func (s *Store) Publish(ctx context.Context, fileID int64, blobID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	src := s.IncompletePath(fileID)
	dst := s.BlobPath(blobID)
	if err := os.Link(src, dst); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("cache: publish %s → %s: %w", src, dst, err)
		}
		dstInfo, derr := os.Stat(dst)
		if derr != nil {
			return "", fmt.Errorf("cache: stat published blob %s: %w", dst, derr)
		}
		if srcInfo, serr := os.Stat(src); serr == nil && srcInfo.Size() != dstInfo.Size() {
			return "", &BlobConflictError{BlobID: blobID, Want: srcInfo.Size(), Have: dstInfo.Size()}
		}
		if err := os.Remove(src); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("cache: drop duplicate %s: %w", src, err)
		}
		return dst, nil
	}
	if err := os.Remove(src); err != nil {
		return "", fmt.Errorf("cache: unlink published %s: %w", src, err)
	}
	if err := s.fsyncDirFn(filepath.Dir(dst)); err != nil {
		return "", err
	}
	return dst, nil
}
