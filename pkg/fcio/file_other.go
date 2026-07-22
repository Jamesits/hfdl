//go:build !linux && !windows && !darwin

package fcio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var unsupportedHintsLog sync.Once

// Plain buffered fallback for genuinely-unknown platforms (Windows and macOS
// have real tiers in file_windows.go / file_darwin.go): every file resolves to
// the plain tier, there is no sparse attribute, and ProbeFs reports FsUnknown.

// Open creates (or opens) path; size >= 0 sets the logical size via ftruncate.
// There is no fallocate here, so preallocation is grow-as-blocks-land; the
// file is fully written by verify time, at which point the whole file reads as
// one data extent (see DataExtents) — the honest non-sparse case.
func (e *Engine) Open(ctx context.Context, path string, size int64, h Hints) (*File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if h.Sequential {
		unsupportedHintsLog.Do(func() { e.logDebug("access-pattern hints unsupported on this platform") })
	}
	bf, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, fmt.Errorf("fcio: open %s: %w", path, err)
	}
	f := &File{path: path, f: bf, size: size, tier: tierPlain, fsType: FsUnknown}
	if size >= 0 {
		if err := bf.Truncate(size); err != nil {
			f.Close()
			return nil, fmt.Errorf("fcio: truncate %s: %w", path, err)
		}
	}
	return f, nil
}

// WriteAt writes b's filled bytes at off (buffered).
func (f *File) WriteAt(b *Buf, off int64) error {
	p := b.Data()
	if len(p) == 0 {
		return nil
	}
	if err := writeFullAt(f.f, p, off); err != nil {
		return fmt.Errorf("fcio: write %s @%d: %w", f.path, off, err)
	}
	return nil
}

// WriteUnaligned writes a fragment at off (buffered; identical to WriteAt on
// the plain tier, kept as a separate entry point for API parity).
func (f *File) WriteUnaligned(p []byte, off int64) error {
	if len(p) == 0 {
		return nil
	}
	if err := writeFullAt(f.f, p, off); err != nil {
		return fmt.Errorf("fcio: write fragment %s @%d: %w", f.path, off, err)
	}
	return nil
}

// ReadAt fills b's filled length with file bytes at off.
func (f *File) ReadAt(b *Buf, off int64) error {
	p := b.Data()
	if len(p) == 0 {
		return nil
	}
	if err := readFullAt(f.f, p, off); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("fcio: read %s @%d: %w", f.path, off, io.ErrUnexpectedEOF)
		}
		return fmt.Errorf("fcio: read %s @%d: %w", f.path, off, err)
	}
	return nil
}

// DontNeed is a no-op on the plain tier.
func (f *File) DontNeed(off, length int64) {}

// Fallocate reports UNSUPPORTED honestly: there is no hole-to-allocated
// conversion on this platform, so the de-sparse caller must run the zero-fill
// walk. Faking success here (the former ftruncate-as-fallocate) silently
// violated the dense-blob invariant (P0).
func (f *File) Fallocate(size int64) error {
	return fmt.Errorf("fcio: fallocate %s: %w", f.path, ErrFallocateUnsupported)
}

// DataExtents reports the whole file as one extent. Without a SEEK_HOLE /
// QUERY_ALLOCATED_RANGES equivalent this is the only honest answer, and it is
// correct for the case that reaches here: no sparse attribute was ever set and
// the file is fully written by verify time, so it is genuinely non-sparse —
// exactly the "whole file is one data extent" the de-sparse walk treats as a
// no-op.
func (f *File) DataExtents() ([][2]int64, error) {
	fi, err := f.f.Stat()
	if err != nil {
		return nil, fmt.Errorf("fcio: stat %s: %w", f.path, err)
	}
	if fi.Size() == 0 {
		return nil, nil
	}
	return [][2]int64{{0, fi.Size()}}, nil
}

func (f *File) readChunk(p []byte, off int64, _ bool) (int, error) {
	if err := readFullAt(f.f, p, off); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	return len(p), nil
}

func (f *File) declareSequential() {}

// clearSparse is a no-op: no sparse attribute exists on the plain fallback.
func (f *File) clearSparse() error { return nil }
