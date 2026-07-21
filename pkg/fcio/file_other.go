//go:build !linux

package fcio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// Plain buffered implementation of the engine API for windows/darwin:
// every file resolves to the plain tier, Fallocate degrades to
// ftruncate, sparse APIs are no-ops, ProbeFs reports FsUnknown.

// Open creates (or opens) path; size >= 0 preallocates logically via
// ftruncate. Windows has no fallocate equivalent — zero-fill preallocation
// is self-defeating and SetFileValidData needs admin and can expose stale
// disk contents — so grow-as-blocks-land is the designed default there.
func (e *Engine) Open(ctx context.Context, path string, size int64, h Hints) (*File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
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
		f.preallocated = true
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

// Fallocate degrades to ftruncate.
func (f *File) Fallocate(size int64) error {
	if err := f.f.Truncate(size); err != nil {
		return fmt.Errorf("fcio: fallocate %s: %w", f.path, err)
	}
	return nil
}

// DataExtents reports the whole file as one extent (no sparse walk off
// Linux).
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
