//go:build darwin

package fcio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// Open creates (or opens) path. The direct/uncached tier is fcntl(F_NOCACHE):
// unlike O_DIRECT it imposes no alignment requirement, so there is no separate
// aligned fd and IO routes through the single fd. Sequential intent is
// fcntl(F_RDAHEAD). macOS has no fallocate; size >= 0 attempts an
// F_PREALLOCATE+ftruncate reservation (Tier A) and degrades to a plain
// ftruncate (Tier B — any residual holes are filled by the SEEK_HOLE de-sparse
// walk at verify time).
func (e *Engine) Open(ctx context.Context, path string, size int64, h Hints) (*File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bf, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, fmt.Errorf("fcio: open %s: %w", path, err)
	}
	f := &File{path: path, f: bf, size: size, tier: tierPlain, fsType: FsUnknown}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
		}
	}()
	if size >= 0 {
		if err := preallocate(bf, size); err != nil {
			e.logDebug("F_PREALLOCATE unsupported; storage tier B (no prealloc)", "path", path, "err", err)
			if terr := bf.Truncate(size); terr != nil {
				return nil, fmt.Errorf("fcio: truncate %s: %w", path, terr)
			}
		} else {
			f.preallocated = true
		}
	}
	if e.mode == TierDirect {
		// F_NOCACHE gives uncached IO with no alignment constraint, so the file
		// stays on the plain read/write path (no separate aligned fd).
		if _, err := unix.FcntlInt(bf.Fd(), unix.F_NOCACHE, 1); err != nil {
			e.logDebug("F_NOCACHE rejected; cached buffered tier", "path", path, "err", err)
		}
	}
	if h.Sequential {
		f.declareSequential()
	}
	fs, err := ProbeFs(ctx, path)
	if err != nil {
		e.logDebug("fs probe failed; media unknown", "path", path, "err", err)
		fs = FsUnknown
	}
	f.fsType = fs
	ok = true
	return f, nil
}

// preallocate reserves size bytes with F_PREALLOCATE (contiguous preferred,
// then any) and sets the logical size. The reservation makes ENOSPC surface up
// front; the extended range reads as zeros (no stale-data exposure).
func preallocate(f *os.File, size int64) error {
	st := &unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG | unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  size,
	}
	if err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, st); err != nil {
		st.Flags = unix.F_ALLOCATEALL // retry fragmented
		if err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, st); err != nil {
			return err
		}
	}
	return f.Truncate(size)
}

// WriteAt writes b's filled bytes at off (F_NOCACHE needs no alignment).
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

// WriteUnaligned writes a fragment at off (identical to WriteAt here; kept as a
// separate entry point for API parity).
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

// DontNeed is a no-op on macOS: there is no posix_fadvise(DONTNEED) equivalent,
// and the direct tier's F_NOCACHE already keeps reads out of the cache.
func (f *File) DontNeed(off, length int64) {}

// Fallocate reports UNSUPPORTED honestly: macOS has no hole-to-allocated
// conversion, so the de-sparse caller runs the SEEK_DATA/SEEK_HOLE zero-fill
// walk. Never faked as success.
func (f *File) Fallocate(size int64) error {
	return fmt.Errorf("fcio: fallocate %s: %w", f.path, ErrFallocateUnsupported)
}

// DataExtents walks SEEK_DATA/SEEK_HOLE (available on macOS) returning data
// ranges as [start, end) pairs. Non-sparse filesystems report the whole file
// as one extent — the de-sparse no-op case.
func (f *File) DataExtents() ([][2]int64, error) {
	fi, err := f.f.Stat()
	if err != nil {
		return nil, fmt.Errorf("fcio: stat %s: %w", f.path, err)
	}
	size := fi.Size()
	fd := int(f.f.Fd())
	var extents [][2]int64
	for off := int64(0); off < size; {
		data, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break // no further data extents
		}
		if err != nil {
			return nil, fmt.Errorf("fcio: seek data %s @%d: %w", f.path, off, err)
		}
		hole, err := unix.Seek(fd, data, unix.SEEK_HOLE)
		if err != nil {
			return nil, fmt.Errorf("fcio: seek hole %s @%d: %w", f.path, data, err)
		}
		if hole > size {
			hole = size
		}
		if hole <= data {
			return nil, fmt.Errorf("fcio: invalid extent walk on %s @%d", f.path, data)
		}
		extents = append(extents, [2]int64{data, hole})
		off = hole
	}
	return extents, nil
}

// readChunk reads through the single fd (the F_NOCACHE tier needs no separate
// direct fd).
func (f *File) readChunk(p []byte, off int64, _ bool) (int, error) {
	if err := readFullAt(f.f, p, off); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	return len(p), nil
}

// declareSequential announces sequential access via fcntl(F_RDAHEAD, 1),
// widening the kernel readahead window.
func (f *File) declareSequential() {
	_, _ = unix.FcntlInt(f.f.Fd(), unix.F_RDAHEAD, 1)
}

// clearSparse is a no-op on macOS: a fallocate-walk de-sparse leaves the blob
// dense with no sparse attribute to strip.
func (f *File) clearSparse() error { return nil }
