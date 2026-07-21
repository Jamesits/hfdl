//go:build linux

package fcio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// posix_fadvise advice values (linux/fadvise.h); x/sys/unix only exports
// these constants for the BSDs.
const (
	posixFadvSequential = 2
	posixFadvDontNeed   = 4
)

// Open creates (or opens) path on the engine's tier. size >= 0 attempts a
// tier-A fallocate preallocation; ENOSYS/EOPNOTSUPP/EINVAL degrade to tier B
// (no preallocation, the file grows as blocks land). size < 0
// opens without preallocation.
func (e *Engine) Open(ctx context.Context, path string, size int64, h Hints) (*File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bf, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, fmt.Errorf("fcio: open %s: %w", path, err)
	}
	f := &File{path: path, f: bf, size: size}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
		}
	}()
	if size >= 0 {
		if err := e.fallocate(int(bf.Fd()), size); err != nil {
			if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EINVAL) {
				e.logDebug("fallocate unsupported; storage tier B (no prealloc)", "path", path, "err", err)
			} else {
				return nil, fmt.Errorf("fcio: fallocate %s: %w", path, err)
			}
		} else {
			f.preallocated = true
		}
	}
	switch e.mode {
	case TierPlain:
		f.tier = tierPlain
	case TierDirect:
		caps, err := e.volumeCaps(ctx, path)
		if err != nil {
			return nil, err
		}
		if !caps.DirectOK {
			f.tier = tierFadvise
			break
		}
		df, err := os.OpenFile(path, os.O_RDWR|unix.O_DIRECT, 0)
		switch {
		case err == nil:
			f.df = df
			f.tier = tierDirect
			f.align = caps.Align
		case errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP):
			// O_DIRECT accepted at probe time but rejected for this file:
			// downgrade this file now (first-hit fallback) and settle the
			// whole volume to fadvise via the CapsCache so sibling files skip
			// the doomed O_DIRECT open.
			e.logDebug("O_DIRECT open rejected; downgrading file and volume to fadvise", "path", path, "err", err)
			e.downgradeVolumeDirect(ctx, path, caps.Align)
			f.tier = tierFadvise
		default:
			return nil, fmt.Errorf("fcio: open direct %s: %w", path, err)
		}
	default: // TierAuto
		f.tier = tierFadvise
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

func (e *Engine) fallocate(fd int, size int64) error {
	if e.fallocateFn != nil {
		return e.fallocateFn(fd, size)
	}
	return unix.Fallocate(fd, 0, 0, size)
}

// WriteAt writes b's filled bytes at off. On the direct tier off, length and
// buffer address must be volume-aligned (aligned interior only); anything
// else must go through WriteUnaligned.
func (f *File) WriteAt(b *Buf, off int64) error {
	p := b.Data()
	if len(p) == 0 {
		return nil
	}
	if f.tier == tierDirect {
		if err := checkAligned(f.path, p, off, f.align); err != nil {
			return err
		}
		if err := writeFullAt(f.df, p, off); err != nil {
			return fmt.Errorf("fcio: direct write %s @%d: %w", f.path, off, err)
		}
		return nil
	}
	if err := writeFullAt(f.f, p, off); err != nil {
		return fmt.Errorf("fcio: write %s @%d: %w", f.path, off, err)
	}
	return nil
}

// WriteUnaligned carries unaligned head/tail fragments and the EOF tail
// through the buffered fd — never the O_DIRECT fd, so no read-modify-write
// is needed. Callers partition block ranges
// so a fragment never crosses a block boundary; concurrent fragments and
// direct interiors of *other* blocks only ever share a page-cache page with
// other buffered writes, which the page lock serializes.
func (f *File) WriteUnaligned(p []byte, off int64) error {
	if len(p) == 0 {
		return nil
	}
	if err := writeFullAt(f.f, p, off); err != nil {
		return fmt.Errorf("fcio: write fragment %s @%d: %w", f.path, off, err)
	}
	return nil
}

// ReadAt fills b's filled length with file bytes at off (verify/copy reads;
// same tiers and alignment rules as WriteAt). A short read at EOF is an
// io.ErrUnexpectedEOF-wrapped error.
func (f *File) ReadAt(b *Buf, off int64) error {
	p := b.Data()
	if len(p) == 0 {
		return nil
	}
	rd := f.f
	if f.tier == tierDirect {
		if err := checkAligned(f.path, p, off, f.align); err != nil {
			return err
		}
		rd = f.df
	}
	if err := readFullAt(rd, p, off); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("fcio: read %s @%d: %w", f.path, off, io.ErrUnexpectedEOF)
		}
		return fmt.Errorf("fcio: read %s @%d: %w", f.path, off, err)
	}
	return nil
}

// DontNeed evicts [off, off+length) from the page cache on the fadvise tier
// (best-effort; trailing behind the flush offset). No-op on other tiers.
func (f *File) DontNeed(off, length int64) {
	if f.tier != tierFadvise {
		return
	}
	_ = unix.Fadvise(int(f.f.Fd()), off, length, posixFadvDontNeed)
}

// Fallocate is the raw fallocate(0, 0, size): ENOSYS/EOPNOTSUPP/EINVAL are
// surfaced for the caller's tier-B decision.
func (f *File) Fallocate(size int64) error {
	if err := unix.Fallocate(int(f.f.Fd()), 0, 0, size); err != nil {
		return fmt.Errorf("fcio: fallocate %s: %w", f.path, err)
	}
	return nil
}

// DataExtents walks SEEK_DATA/SEEK_HOLE returning data ranges as [start,
// end) pairs. Non-sparse filesystems report the whole file as one extent —
// exactly the de-sparse no-op case, since there is nothing to fix.
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

// readChunk is the ReadAll pipeline read: the aligned body via the O_DIRECT
// fd, the unaligned EOF tail via the buffered fd.
func (f *File) readChunk(p []byte, off int64, direct bool) (int, error) {
	rd := f.f
	if direct && f.df != nil {
		rd = f.df
	}
	if err := readFullAt(rd, p, off); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	return len(p), nil
}

// declareSequential announces POSIX_FADV_SEQUENTIAL on the fadvise tier
// (doubles the kernel readahead window; the direct tier runs its own
// application-level readahead instead).
func (f *File) declareSequential() {
	if f.tier != tierFadvise {
		return
	}
	_ = unix.Fadvise(int(f.f.Fd()), 0, 0, posixFadvSequential)
}

// clearSparse is a no-op on Linux: the fallocate/SEEK_HOLE de-sparse path
// leaves the blob dense with no sparse attribute to strip.
func (f *File) clearSparse() error { return nil }
