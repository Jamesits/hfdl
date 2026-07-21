package fcio

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"unsafe"
)

// ioTier is the resolved per-file tier (distinct from the requested IOTier:
// TierAuto resolves to tierFadvise, TierDirect may downgrade per volume).
type ioTier int

const (
	tierFadvise ioTier = iota // buffered IO + trailing fadvise(DONTNEED) (default)
	tierDirect                // O_DIRECT aligned interior + buffered fragments
	tierPlain                 // buffered, no fadvise (exotic netfs / non-linux)
)

func (t ioTier) String() string {
	switch t {
	case tierFadvise:
		return "fadvise"
	case tierDirect:
		return "direct"
	case tierPlain:
		return "plain"
	}
	return "invalid"
}

// File is one open cache/blob file on its resolved tier.
type File struct {
	path string
	f    *os.File // buffered fd: unaligned fragments, tail reads, metadata
	df   *os.File // linux direct tier only: O_DIRECT fd for the aligned interior

	tier         ioTier
	align        int64 // direct-tier alignment; 0 otherwise
	size         int64 // declared size at Open; -1 when unknown
	fsType       FsType
	preallocated bool // tier-A fallocate succeeded at Open

	maxInflight atomic.Int32 // ReadAll pipeline high-water mark (observability)
}

// Path returns the file path given at Open.
func (f *File) Path() string { return f.path }

// Fsync flushes the inode regardless of tier — O_DIRECT bypasses the page
// cache, not the volatile drive cache.
func (f *File) Fsync() error {
	if err := f.f.Sync(); err != nil {
		return fmt.Errorf("fcio: fsync %s: %w", f.path, err)
	}
	return nil
}

// Close releases both fds; it is safe to call on an already-closed File.
func (f *File) Close() error {
	var err error
	if f.df != nil {
		if cerr := f.df.Close(); cerr != nil && !errors.Is(cerr, os.ErrClosed) {
			err = cerr
		}
		f.df = nil
	}
	if f.f != nil {
		if cerr := f.f.Close(); cerr != nil && !errors.Is(cerr, os.ErrClosed) && err == nil {
			err = cerr
		}
		f.f = nil
	}
	if err != nil {
		return fmt.Errorf("fcio: close %s: %w", f.path, err)
	}
	return nil
}

// UnalignedError rejects a direct-tier WriteAt/ReadAt whose offset, length
// or buffer address is not a multiple of the volume alignment. Callers must
// route such fragments through WriteUnaligned, which writes them via the
// buffered fd.
type UnalignedError struct {
	Path  string
	Off   int64
	Len   int
	Align int64
	Cause string // "offset" | "length" | "buffer address"
}

func (e *UnalignedError) Error() string {
	return fmt.Sprintf("fcio: unaligned direct IO on %s: off=%d len=%d align=%d (bad %s)",
		e.Path, e.Off, e.Len, e.Align, e.Cause)
}

// checkAligned enforces the direct-tier alignment rule on offset, length and
// buffer address (FastCopy minSectorSize / ALIGN_SIZE analog).
func checkAligned(path string, p []byte, off, align int64) error {
	if len(p) == 0 {
		return nil
	}
	if align <= 0 {
		return &UnalignedError{Path: path, Off: off, Len: len(p), Align: align, Cause: "alignment unknown"}
	}
	if off%align != 0 {
		return &UnalignedError{Path: path, Off: off, Len: len(p), Align: align, Cause: "offset"}
	}
	if int64(len(p))%align != 0 {
		return &UnalignedError{Path: path, Off: off, Len: len(p), Align: align, Cause: "length"}
	}
	if uintptr(unsafe.Pointer(&p[0]))%uintptr(align) != 0 {
		return &UnalignedError{Path: path, Off: off, Len: len(p), Align: align, Cause: "buffer address"}
	}
	return nil
}

func writeFullAt(w io.WriterAt, p []byte, off int64) error {
	for len(p) > 0 {
		n, err := w.WriteAt(p, off)
		off += int64(n)
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func readFullAt(r io.ReaderAt, p []byte, off int64) error {
	for len(p) > 0 {
		n, err := r.ReadAt(p, off)
		off += int64(n)
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

// existingAncestor walks up from path to the nearest existing directory (or
// file): probing/stat must work for paths that do not exist yet.
func existingAncestor(path string) string {
	p := path
	for {
		if _, err := os.Stat(p); err == nil {
			return p
		}
		parent := filepath.Dir(p)
		if parent == p {
			return p
		}
		p = parent
	}
}
