//go:build windows

package fcio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

// winDirectAlign is the alignment for the FILE_FLAG_NO_BUFFERING direct tier.
// Modern volumes use 512- or 4096-byte sectors; 4096 is a multiple of both and
// matches slabAlign — pool slabs are VirtualAlloc'd (page-aligned) so every
// slab base satisfies the direct-tier buffer-address rule.
const winDirectAlign int64 = 4096

// winVolCaps mirrors the Linux volCaps JSON so the injected CapsCache carries
// the same "volcaps:<id>" value shape across platforms.
type winVolCaps struct {
	DirectOK bool  `json:"direct_ok"`
	Align    int64 `json:"align"`
}

// fileSetSparseBuffer is the FSCTL_SET_SPARSE input. A nil input sets the
// attribute; SetSparse=0 clears it.
type fileSetSparseBuffer struct {
	SetSparse uint8 // BOOLEAN
}

// fileAllocatedRangeBuffer is both the FSCTL_QUERY_ALLOCATED_RANGES input (the
// queried span) and each output entry (one allocated — i.e. non-hole — range).
type fileAllocatedRangeBuffer struct {
	FileOffset int64
	Length     int64
}

// Open creates (or opens) path on the engine's tier. size >= 0 marks the file
// sparse (FSCTL_SET_SPARSE — Tier B: out-of-order block writes leave gaps
// unallocated; best-effort, FAT/exFAT/ReFS fall back to plain non-sparse) and
// sets the logical size (SetEndOfFile, via Truncate). Windows has no fallocate
// equivalent — zero-fill preallocation is self-defeating and SetFileValidData
// needs admin and can expose stale disk contents — so grow-as-blocks-land with
// the file marked sparse is the designed default.
func (e *Engine) Open(ctx context.Context, path string, size int64, h Hints) (*File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bf, err := createFileHandle(path, h.Sequential, false)
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
		sparse := setSparse(bf, true) == nil
		if !sparse {
			e.logDebug("FSCTL_SET_SPARSE rejected; plain non-sparse file", "path", path)
		}
		if err := bf.Truncate(size); err != nil {
			return nil, fmt.Errorf("fcio: set size %s: %w", path, err)
		}
		// A non-sparse volume's SetEndOfFile reserves the full size (FAT/exFAT
		// have no holes), so the file is dense — the Tier-A analog; a sparse
		// file grows as blocks land (Tier B).
		f.preallocated = !sparse
	}
	if e.mode == TierDirect {
		directOK, align := e.windowsDirectCaps(ctx, path)
		if directOK {
			df, derr := createFileHandle(path, h.Sequential, true)
			if derr != nil {
				// FILE_FLAG_NO_BUFFERING accepted at probe time but rejected for
				// this file: settle it onto the buffered tier.
				e.logDebug("FILE_FLAG_NO_BUFFERING open rejected; buffered tier", "path", path, "err", derr)
			} else {
				f.df = df
				f.tier = tierDirect
				f.align = align
			}
		}
	}
	// TierAuto has no fadvise analog on Windows and TierPlain is explicit: both
	// resolve to the plain buffered tier (f.tier already tierPlain).
	fs, err := ProbeFs(ctx, path)
	if err != nil {
		e.logDebug("fs probe failed; media unknown", "path", path, "err", err)
		fs = FsUnknown
	}
	f.fsType = fs
	ok = true
	return f, nil
}

// createFileHandle opens path via CreateFile so the sequential and
// no-buffering flags (which os.OpenFile cannot express) can be set at open.
// noBuffering adds FILE_FLAG_NO_BUFFERING+FILE_FLAG_WRITE_THROUGH for the
// direct tier; sequential adds FILE_FLAG_SEQUENTIAL_SCAN (FastCopy sets it on
// every handle).
func createFileHandle(path string, sequential, noBuffering bool) (*os.File, error) {
	namep, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	access := uint32(windows.GENERIC_READ | windows.GENERIC_WRITE)
	share := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE)
	attrs := uint32(windows.FILE_ATTRIBUTE_NORMAL)
	if sequential {
		attrs |= windows.FILE_FLAG_SEQUENTIAL_SCAN
	}
	if noBuffering {
		attrs |= windows.FILE_FLAG_NO_BUFFERING | windows.FILE_FLAG_WRITE_THROUGH
	}
	h, err := windows.CreateFile(namep, access, share, nil, windows.OPEN_ALWAYS, attrs, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(h), path), nil
}

// setSparse toggles FILE_ATTRIBUTE_SPARSE_FILE. Setting it may pass a nil
// input; clearing requires an explicit SetSparse=FALSE buffer.
func setSparse(f *os.File, on bool) error {
	var in *byte
	var inLen uint32
	if !on {
		buf := fileSetSparseBuffer{SetSparse: 0}
		in = (*byte)(unsafe.Pointer(&buf))
		inLen = uint32(unsafe.Sizeof(buf))
	}
	var bytesReturned uint32
	return windows.DeviceIoControl(windows.Handle(f.Fd()), windows.FSCTL_SET_SPARSE,
		in, inLen, nil, 0, &bytesReturned, nil)
}

// WriteAt writes b's filled bytes at off. On the direct tier off, length and
// buffer address must be volume-aligned; anything else goes through
// WriteUnaligned.
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
// through the buffered fd — never the no-buffering fd.
func (f *File) WriteUnaligned(p []byte, off int64) error {
	if len(p) == 0 {
		return nil
	}
	if err := writeFullAt(f.f, p, off); err != nil {
		return fmt.Errorf("fcio: write fragment %s @%d: %w", f.path, off, err)
	}
	return nil
}

// ReadAt fills b's filled length with file bytes at off (same tiers and
// alignment rules as WriteAt).
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

// DontNeed is a no-op on Windows: there is no posix_fadvise(DONTNEED)
// equivalent, and the direct tier already bypasses the cache.
func (f *File) DontNeed(off, length int64) {}

// Fallocate reports UNSUPPORTED honestly: Windows has no hole-to-allocated
// conversion, so the de-sparse caller runs the FSCTL_QUERY_ALLOCATED_RANGES
// zero-fill walk. Faking success (the former ftruncate-as-fallocate) silently
// violated the dense-blob invariant (P0).
func (f *File) Fallocate(size int64) error {
	return fmt.Errorf("fcio: fallocate %s: %w", f.path, ErrFallocateUnsupported)
}

// DataExtents enumerates the file's allocated (non-hole) ranges via
// FSCTL_QUERY_ALLOCATED_RANGES — the SEEK_DATA/SEEK_HOLE counterpart. A
// non-sparse file reports the whole file as one range, so the de-sparse walk
// is a no-op exactly when there is nothing to fix.
func (f *File) DataExtents() ([][2]int64, error) {
	fi, err := f.f.Stat()
	if err != nil {
		return nil, fmt.Errorf("fcio: stat %s: %w", f.path, err)
	}
	size := fi.Size()
	if size == 0 {
		return nil, nil
	}
	h := windows.Handle(f.f.Fd())
	out := make([]fileAllocatedRangeBuffer, 64)
	entrySize := int(unsafe.Sizeof(out[0]))
	var extents [][2]int64
	pos := int64(0)
	for pos < size {
		in := fileAllocatedRangeBuffer{FileOffset: pos, Length: size - pos}
		var bytesReturned uint32
		err := windows.DeviceIoControl(h, windows.FSCTL_QUERY_ALLOCATED_RANGES,
			(*byte)(unsafe.Pointer(&in)), uint32(unsafe.Sizeof(in)),
			(*byte)(unsafe.Pointer(&out[0])), uint32(len(out)*entrySize),
			&bytesReturned, nil)
		more := errors.Is(err, windows.ERROR_MORE_DATA)
		if err != nil && !more {
			return nil, fmt.Errorf("fcio: query allocated ranges %s: %w", f.path, err)
		}
		n := int(bytesReturned) / entrySize
		if n == 0 {
			break
		}
		for i := 0; i < n; i++ {
			start := out[i].FileOffset
			end := out[i].FileOffset + out[i].Length
			if end > size {
				end = size
			}
			if end > start {
				extents = append(extents, [2]int64{start, end})
			}
			pos = end
		}
		if !more {
			break
		}
	}
	return extents, nil
}

// readChunk is the ReadAll pipeline read: the aligned body via the no-buffering
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

// declareSequential is a no-op on Windows: FILE_FLAG_SEQUENTIAL_SCAN can only
// be requested at CreateFile, which Open already does for Sequential hints.
func (f *File) declareSequential() {}

// clearSparse strips FILE_ATTRIBUTE_SPARSE_FILE (FSCTL_SET_SPARSE
// SetSparse=FALSE) and fsyncs, so the finished blob is an ordinary non-sparse
// file. It is a no-op when the file was never marked sparse (FAT/exFAT/ReFS),
// avoiding the FSCTL error such volumes return.
func (f *File) clearSparse() error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(f.f.Fd()), &info); err == nil {
		if info.FileAttributes&windows.FILE_ATTRIBUTE_SPARSE_FILE != 0 {
			if err := setSparse(f.f, false); err != nil {
				return fmt.Errorf("fcio: clear sparse %s: %w", f.path, err)
			}
		}
	}
	if err := f.f.Sync(); err != nil {
		return fmt.Errorf("fcio: fsync after densify %s: %w", f.path, err)
	}
	return nil
}

// windowsDirectCaps resolves the cached or freshly probed FILE_FLAG_NO_BUFFERING
// capability of the volume behind path, persisted through the injected
// CapsCache under the same "volcaps:<id>" key scheme as Linux.
func (e *Engine) windowsDirectCaps(ctx context.Context, path string) (bool, int64) {
	align := winDirectAlign
	dev, err := statVolumeID(path)
	if err != nil {
		return false, align
	}
	key := "volcaps:" + string(dev)
	if v, ok := e.vols.Load(key); ok {
		c := v.(*winVolCaps)
		return c.DirectOK, c.Align
	}
	if e.caps != nil {
		if raw, err := e.caps.GetCaps(ctx, key); err != nil {
			e.logDebug("caps cache read failed; probing volume", "key", key, "err", err)
		} else if raw != nil {
			var c winVolCaps
			if json.Unmarshal(raw, &c) == nil && c.Align > 0 {
				e.vols.Store(key, &c)
				return c.DirectOK, c.Align
			}
		}
	}
	directOK := probeWindowsDirect(filepath.Dir(path), align)
	c := &winVolCaps{DirectOK: directOK, Align: align}
	e.vols.Store(key, c)
	if e.caps != nil {
		if raw, err := json.Marshal(c); err == nil {
			if err := e.caps.PutCaps(ctx, key, raw); err != nil {
				e.logDebug("caps cache write failed", "key", key, "err", err)
			}
		}
	}
	e.logDebug("volume direct-IO probed", "dev", string(dev), "direct", directOK, "align", align)
	return directOK, align
}

// probeWindowsDirect writes and reads back one aligned block through a
// FILE_FLAG_NO_BUFFERING handle on a temp file, verifying the bytes round-trip.
func probeWindowsDirect(dir string, align int64) bool {
	tmp, err := os.CreateTemp(dir, ".hfdl-dioprobe-*")
	if err != nil {
		return false
	}
	name := tmp.Name()
	if cerr := tmp.Close(); cerr != nil {
		_ = os.Remove(name)
		return false
	}
	defer func() { _ = os.Remove(name) }()
	df, err := createFileHandle(name, false, true)
	if err != nil {
		return false
	}
	defer df.Close()
	size := int((align + slabAlign - 1) / slabAlign * slabAlign)
	buf, err := allocArena(int64(2 * size))
	if err != nil {
		return false
	}
	defer func() { _ = freeArena(buf) }()
	src := buf[:align]
	dst := buf[size : size+int(align)]
	for i := range src {
		src[i] = byte(i*7 + 1)
	}
	if _, err := df.WriteAt(src, 0); err != nil {
		return false
	}
	if _, err := df.ReadAt(dst, 0); err != nil {
		return false
	}
	return bytes.Equal(src, dst)
}
