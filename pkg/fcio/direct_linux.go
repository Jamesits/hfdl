//go:build linux

package fcio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// defaultDIOAlign is the final fallback of the alignment chain:
// statx(STATX_DIOALIGN) → BLKSSZGET → 4KiB.
const defaultDIOAlign int64 = 4096

// volCaps is the per-volume direct-IO capability record, cached in memory
// and in the injected CapsCache as JSON under "volcaps:<dev>".
type volCaps struct {
	DirectOK bool  `json:"direct_ok"`
	Align    int64 `json:"align"`
}

// volumeCaps resolves the cached or freshly probed direct-IO capability of
// the volume behind path.
func (e *Engine) volumeCaps(ctx context.Context, path string) (*volCaps, error) {
	dev, err := statVolumeID(path)
	if err != nil {
		return nil, err
	}
	key := "volcaps:" + string(dev)
	if v, ok := e.vols.Load(key); ok {
		return v.(*volCaps), nil
	}
	if e.caps != nil {
		raw, err := e.caps.GetCaps(ctx, key)
		if err != nil {
			e.logDebug("caps cache read failed; probing volume", "key", key, "err", err)
		} else if raw != nil {
			var c volCaps
			if json.Unmarshal(raw, &c) == nil && c.Align > 0 {
				e.vols.Store(key, &c)
				return &c, nil
			}
		}
	}
	align := probeAlignment(path)
	var directOK, cacheable bool
	if align > slabAlign {
		// Pool slabs and ReadAll scratch are only guaranteed slabAlign-aligned
		// (4KiB); a volume demanding a larger DIO alignment cannot be served
		// from the pool without mis-aligning, so downgrade it to fadvise
		// rather than risk an unaligned O_DIRECT write. Alignment is a stable
		// property, so the decision is cacheable.
		e.logDebug("direct-IO alignment exceeds slab alignment; fadvise tier",
			"path", path, "align", align, "slab_align", slabAlign)
		directOK, cacheable = false, true
	} else {
		directOK, cacheable = probeDirectIO(filepath.Dir(path), align)
	}
	caps := &volCaps{DirectOK: directOK, Align: align}
	e.vols.Store(key, caps)
	// Only persist a definitive verdict (success or a stable "unsupported"):
	// a transient probe failure (EIO/ENOSPC/…) must not bake DirectOK=false
	// into the durable CapsCache, or the volume would stay downgraded across
	// restarts on a one-off error.
	if cacheable && e.caps != nil {
		if raw, err := json.Marshal(caps); err == nil {
			if err := e.caps.PutCaps(ctx, key, raw); err != nil {
				e.logDebug("caps cache write failed", "key", key, "err", err)
			}
		}
	}
	e.logDebug("volume direct-IO probed", "dev", string(dev), "direct", caps.DirectOK, "align", caps.Align, "cacheable", cacheable)
	return caps, nil
}

// downgradeVolumeDirect settles the volume behind path onto the fadvise tier
// after a per-file O_DIRECT rejection: it rewrites the cached volCaps with
// DirectOK=false (in memory and, best-effort, the durable CapsCache) so
// sibling files on the same volume skip the doomed O_DIRECT open. The probed
// alignment is preserved.
func (e *Engine) downgradeVolumeDirect(ctx context.Context, path string, align int64) {
	dev, err := statVolumeID(path)
	if err != nil {
		return
	}
	key := "volcaps:" + string(dev)
	caps := &volCaps{DirectOK: false, Align: align}
	e.vols.Store(key, caps)
	if e.caps != nil {
		if raw, err := json.Marshal(caps); err == nil {
			if err := e.caps.PutCaps(ctx, key, raw); err != nil {
				e.logDebug("caps cache downgrade write failed", "key", key, "err", err)
			}
		}
	}
}

// directUnsupported reports whether an O_DIRECT open/read/write error means
// the volume genuinely does not support direct IO (a cacheable verdict), as
// opposed to a transient failure that must not persist.
func directUnsupported(err error) bool {
	return errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP)
}

// probeAlignment implements the alignment chain: statx(STATX_DIOALIGN)
// (Linux ≥6.1), then BLKSSZGET (only answers for block devices — regular
// files fall through), then the 4KiB default.
func probeAlignment(path string) int64 {
	var stx unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, path, 0, unix.STATX_DIOALIGN, &stx); err == nil &&
		stx.Mask&unix.STATX_DIOALIGN != 0 {
		a := int64(stx.Dio_mem_align)
		if off := int64(stx.Dio_offset_align); off > a {
			a = off
		}
		if a > 0 {
			return a
		}
	}
	if f, err := os.Open(path); err == nil {
		defer f.Close()
		if ssz, err := unix.IoctlGetInt(int(f.Fd()), unix.BLKSSZGET); err == nil && ssz > 0 {
			return int64(ssz)
		}
	}
	return defaultDIOAlign
}

// probeDirectIO writes and reads back one aligned block through O_DIRECT on a
// temp file, verifying the bytes round-trip. It returns (ok, cacheable):
//   - ok reports whether direct IO works on the volume;
//   - cacheable reports whether the verdict is stable enough to persist. Only
//     a success or a genuine "unsupported" (EINVAL/EOPNOTSUPP — tmpfs and
//     several netfs) is cacheable; a transient failure (temp create, mmap,
//     EIO/ENOSPC) returns cacheable=false so it never bakes a false negative
//     into the durable CapsCache.
func probeDirectIO(dir string, align int64) (ok, cacheable bool) {
	tmp, err := os.CreateTemp(dir, ".hfdl-dioprobe-*")
	if err != nil {
		return false, false // environment issue, not a capability verdict
	}
	name := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return false, false
	}
	defer func() { _ = os.Remove(name) }()
	dfd, err := os.OpenFile(name, os.O_RDWR|unix.O_DIRECT, 0o600)
	if err != nil {
		return false, directUnsupported(err)
	}
	defer dfd.Close()
	size := int((align + slabAlign - 1) / slabAlign * slabAlign)
	// Two aligned regions in one page-aligned mapping: write from the first,
	// read back into the second, so a silently short or misdirected O_DIRECT
	// read is caught by the compare rather than passing on stale bytes.
	buf, err := unix.Mmap(-1, 0, 2*size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		return false, false // mmap failure is environmental
	}
	defer func() { _ = unix.Munmap(buf) }()
	src := buf[:align]
	dst := buf[size : size+int(align)]
	for i := range src {
		src[i] = byte(i*7 + 1) // known non-zero pattern
	}
	if _, err := dfd.Write(src); err != nil {
		return false, directUnsupported(err)
	}
	if _, err := dfd.ReadAt(dst, 0); err != nil {
		return false, directUnsupported(err)
	}
	if !bytes.Equal(src, dst) {
		// O_DIRECT accepted the IO but did not round-trip: unusable, and the
		// mismatch is a stable property of this volume — cache the downgrade.
		return false, true
	}
	return true, true
}
