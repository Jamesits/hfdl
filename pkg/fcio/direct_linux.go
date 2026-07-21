//go:build linux

package fcio

import (
	"context"
	"encoding/json"
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
	caps := &volCaps{DirectOK: probeDirectIO(filepath.Dir(path), align), Align: align}
	e.vols.Store(key, caps)
	if e.caps != nil {
		if raw, err := json.Marshal(caps); err == nil {
			if err := e.caps.PutCaps(ctx, key, raw); err != nil {
				e.logDebug("caps cache write failed", "key", key, "err", err)
			}
		}
	}
	e.logDebug("volume direct-IO probed", "dev", string(dev), "direct", caps.DirectOK, "align", caps.Align)
	return caps, nil
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

// probeDirectIO writes and reads back one aligned block through O_DIRECT on
// a temp file. Any failure (EINVAL/EOPNOTSUPP on tmpfs and several netfs,
// or anything else) conservatively reports the volume as unsupported — the
// fadvise fallback is always safe.
func probeDirectIO(dir string, align int64) bool {
	tmp, err := os.CreateTemp(dir, ".hfdl-dioprobe-*")
	if err != nil {
		return false
	}
	name := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return false
	}
	defer func() { _ = os.Remove(name) }()
	dfd, err := os.OpenFile(name, os.O_RDWR|unix.O_DIRECT, 0o600)
	if err != nil {
		return false
	}
	defer dfd.Close()
	size := int((align + slabAlign - 1) / slabAlign * slabAlign)
	buf, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		return false
	}
	defer func() { _ = unix.Munmap(buf) }()
	if _, err := dfd.Write(buf[:align]); err != nil {
		return false
	}
	if _, err := dfd.ReadAt(buf[:align], 0); err != nil {
		return false
	}
	return true
}
