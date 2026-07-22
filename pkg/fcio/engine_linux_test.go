//go:build linux

package fcio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

// fakeCaps is an in-memory CapsCache.
type fakeCaps struct {
	mu   sync.Mutex
	m    map[string][]byte
	gets int
	puts int
}

func newFakeCaps() *fakeCaps { return &fakeCaps{m: map[string][]byte{}} }

func (c *fakeCaps) GetCaps(_ context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	return c.m[key], nil
}

func (c *fakeCaps) PutCaps(_ context.Context, key string, caps []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts++
	c.m[key] = append([]byte{}, caps...)
	return nil
}

func (c *fakeCaps) get(key string) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[key]
}

func TestProbeFsRoot(t *testing.T) {
	dir := t.TempDir()
	var st unix.Stat_t
	if err := unix.Stat(dir, &st); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	oldRoot := sysfsRoot
	sysfsRoot = root
	t.Cleanup(func() { sysfsRoot = oldRoot })
	device := filepath.Join(root, "devices", "virtual-test")
	if err := os.MkdirAll(filepath.Join(device, "queue"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(device, "queue", "rotational"), []byte("0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "dev", "block", fmt.Sprintf("%d:%d", unix.Major(st.Dev), unix.Minor(st.Dev)))
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(device, link); err != nil {
		t.Fatal(err)
	}
	fs, err := ProbeFs(t.Context(), dir)
	if err != nil {
		t.Fatalf("ProbeFs: %v", err)
	}
	if fs != FsSSD {
		t.Fatalf("media class = %s, want ssd", fs)
	}
}

func TestVolumeOf(t *testing.T) {
	vs := NewVolumeSet()
	id, err := vs.VolumeOf(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("VolumeOf: %v", err)
	}
	if id == "" {
		t.Fatal("empty volume id")
	}
}

func TestFallocateTierBSeam(t *testing.T) {
	eng := NewEngine(testLogger(), nil, TierAuto)
	eng.fallocateFn = func(fd int, size int64) error { return unix.ENOSYS }
	f, err := eng.Open(t.Context(), filepath.Join(t.TempDir(), "f.bin"), 1<<20, Hints{})
	if err != nil {
		t.Fatalf("open with ENOSYS fallocate: %v", err)
	}
	defer f.Close()
	if f.preallocated {
		t.Fatal("tier B must skip preallocation on ENOSYS")
	}
	// tier B files still grow and round-trip as blocks land
	if err := f.WriteUnaligned([]byte("tier-b"), 4096); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, 6)
	if _, err := f.f.ReadAt(got, 4096); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "tier-b" {
		t.Fatalf("got %q", got)
	}
}

func TestFallocateReal(t *testing.T) {
	eng := NewEngine(testLogger(), nil, TierAuto)
	f, err := eng.Open(t.Context(), filepath.Join(t.TempDir(), "f.bin"), 1<<20, Hints{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if !f.preallocated {
		t.Skip("fallocate unsupported on this filesystem")
	}
	if err := f.Fallocate(2 << 20); err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EINVAL) {
			t.Skipf("fallocate unsupported here: %v", err)
		}
		t.Fatalf("raw Fallocate: %v", err)
	}
}

func TestDataExtentsPunchedHole(t *testing.T) {
	eng := NewEngine(testLogger(), nil, TierAuto)
	path := filepath.Join(t.TempDir(), "f.bin")
	f, err := eng.Open(t.Context(), path, -1, Hints{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	full := bytes.Repeat([]byte{0x7E}, 12288)
	if err := f.WriteUnaligned(full, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	err = unix.Fallocate(int(f.f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 4096, 4096)
	if err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) {
			t.Skipf("punch hole unsupported here: %v", err)
		}
		t.Fatalf("punch hole: %v", err)
	}
	exts, err := f.DataExtents()
	if err != nil {
		t.Fatalf("DataExtents: %v", err)
	}
	want := [][2]int64{{0, 4096}, {8192, 12288}}
	if !reflect.DeepEqual(exts, want) {
		t.Fatalf("extents = %v, want %v", exts, want)
	}
	// the hole must read back as zeros
	hole := make([]byte, 4096)
	if _, err := f.f.ReadAt(hole, 4096); err != nil {
		t.Fatalf("read hole: %v", err)
	}
	if !bytes.Equal(hole, make([]byte, 4096)) {
		t.Fatal("hole must read as zeros")
	}
}

func TestDirectDowngradeViaCapsCache(t *testing.T) {
	dir := t.TempDir()
	dev, err := statVolumeID(dir)
	if err != nil {
		t.Fatalf("statVolumeID: %v", err)
	}
	caps := newFakeCaps()
	caps.m["volcaps:"+string(dev)] = []byte(`{"direct_ok":false,"align":4096}`)
	eng := NewEngine(testLogger(), caps, TierDirect)
	f, err := eng.Open(t.Context(), filepath.Join(dir, "f.bin"), 4096, Hints{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if f.tier != tierFadvise {
		t.Fatalf("tier = %s, want fadvise downgrade", f.tier)
	}
	if f.df != nil {
		t.Fatal("downgraded file must not hold an O_DIRECT fd")
	}
}

func TestDirectPerFileRejectionDowngradesVolume(t *testing.T) {
	if _, err := os.Stat("/dev/shm"); err != nil {
		t.Skipf("/dev/shm unavailable: %v", err)
	}
	dir, err := os.MkdirTemp("/dev/shm", "hfdl-direct-test-")
	if err != nil {
		t.Skipf("cannot create under /dev/shm: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove temp directory: %v", err)
		}
	})
	dev, err := statVolumeID(dir)
	if err != nil {
		t.Fatalf("statVolumeID: %v", err)
	}
	caps := newFakeCaps()
	key := "volcaps:" + string(dev)
	caps.m[key] = []byte(`{"direct_ok":true,"align":4096}`)
	eng := NewEngine(testLogger(), caps, TierDirect)
	f, err := eng.Open(t.Context(), filepath.Join(dir, "f.bin"), -1, Hints{})
	if err != nil {
		t.Fatalf("per-file O_DIRECT rejection must downgrade without error: %v", err)
	}
	defer f.Close()
	if f.tier == tierDirect {
		t.Skip("/dev/shm unexpectedly supports O_DIRECT")
	}
	if f.tier != tierFadvise || f.df != nil {
		t.Fatalf("file did not downgrade: tier=%s direct-fd=%v", f.tier, f.df != nil)
	}
	var cached volCaps
	if err := json.Unmarshal(caps.get(key), &cached); err != nil {
		t.Fatalf("decode downgraded caps: %v", err)
	}
	if cached.DirectOK || cached.Align != 4096 {
		t.Fatalf("downgraded caps = %+v", cached)
	}
}

func TestDirectProbeCachesDecision(t *testing.T) {
	dir := t.TempDir()
	dev, err := statVolumeID(dir)
	if err != nil {
		t.Fatalf("statVolumeID: %v", err)
	}
	caps := newFakeCaps()
	eng := NewEngine(testLogger(), caps, TierDirect)
	f, err := eng.Open(t.Context(), filepath.Join(dir, "f.bin"), 4096, Hints{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	raw := caps.get("volcaps:" + string(dev))
	if raw == nil {
		t.Fatal("probe decision not persisted to CapsCache")
	}
	var c volCaps
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("cached caps not JSON: %v", err)
	}
	if c.Align <= 0 {
		t.Fatalf("cached align = %d", c.Align)
	}
	second := NewEngine(testLogger(), caps, TierDirect)
	c2, err := second.volumeCaps(t.Context(), filepath.Join(dir, "second.bin"))
	if err != nil {
		t.Fatalf("cached volumeCaps: %v", err)
	}
	if *c2 != c {
		t.Fatalf("round-trip caps = %+v, want %+v", *c2, c)
	}
	caps.mu.Lock()
	gets, puts := caps.gets, caps.puts
	caps.mu.Unlock()
	if gets != 2 || puts != 1 {
		t.Fatalf("cache calls after second lookup: gets=%d puts=%d, want 2/1", gets, puts)
	}
}

// TestDirectTierMixedWrites exercises the aligned-interior plus
// head/tail-fragment split on a real O_DIRECT volume; skipped where the
// probe downgrades (tmpfs et al).
func TestDirectTierMixedWrites(t *testing.T) {
	eng := NewEngine(testLogger(), nil, TierDirect)
	path := filepath.Join(t.TempDir(), "f.bin")
	const total = 4096 + 64<<10 + 37 // head + one aligned slab + tail
	f, err := eng.Open(t.Context(), path, total, Hints{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if f.tier != tierDirect {
		t.Skipf("direct IO unsupported here (tier %s)", f.tier)
	}
	rng := rand.New(rand.NewPCG(3, 0))
	ref := make([]byte, total)
	fillRandom(rng, ref)

	if err := f.WriteUnaligned(ref[:37], 0); err != nil {
		t.Fatalf("head: %v", err)
	}
	pool := NewPool(64<<10, 64<<10)
	buf, err := pool.Get(t.Context())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	buf.SetLen(64 << 10)
	copy(buf.Data(), ref[4096:4096+64<<10])
	if err := f.WriteAt(buf, 4096); err != nil {
		buf.Release()
		t.Fatalf("interior: %v", err)
	}
	buf.Release()
	if err := f.WriteUnaligned(ref[4096+64<<10:], 4096+64<<10); err != nil {
		t.Fatalf("tail: %v", err)
	}
	if err := f.Fsync(); err != nil {
		t.Fatalf("fsync: %v", err)
	}
	// unaligned WriteAt must be rejected on the direct tier
	bad, err := pool.Get(t.Context())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer bad.Release()
	bad.SetLen(37)
	var unaligned *UnalignedError
	if err := f.WriteAt(bad, 0); !errors.As(err, &unaligned) {
		t.Fatalf("unaligned direct WriteAt: got %v, want UnalignedError", err)
	}
	// full-file readback through the ReadAll pipeline (direct body + tail)
	var got []byte
	if err := eng.ReadAll(t.Context(), f, func(p []byte, off int64) error {
		got = append(got, p...)
		return nil
	}); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	want := make([]byte, total)
	copy(want[:37], ref[:37])
	copy(want[4096:], ref[4096:])
	if !bytes.Equal(got, want) {
		t.Fatalf("readback mismatch: got %d bytes, want %d", len(got), len(want))
	}
	// the gap [37,4096) must be zeros
	if !bytes.Equal(got[37:4096], make([]byte, 4096-37)) {
		t.Fatal("interior gap must be zeros")
	}
}

func TestProbeAlignmentSane(t *testing.T) {
	a := probeAlignment(t.TempDir())
	if a < 512 || a > 1<<20 || a&(a-1) != 0 {
		t.Fatalf("implausible alignment %d", a)
	}
}
