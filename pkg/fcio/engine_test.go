package fcio

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fillRandom fills p from a math/rand/v2 source (v2 dropped Rand.Read).
func fillRandom(rng *rand.Rand, p []byte) {
	for len(p) >= 8 {
		binary.LittleEndian.PutUint64(p, rng.Uint64())
		p = p[8:]
	}
	for i := range p {
		p[i] = byte(rng.Uint32())
	}
}

// TestWriteAtReadAtRoundTrip is the fuzz-style table: 200 deterministic
// cases of random odd sizes, written as a shuffled random partition of the
// file, then read back at random odd offsets and compared byte-identically
// against the reference content.
func TestWriteAtReadAtRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 0))
	eng := NewEngine(testLogger(), nil, TierAuto)
	const slab = 64 << 10
	pool := NewPool(slab, 4*slab)
	ctx := t.Context()
	for i := range 200 {
		size := int64(rng.IntN(512 << 10))
		if i%7 == 0 {
			size = int64(rng.IntN(4096)) // small odd sizes
		}
		ref := make([]byte, size)
		fillRandom(rng, ref)

		f, err := eng.Open(ctx, filepath.Join(t.TempDir(), "f.bin"), size, Hints{})
		if err != nil {
			t.Fatalf("case %d open: %v", i, err)
		}
		// partition [0,size) into random-sized chunks, shuffled
		var chunks [][2]int64
		for off := int64(0); off < size; {
			n := min(int64(1+rng.IntN(slab)), size-off)
			chunks = append(chunks, [2]int64{off, n})
			off += n
		}
		rng.Shuffle(len(chunks), func(a, b int) { chunks[a], chunks[b] = chunks[b], chunks[a] })
		for _, c := range chunks {
			buf, err := pool.Get(ctx)
			if err != nil {
				t.Fatalf("case %d pool: %v", i, err)
			}
			buf.SetLen(int(c[1]))
			copy(buf.Data(), ref[c[0]:c[0]+c[1]])
			if err := f.WriteAt(buf, c[0]); err != nil {
				buf.Release()
				t.Fatalf("case %d write @%d+%d: %v", i, c[0], c[1], err)
			}
			buf.Release()
		}
		for range 8 {
			if size == 0 {
				break
			}
			off := rng.Int64N(size)
			n := min(int64(1+rng.Int64N(size-off)), slab)
			buf, err := pool.Get(ctx)
			if err != nil {
				t.Fatalf("case %d pool: %v", i, err)
			}
			buf.SetLen(int(n))
			if err := f.ReadAt(buf, off); err != nil {
				buf.Release()
				t.Fatalf("case %d read @%d+%d: %v", i, off, n, err)
			}
			if !bytes.Equal(buf.Data(), ref[off:off+n]) {
				t.Fatalf("case %d read @%d+%d mismatch", i, off, n)
			}
			buf.Release()
		}
		if err := f.Close(); err != nil {
			t.Fatalf("case %d close: %v", i, err)
		}
	}
}

// TestWriteUnalignedConcurrentNeighbors writes two adjacent unaligned blocks
// (head fragment + EOF tail shape) into one file concurrently; neither may
// clobber the other's bytes.
func TestWriteUnalignedConcurrentNeighbors(t *testing.T) {
	for _, mode := range []IOTier{TierAuto, TierDirect, TierPlain} {
		t.Run(mode.String(), func(t *testing.T) {
			eng := NewEngine(testLogger(), nil, mode)
			const blk = 1<<20 + 37 // deliberately unaligned
			d0 := bytes.Repeat([]byte{0xA5}, blk)
			d1 := bytes.Repeat([]byte{0x5A}, blk+5) // neighbor + EOF tail
			path := filepath.Join(t.TempDir(), "f.bin")
			f, err := eng.Open(t.Context(), path, -1, Hints{})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, 2)
			for _, job := range []struct {
				p   []byte
				off int64
			}{{d0, 0}, {d1, blk}} {
				wg.Go(func() {
					if err := f.WriteUnaligned(job.p, job.off); err != nil {
						errs <- err
					}
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatalf("write: %v", err)
			}
			if err := f.Fsync(); err != nil {
				t.Fatalf("fsync: %v", err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("readback: %v", err)
			}
			want := append(append([]byte{}, d0...), d1...)
			if !bytes.Equal(got, want) {
				t.Fatalf("file clobbered: got %d bytes, want %d", len(got), len(want))
			}
		})
	}
}

// TestReadAllFadviseSequential verifies the fadvise/plain ReadAll path: a
// simple in-order sequential loop (no readahead goroutine pipeline — the
// kernel reads ahead via FADV_SEQUENTIAL), so exactly one buffer is in flight
// regardless of media class. The readahead pipeline is direct-tier only.
func TestReadAllFadviseSequential(t *testing.T) {
	rng := rand.New(rand.NewPCG(2, 0))
	size := 4*readChunkSize + 12345 // several chunks per pass
	content := make([]byte, size)
	fillRandom(rng, content)
	for _, fs := range []FsType{FsSSD, FsHDD, FsNetFS, FsUnknown} {
		t.Run(fs.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "f.bin")
			if err := os.WriteFile(path, content, 0o644); err != nil {
				t.Fatalf("seed: %v", err)
			}
			eng := NewEngine(testLogger(), nil, TierAuto)
			f, err := eng.Open(t.Context(), path, -1, Hints{Sequential: true})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer f.Close()
			f.fsType = fs
			var got []byte
			prevOff := int64(-1)
			prevEnd := int64(0)
			err = eng.ReadAll(t.Context(), f, func(p []byte, off int64) error {
				if off <= prevOff {
					t.Errorf("out-of-order delivery: @%d after @%d", off, prevOff)
				}
				if off != prevEnd && prevOff >= 0 {
					t.Errorf("gap: @%d after end %d", off, prevEnd)
				}
				prevOff = off
				prevEnd = off + int64(len(p))
				got = append(got, p...)
				return nil
			})
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if !bytes.Equal(got, content) {
				t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(content))
			}
			if hw := f.maxInflight.Load(); hw != 1 {
				t.Fatalf("fadvise ReadAll high-water = %d, want 1 (sequential)", hw)
			}
		})
	}
}

func TestReadAllSmallFileSingleShot(t *testing.T) {
	content := []byte("hello, fcio")
	path := filepath.Join(t.TempDir(), "f.bin")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	eng := NewEngine(testLogger(), nil, TierAuto)
	f, err := eng.Open(t.Context(), path, -1, Hints{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	calls := 0
	var got []byte
	if err := eng.ReadAll(t.Context(), f, func(p []byte, off int64) error {
		calls++
		got = append(got, p...)
		return nil
	}); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if calls != 1 || !bytes.Equal(got, content) {
		t.Fatalf("calls = %d, got %q", calls, got)
	}
	if hw := f.maxInflight.Load(); hw != 1 {
		t.Fatalf("high-water = %d, want 1", hw)
	}
}

func TestReadAllEmptyFile(t *testing.T) {
	eng := NewEngine(testLogger(), nil, TierAuto)
	f, err := eng.Open(t.Context(), filepath.Join(t.TempDir(), "f.bin"), 0, Hints{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	called := false
	if err := eng.ReadAll(t.Context(), f, func(p []byte, off int64) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if called {
		t.Fatal("fn must not be called for an empty file")
	}
}

func TestReadAllContextCancel(t *testing.T) {
	content := make([]byte, 4*readChunkSize)
	path := filepath.Join(t.TempDir(), "f.bin")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	eng := NewEngine(testLogger(), nil, TierAuto)
	f, err := eng.Open(t.Context(), path, -1, Hints{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	called := false
	err = eng.ReadAll(ctx, f, func(p []byte, off int64) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("cancelled ReadAll must fail")
	}
	if called {
		t.Fatal("fn must not run after cancellation")
	}
}

func TestReadAtShortReadIsUnexpectedEOF(t *testing.T) {
	eng := NewEngine(testLogger(), nil, TierAuto)
	f, err := eng.Open(t.Context(), filepath.Join(t.TempDir(), "f.bin"), -1, Hints{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if err := f.WriteUnaligned([]byte("abc"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	pool := NewPool(64<<10, 64<<10)
	buf, err := pool.Get(t.Context())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer buf.Release()
	buf.SetLen(16)
	if err := f.ReadAt(buf, 0); err == nil {
		t.Fatal("read past EOF must fail")
	}
}

func TestDontNeedBestEffort(t *testing.T) {
	for _, mode := range []IOTier{TierAuto, TierDirect, TierPlain} {
		eng := NewEngine(testLogger(), nil, mode)
		f, err := eng.Open(t.Context(), filepath.Join(t.TempDir(), "f.bin"), 4096, Hints{})
		if err != nil {
			t.Fatalf("%s open: %v", mode, err)
		}
		if err := f.WriteUnaligned(bytes.Repeat([]byte{1}, 4096), 0); err != nil {
			t.Fatalf("%s write: %v", mode, err)
		}
		f.DontNeed(0, 4096) // best-effort: must not panic on any tier
		if err := f.Close(); err != nil {
			t.Fatalf("%s close: %v", mode, err)
		}
	}
}
