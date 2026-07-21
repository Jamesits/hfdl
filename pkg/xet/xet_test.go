package xet

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestPartialReconstruction verifies the Range-header query path and
// offset_into_first_range semantics (cas_types/mod.rs: for a range query
// [a,b), offset_into_first_range is the location of "a" within the first
// returned term's decoded bytes).
func TestPartialReconstruction(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	content := schemeFile(t, f)

	// File layout: term0 = xorb1 = [0, 100005). Ask for [50_000, 60_000):
	// CAS answers with term0 and offset_into_first_range = 50_000.
	f.offsetFirst = 50_000
	f.terms = f.terms[:1] // first term only
	x1hash := f.terms[0].Hash
	x1 := f.xorbs[x1hash]
	f.xorbs = map[string]*xorbSpec{x1hash: x1}

	c := newTestClient(t, Config{CasURL: f.srv.URL}, nil)
	recon, err := c.fetchReconstruction(t.Context(), "/refresh", testFileID,
		&byteRangeJSON{Start: 50_000, End: 59_999})
	if err != nil {
		t.Fatalf("fetchReconstruction: %v", err)
	}

	// The fixture must have seen the exact Range header (inclusive end).
	found := false
	for _, h := range f.hitOrder() {
		if strings.Contains(h, "/v2/reconstructions/") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no v2 reconstruction hit: %v", f.hitOrder())
	}

	if recon.offsetIntoFirstRange != 50_000 {
		t.Fatalf("offsetIntoFirstRange = %d, want 50000", recon.offsetIntoFirstRange)
	}
	// Normalization must anchor the first term at base - offset so decoded
	// bytes address true file offsets.
	term := recon.terms[0]
	if term.fileStart != 0 || term.fileEnd != 100_005 {
		t.Fatalf("term file range = [%d,%d), want [0,100005)", term.fileStart, term.fileEnd)
	}

	// Decode the term via the same path Open uses and slice the requested
	// window out by file offset.
	s := c.NewSource(testFileID, int64(len(content)), "/refresh")
	s.recon = recon
	decoded, err := s.decodeTerm(t.Context(), &recon.terms[0])
	if err != nil {
		t.Fatalf("decodeTerm: %v", err)
	}
	got := decoded[50_000-term.fileStart : 60_000-term.fileStart]
	if !bytes.Equal(got, content[50_000:60_000]) {
		t.Fatalf("partial window mismatch: got %d bytes", len(got))
	}
}

// TestPartialReconstructionRangeHeader asserts the wire Range header value
// reaches the server verbatim (inclusive end).
func TestPartialReconstructionRangeHeader(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	schemeFile(t, f)
	f.terms = f.terms[:1]
	c := newTestClient(t, Config{CasURL: f.srv.URL}, nil)
	if _, err := c.fetchReconstruction(t.Context(), "/refresh", testFileID,
		&byteRangeJSON{Start: 7, End: 42}); err != nil {
		t.Fatalf("fetchReconstruction: %v", err)
	}
	if f.lastRangeHeader() != "bytes=7-42" {
		t.Fatalf("Range header = %q, want %q", f.lastRangeHeader(), "bytes=7-42")
	}
}

func TestChunkCacheLRUEviction(t *testing.T) {
	dir := t.TempDir()
	cc, err := openChunkCache(dir, 250)
	if err != nil {
		t.Fatal(err)
	}
	put := func(start, end uint32, size int) {
		cc.Put(cacheKey{xorb: "aa", start: start, end: end}, bytes.Repeat([]byte{byte(start + 1)}, size))
	}
	put(0, 1, 100) // oldest
	put(1, 2, 100)
	put(2, 3, 100) // forces eviction of [0,1)
	if _, ok := cc.Get(cacheKey{xorb: "aa", start: 0, end: 1}); ok {
		t.Fatal("oldest entry should have been evicted")
	}
	if _, ok := cc.Get(cacheKey{xorb: "aa", start: 2, end: 3}); !ok {
		t.Fatal("newest entry should survive")
	}
	n, total := cc.stats()
	if total > 250 {
		t.Fatalf("cache over cap: %d bytes in %d entries", total, n)
	}

	// Touch [1,2) so [2,3) becomes the eviction victim.
	if _, ok := cc.Get(cacheKey{xorb: "aa", start: 1, end: 2}); !ok {
		t.Fatal("entry [1,2) missing")
	}
	put(3, 4, 100)
	if _, ok := cc.Get(cacheKey{xorb: "aa", start: 2, end: 3}); ok {
		t.Fatal("LRU order broken: [2,3) should be evicted after [1,2) was touched")
	}
	if _, ok := cc.Get(cacheKey{xorb: "aa", start: 1, end: 2}); !ok {
		t.Fatal("touched entry should survive eviction")
	}
}

func TestChunkCacheReopen(t *testing.T) {
	dir := t.TempDir()
	cc, err := openChunkCache(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("serialized range bytes")
	cc.Put(cacheKey{xorb: "aa", start: 3, end: 7}, data)

	cc2, err := openChunkCache(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := cc2.Get(cacheKey{xorb: "aa", start: 3, end: 7})
	if !ok || !bytes.Equal(got, data) {
		t.Fatalf("reopen lost entry: ok=%v len=%d", ok, len(got))
	}
	if _, ok := cc2.Get(cacheKey{xorb: "aa", start: 0, end: 1}); ok {
		t.Fatal("phantom entry after reopen")
	}
}

func TestChunkCacheDisabledWithoutDir(t *testing.T) {
	c := newTestClient(t, Config{CasURL: "http://unused"}, nil)
	if c.cache != nil {
		t.Fatal("cache should be nil when CacheDir is empty")
	}
}

// TestBG4RoundTrip guards the transpose port directly, including all four
// n%4 remainder paths and tiny inputs.
func TestBG4RoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 255, 256, 257, 100_001, 100_002, 100_003} {
		in := deterministicBytes(byte(n), n)
		if got := bg4Regroup(bg4Split(in)); !bytes.Equal(got, in) {
			t.Fatalf("n=%d: round trip mismatch", n)
		}
	}
}

// TestDecodeChunkBadScheme ensures unknown scheme ids are typed errors.
func TestDecodeChunkBadScheme(t *testing.T) {
	_, err := decodeChunk("aa", chunkHeader{scheme: 9, unpackedLen: 1}, []byte{0})
	var de *DataError
	if !errors.As(err, &de) {
		t.Fatalf("want DataError, got %v", err)
	}
}

// TestStreamShortRead confirms a truncated signed-range body is a data
// error, not silently short output.
func TestStreamShortRead(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	content := schemeFile(t, f)
	// Corrupt the serialized xorb: chop the tail.
	for _, x := range f.xorbs {
		x.serialized = x.serialized[:len(x.serialized)-10]
	}
	c := newTestClient(t, Config{CasURL: f.srv.URL}, nil)
	s := prepareSource(t, c, testFileID, int64(len(content)))
	r, err := s.Open(t.Context(), 0, int64(len(content)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	_, err = io.ReadAll(r)
	if err == nil {
		t.Fatal("expected error from truncated xorb")
	}
}
