package xet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/transfer"
)

func intervals(start, end int64) []transfer.Interval {
	return []transfer.Interval{{Start: start, End: end}}
}

const testFileID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// schemeFile builds a file spanning three xorbs, one per compression
// scheme, plus the term table mapping file order to chunks. BG4 chunk sizes
// cover all four n%4 remainder paths.
func schemeFile(t *testing.T, f *fixtureCAS) []byte {
	t.Helper()
	x1 := buildXorb(t, "aa00000000000000000000000000000000000000000000000000000000000001",
		[][]byte{deterministicBytes(0x11, 100_000), deterministicBytes(0x12, 5)},
		[]int{schemeNone, schemeNone})
	x2 := buildXorb(t, "bb00000000000000000000000000000000000000000000000000000000000002",
		[][]byte{deterministicBytes(0x21, 128*1024), deterministicBytes(0x22, 37_000)},
		[]int{schemeLZ4, schemeLZ4})
	x3 := buildXorb(t, "cc00000000000000000000000000000000000000000000000000000000000003",
		[][]byte{
			deterministicBytes(0x31, 100_001), // n%4 == 1
			deterministicBytes(0x32, 100_002), // n%4 == 2
			deterministicBytes(0x33, 100_003), // n%4 == 3
			deterministicBytes(0x34, 100_000), // n%4 == 0
		},
		[]int{schemeBG4LZ4, schemeBG4LZ4, schemeBG4LZ4, schemeBG4LZ4})

	var content []byte
	for _, x := range []*xorbSpec{x1, x2, x3} {
		f.xorbs[x.hash] = x
		for _, c := range x.chunks {
			content = append(content, c...)
		}
		f.terms = append(f.terms, termJSON{
			Hash:           x.hash,
			UnpackedLength: uint32(totalLen(x.chunks)),
			Range:          chunkRangeJSON{Start: 0, End: uint32(len(x.chunks))},
		})
	}
	return content
}

func totalLen(chunks [][]byte) (n int) {
	for _, c := range chunks {
		n += len(c)
	}
	return n
}

// tokenQueue serves tokens in order (last one repeats) and counts calls.
type tokenQueue struct {
	mu     *sync.Mutex
	calls  *int
	tokens []*hfapi.XetToken
}

func (q tokenQueue) source() TokenSource {
	return func(ctx context.Context, refreshRoute string) (*hfapi.XetToken, error) {
		q.mu.Lock()
		defer q.mu.Unlock()
		*q.calls++
		i := *q.calls - 1
		if i >= len(q.tokens) {
			i = len(q.tokens) - 1
		}
		return q.tokens[i], nil
	}
}

func newTokenQueue(tokens ...*hfapi.XetToken) (TokenSource, *int) {
	calls := new(int)
	return tokenQueue{mu: &sync.Mutex{}, calls: calls, tokens: tokens}.source(), calls
}

func goodToken() *hfapi.XetToken {
	return &hfapi.XetToken{AccessToken: "good", Exp: time.Now().Add(time.Hour)}
}

func newTestClient(t *testing.T, cfg Config, tok TokenSource) *Client {
	t.Helper()
	if tok == nil {
		tok, _ = newTokenQueue(goodToken())
	}
	return NewClient(nil, http.DefaultClient, cfg, tok, nil)
}

func prepareSource(t *testing.T, c *Client, fileID string, size int64) *Source {
	t.Helper()
	s := c.NewSource(fileID, size, "/refresh")
	if err := s.Prepare(t.Context()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	return s
}

func readOpen(t *testing.T, s *Source, off, length int64) []byte {
	t.Helper()
	r, err := s.Open(t.Context(), off, length)
	if err != nil {
		t.Fatalf("Open(%d,%d): %v", off, length, err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return b
}

func TestOpenAllSchemes(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	content := schemeFile(t, f)

	c := newTestClient(t, Config{CasURL: f.srv.URL}, nil)
	s := prepareSource(t, c, testFileID, int64(len(content)))

	got := readOpen(t, s, 0, int64(len(content)))
	if !bytes.Equal(got, content) {
		t.Fatalf("full read mismatch: got %d bytes, want %d", len(got), len(content))
	}

	// Sub-range spanning all three xorbs, starting and ending mid-chunk.
	off, ln := int64(90_000), int64(500_000)
	got = readOpen(t, s, off, ln)
	if !bytes.Equal(got, content[off:off+ln]) {
		t.Fatalf("sub-range read mismatch at [%d,%d)", off, off+ln)
	}

	// One byte at the very end.
	got = readOpen(t, s, int64(len(content))-1, 1)
	if !bytes.Equal(got, content[len(content)-1:]) {
		t.Fatalf("tail byte mismatch")
	}
}

func TestOpenEmptyRange(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	content := schemeFile(t, f)
	c := newTestClient(t, Config{CasURL: f.srv.URL}, nil)
	s := prepareSource(t, c, testFileID, int64(len(content)))
	got := readOpen(t, s, 10, 0)
	if len(got) != 0 {
		t.Fatalf("empty range returned %d bytes", len(got))
	}
}

func TestOpenNotPrepared(t *testing.T) {
	c := newTestClient(t, Config{CasURL: "http://unused"}, nil)
	s := c.NewSource(testFileID, 10, "/refresh")
	if _, err := s.Open(t.Context(), 0, 10); !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("want ErrNotPrepared, got %v", err)
	}
}

func TestUnpackedLengthMismatch(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	content := schemeFile(t, f)
	f.lieUnpacked = true

	c := newTestClient(t, Config{CasURL: f.srv.URL}, nil)
	s := prepareSource(t, c, testFileID, int64(len(content)))
	r, err := s.Open(t.Context(), 0, int64(len(content)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	_, err = io.ReadAll(r)
	var lm *LengthMismatchError
	if !errors.As(err, &lm) {
		t.Fatalf("want LengthMismatchError, got %v", err)
	}
	if lm.Want != lm.Got+1 {
		t.Fatalf("lie was +1, got want=%d got=%d", lm.Want, lm.Got)
	}
}

func TestV2FallbackToV1(t *testing.T) {
	for _, status := range []int{404, 501} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newFixtureCAS(t, testFileID)
			content := schemeFile(t, f)
			f.v2Status = status

			c := newTestClient(t, Config{CasURL: f.srv.URL}, nil)
			s := prepareSource(t, c, testFileID, int64(len(content)))
			got := readOpen(t, s, 0, int64(len(content)))
			if !bytes.Equal(got, content) {
				t.Fatal("content mismatch after v1 fallback")
			}
			hits := f.hitOrder()
			if len(hits) < 2 ||
				!strings.Contains(hits[0], "/v2/reconstructions/") ||
				!strings.Contains(hits[1], "/v1/reconstructions/") {
				t.Fatalf("fallback order wrong: %v", hits)
			}
			if s.current().version != 1 {
				t.Fatalf("recon version = %d, want 1", s.current().version)
			}
		})
	}
}

func TestCAS401TokenRefresh(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	content := schemeFile(t, f)

	src, calls := newTokenQueue(
		&hfapi.XetToken{AccessToken: "stale", Exp: time.Now().Add(time.Hour)},
		goodToken(),
	)
	c := newTestClient(t, Config{CasURL: f.srv.URL}, src)
	s := prepareSource(t, c, testFileID, int64(len(content))) // 401 → refresh → retry
	if *calls != 2 {
		t.Fatalf("token source called %d times, want 2", *calls)
	}
	got := readOpen(t, s, 0, int64(len(content)))
	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch after token refresh")
	}
	// Token now cached until exp: further CAS calls must not re-fetch it.
	_ = readOpen(t, s, 0, 100)
	if *calls != 2 {
		t.Fatalf("token re-fetched: calls=%d", *calls)
	}
}

func TestCAS429RateLimit(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	schemeFile(t, f)
	f.rateLimit = true

	c := newTestClient(t, Config{CasURL: f.srv.URL}, nil)
	s := c.NewSource(testFileID, 1, "/refresh")
	err := s.Prepare(t.Context())
	var rl *hfapi.RateLimitError
	if !errors.As(err, &rl) {
		t.Fatalf("want *hfapi.RateLimitError, got %v", err)
	}
	if rl.RetryAfter != 17*time.Second {
		t.Fatalf("RetryAfter = %v, want 17s", rl.RetryAfter)
	}
}

func TestPresigned403ReacquireSucceeds(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	content := schemeFile(t, f)
	f.xorb403Left = 1 // first signed GET 403s; reacquire must mint a fresh URL

	c := newTestClient(t, Config{CasURL: f.srv.URL}, nil)
	s := prepareSource(t, c, testFileID, int64(len(content)))
	got := readOpen(t, s, 0, int64(len(content)))
	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch after reacquire")
	}
	reconHits := 0
	for _, h := range f.hitOrder() {
		if strings.Contains(h, "/reconstructions/") {
			reconHits++
		}
	}
	if reconHits != 2 {
		t.Fatalf("reconstruction hits = %d, want 2 (prepare + 1 reacquire)", reconHits)
	}
}

func TestPresigned403BudgetExhausted(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	content := schemeFile(t, f)
	f.xorb403Left = 100 // every signed GET 403s

	c := newTestClient(t, Config{CasURL: f.srv.URL}, nil)
	s := prepareSource(t, c, testFileID, int64(len(content)))
	r, err := s.Open(t.Context(), 0, int64(len(content)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	_, err = io.ReadAll(r)
	var re *ReacquireError
	if !errors.As(err, &re) {
		t.Fatalf("want ReacquireError, got %v", err)
	}
	if re.Attempts != maxReacquires {
		t.Fatalf("Attempts = %d, want %d", re.Attempts, maxReacquires)
	}
	reconHits := 0
	for _, h := range f.hitOrder() {
		if strings.Contains(h, "/reconstructions/") {
			reconHits++
		}
	}
	if reconHits != 1+maxReacquires {
		t.Fatalf("reconstruction hits = %d, want %d", reconHits, 1+maxReacquires)
	}
}

func TestConcurrentOpens(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	content := schemeFile(t, f)

	c := newTestClient(t, Config{CasURL: f.srv.URL, CacheDir: t.TempDir()}, nil)
	s := prepareSource(t, c, testFileID, int64(len(content)))

	const workers = 8
	span := int64(len(content)) / workers
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			off := int64(i) * span
			ln := span
			if i == workers-1 {
				ln = int64(len(content)) - off
			}
			got := readOpen(t, s, off, ln)
			if !bytes.Equal(got, content[off:off+ln]) {
				errs[i] = fmt.Errorf("worker %d: mismatch at [%d,%d)", i, off, off+ln)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
}

func TestChunkCacheHitAvoidsNetwork(t *testing.T) {
	f := newFixtureCAS(t, testFileID)
	content := schemeFile(t, f)

	c := newTestClient(t, Config{CasURL: f.srv.URL, CacheDir: t.TempDir()}, nil)
	s := prepareSource(t, c, testFileID, int64(len(content)))

	off, ln := int64(0), int64(120_000) // inside xorb 1's fetch ranges
	_ = readOpen(t, s, off, ln)
	before := f.xorbGetCount()
	if before == 0 {
		t.Fatal("first read made no signed-range requests")
	}
	got := readOpen(t, s, off, ln)
	if after := f.xorbGetCount(); after != before {
		t.Fatalf("second read hit network: signed-range GETs %d -> %d", before, after)
	}
	if !bytes.Equal(got, content[off:off+ln]) {
		t.Fatal("cached read mismatch")
	}
}

func TestBoundariesSnapping(t *testing.T) {
	// Terms with file ranges [0,100),[100,250),[250,600).
	mk := func(start, end int64) reconTerm {
		return reconTerm{xorb: "x", unpackedLength: end - start, fileStart: start, fileEnd: end, chunkEnd: 1}
	}
	s := &Source{size: 600}
	s.recon = &reconstruction{terms: []reconTerm{mk(0, 100), mk(100, 250), mk(250, 600)}}

	out := s.Boundaries(nil, 128)
	if out != nil {
		t.Fatalf("nil missing -> %v", out)
	}

	out = s.Boundaries(intervals(0, 600), 128)
	assertNoStraddle := func() {
		t.Helper()
		for _, iv := range out {
			for _, e := range []int64{100, 250} {
				if iv.Start < e && iv.End > e {
					t.Fatalf("interval %v straddles term edge %d", iv, e)
				}
			}
			if iv.End-iv.Start > 128 {
				t.Fatalf("interval %v exceeds blockSize", iv)
			}
		}
	}
	assertNoStraddle()
	// Must cover [0,600) contiguously and carry the term edges.
	if out[0].Start != 0 || out[len(out)-1].End != 600 {
		t.Fatalf("coverage broken: %v", out)
	}
	var edges []int64
	for _, iv := range out[1:] {
		edges = append(edges, iv.Start)
	}
	for _, want := range []int64{100, 250} {
		found := false
		for _, e := range edges {
			if e == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("term edge %d missing from boundaries: %v", want, out)
		}
	}

	// Partial missing interval mid-term snaps BOTH edges to term boundaries:
	// the leading edge of [50,300) drops down to the enclosing term's start
	// (term [0,100) → 0); the trailing edge snaps up from 300 to 600.
	out = s.Boundaries(intervals(50, 300), 128)
	for _, iv := range out {
		for _, e := range []int64{100, 250} {
			if iv.Start < e && iv.End > e {
				t.Fatalf("interval %v straddles term edge %d", iv, e)
			}
		}
	}
	if out[0].Start != 0 || out[len(out)-1].End != 600 {
		t.Fatalf("coverage broken: %v", out)
	}

	out = s.Boundaries(intervals(110, 200), 128)
	if out[0].Start != 100 || out[len(out)-1].End != 250 {
		t.Fatalf("mid-term trailing edge was not snapped: %v", out)
	}
}

func TestBoundariesWithoutRecon(t *testing.T) {
	s := &Source{}
	in := intervals(0, 100)
	out := s.Boundaries(in, 4)
	if len(out) != 1 || out[0] != in[0] {
		t.Fatalf("unprepared Boundaries should pass through, got %v", out)
	}
}

func TestNormalizeRejectsMalformed(t *testing.T) {
	var de *DataError
	// Empty/inverted chunk range.
	if _, err := normalizeV2(&reconstructionV2JSON{
		Terms: []termJSON{{Hash: "aa", UnpackedLength: 10, Range: chunkRangeJSON{Start: 3, End: 3}}},
	}, 0); !errors.As(err, &de) {
		t.Fatalf("empty chunk range: want DataError, got %v", err)
	}
	// Zero unpacked_length.
	if _, err := normalizeV2(&reconstructionV2JSON{
		Terms: []termJSON{{Hash: "aa", UnpackedLength: 0, Range: chunkRangeJSON{Start: 0, End: 2}}},
	}, 0); !errors.As(err, &de) {
		t.Fatalf("zero unpacked_length: want DataError, got %v", err)
	}
	// Fetch info that cannot cover the term's chunk range.
	if _, err := normalizeV2(&reconstructionV2JSON{
		Terms: []termJSON{{Hash: "bb", UnpackedLength: 10, Range: chunkRangeJSON{Start: 0, End: 2}}},
		Xorbs: map[string][]multiRangeFetchJSON{},
	}, 0); !errors.As(err, &de) {
		t.Fatalf("uncovered term: want DataError, got %v", err)
	}
}

func TestGetSignedRangeRejectsInvalid(t *testing.T) {
	c := newTestClient(t, Config{CasURL: "http://unused"}, nil)
	var de *DataError
	// byteEnd < byteStart.
	if _, _, err := c.getSignedRange(t.Context(), fetchRange{url: "http://x", byteStart: 10, byteEnd: 5, chunkStart: 0, chunkEnd: 1}); !errors.As(err, &de) {
		t.Fatalf("inverted byte range: want DataError, got %v", err)
	}
	// Empty chunk range.
	if _, _, err := c.getSignedRange(t.Context(), fetchRange{url: "http://x", byteStart: 0, byteEnd: 5, chunkStart: 2, chunkEnd: 2}); !errors.As(err, &de) {
		t.Fatalf("empty chunk range: want DataError, got %v", err)
	}
}

func TestCacheMaxBytesFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	if got := CacheMaxBytesFromEnv(env(nil)); got != DefaultCacheMaxBytes {
		t.Fatalf("unset: got %d, want default %d", got, DefaultCacheMaxBytes)
	}
	if got := CacheMaxBytesFromEnv(env(map[string]string{EnvChunkCacheSize: "12345"})); got != 12345 {
		t.Fatalf("valid: got %d, want 12345", got)
	}
	for _, bad := range []string{"", "  ", "0", "-5", "abc"} {
		if got := CacheMaxBytesFromEnv(env(map[string]string{EnvChunkCacheSize: bad})); got != DefaultCacheMaxBytes {
			t.Fatalf("bad %q: got %d, want default", bad, got)
		}
	}
	if got := CacheMaxBytesFromEnv(nil); got != DefaultCacheMaxBytes {
		t.Fatalf("nil getenv: got %d, want default", got)
	}
}
