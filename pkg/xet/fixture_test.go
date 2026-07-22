package xet

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/pierrec/lz4/v4"
)

// Test fixtures: synthetic xorbs built with the real wire formats (LZ4
// frame, BG4 transpose + LZ4 frame, 8-byte chunk headers), served by an
// httptest CAS that enforces the protocol invariants (exact authorized
// Range headers, bearer checks) so the tests fail on any deviation.

// bg4Split is the encoder-side BG4 transpose — port of xet-core
// byte_grouping/bg4.rs bg4_split_together (inverse of bg4Regroup).
func bg4Split(data []byte) []byte {
	n := len(data)
	split := n / 4
	rem := n % 4
	s0 := split + min(rem, 1)
	s1 := split + min(max(rem-1, 0), 1)
	s2 := split + min(max(rem-2, 0), 1)
	out := make([]byte, n)
	d0 := out[:s0]
	d1 := out[s0 : s0+s1]
	d2 := out[s0+s1 : s0+s1+s2]
	d3 := out[s0+s1+s2:]
	for i := range split {
		d0[i] = data[4*i]
		d1[i] = data[4*i+1]
		d2[i] = data[4*i+2]
		d3[i] = data[4*i+3]
	}
	switch rem {
	case 1:
		d0[split] = data[4*split]
	case 2:
		d0[split] = data[4*split]
		d1[split] = data[4*split+1]
	case 3:
		d0[split] = data[4*split]
		d1[split] = data[4*split+1]
		d2[split] = data[4*split+2]
	}
	return out
}

// compressChunk encodes one chunk with a scheme (no auto-fallback; fixtures
// force the scheme under test).
func compressChunk(tb testing.TB, scheme int, data []byte) []byte {
	tb.Helper()
	switch scheme {
	case schemeNone:
		return data
	case schemeLZ4:
		var buf bytes.Buffer
		w := lz4.NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			tb.Fatal(err)
		}
		if err := w.Close(); err != nil {
			tb.Fatal(err)
		}
		return buf.Bytes()
	case schemeBG4LZ4:
		return compressChunk(tb, schemeLZ4, bg4Split(data))
	default:
		tb.Fatalf("bad scheme %d", scheme)
		return nil
	}
}

// serializeChunk frames one chunk with the 8-byte xorb chunk header.
func serializeChunk(tb testing.TB, scheme int, data []byte) []byte {
	tb.Helper()
	packed := compressChunk(tb, scheme, data)
	if len(packed) >= 1<<24 || len(data) >= 1<<24 {
		tb.Fatal("chunk too large for 3-byte length")
	}
	h := make([]byte, chunkHeaderLen)
	h[0] = 0 // version
	h[1] = byte(len(packed))
	h[2] = byte(len(packed) >> 8)
	h[3] = byte(len(packed) >> 16)
	h[4] = byte(scheme)
	h[5] = byte(len(data))
	h[6] = byte(len(data) >> 8)
	h[7] = byte(len(data) >> 16)
	return append(h, packed...)
}

// xorbSpec is a fully serialized synthetic xorb.
type xorbSpec struct {
	hash       string
	chunks     [][]byte // decoded contents
	schemes    []int
	serialized []byte
	// chunkOff[i] = byte offset of chunk i's header within serialized.
	chunkOff []int64
}

func buildXorb(tb testing.TB, hash string, chunks [][]byte, schemes []int) *xorbSpec {
	tb.Helper()
	x := &xorbSpec{hash: hash, chunks: chunks, schemes: schemes}
	for i, c := range chunks {
		x.chunkOff = append(x.chunkOff, int64(len(x.serialized)))
		x.serialized = append(x.serialized, serializeChunk(tb, schemes[i], c)...)
	}
	return x
}

// byteRangeOf returns the inclusive byte range covering chunk range
// [start, end) of the serialized xorb.
func (x *xorbSpec) byteRangeOf(start, end uint32) (int64, int64) {
	s := x.chunkOff[start]
	var e int64
	if int(end) < len(x.chunkOff) {
		e = x.chunkOff[end] - 1
	} else {
		e = int64(len(x.serialized)) - 1
	}
	return s, e
}

// deterministicBytes returns pseudo-random bytes seeded from n (stable
// across test runs without fixed testdata).
func deterministicBytes(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		// xorshift-ish mix; compressible enough for LZ4 to not bail out,
		// varied enough to catch transpose mistakes.
		v := byte(i>>3) ^ seed
		b[i] = v + byte(i>>11)
	}
	// sprinkle high-entropy sections
	var rnd [64]byte
	_, _ = rand.Read(rnd[:])
	for i := 0; i+64 <= n; i += 4096 {
		copy(b[i:i+64], rnd[:])
	}
	return b
}

// fixtureCAS is a scriptable CAS + signed-range server.
type fixtureCAS struct {
	t   *testing.T
	srv *httptest.Server

	mu sync.Mutex
	// hits records "METHOD path" for every request, in order.
	hits []string
	// reconRange is the Range header of the most recent reconstruction hit.
	reconRange string
	// xorbHits counts signed-range GETs per xorb hash.
	xorbHits map[string]int

	v2Status    int    // override status for /v2/ (0 = serve 200)
	wantToken   string // bearer the CAS demands ("good" by default in tests)
	rateLimit   bool   // reconstruction answers 429
	xorb403Left int    // remaining 403s to serve on signed-range GETs
	urlGen      int    // generation counter mixed into signed URLs
	latestGen   int    // generation the xorb handler accepts

	fileID string
	terms  []termJSON
	xorbs  map[string]*xorbSpec
	// entrySplit splits the last xorb's fetch entry into two chunk ranges
	// (exercises coverRanges + v2 multi-range entries).
	entrySplit bool
	// offsetFirst is served as offset_into_first_range (partial tests).
	offsetFirst int64
	// lieUnpacked inflates every term's unpacked_length by one.
	lieUnpacked bool
}

func newFixtureCAS(t *testing.T, fileID string) *fixtureCAS {
	t.Helper()
	f := &fixtureCAS{
		t:         t,
		wantToken: "good",
		fileID:    fileID,
		xorbs:     make(map[string]*xorbSpec),
		xorbHits:  make(map[string]int),
		latestGen: 1,
		urlGen:    1,
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fixtureCAS) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.hits = append(f.hits, r.Method+" "+r.URL.Path)
	f.mu.Unlock()
	switch {
	case strings.HasPrefix(r.URL.Path, "/v2/reconstructions/"):
		f.handleRecon(w, r, 2)
	case strings.HasPrefix(r.URL.Path, "/v1/reconstructions/"):
		f.handleRecon(w, r, 1)
	case strings.HasPrefix(r.URL.Path, "/xorb/"):
		f.handleXorb(w, r, strings.TrimPrefix(r.URL.Path, "/xorb/"))
	default:
		http.NotFound(w, r)
	}
}

// hitOrder returns paths hit, for fallback-order assertions.
func (f *fixtureCAS) hitOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.hits...)
}

func (f *fixtureCAS) xorbGetCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.xorbHits {
		n += c
	}
	return n
}

// lastRangeHeader returns the Range header of the most recent
// reconstruction request.
func (f *fixtureCAS) lastRangeHeader() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reconRange
}

// reconEntries builds the (v2-normalized) fetch entries for one xorb:
// either a single entry covering all chunks, or two entries over one URL.
func (f *fixtureCAS) reconEntries(x *xorbSpec) []rangeDescriptorJSON {
	n := uint32(len(x.chunks))
	var splits [][2]uint32
	if f.entrySplit && n >= 4 {
		splits = [][2]uint32{{0, n / 2}, {n / 2, n}}
	} else {
		splits = [][2]uint32{{0, n}}
	}
	var out []rangeDescriptorJSON
	for _, sp := range splits {
		s, e := x.byteRangeOf(sp[0], sp[1])
		out = append(out, rangeDescriptorJSON{
			Chunks: chunkRangeJSON{Start: sp[0], End: sp[1]},
			Bytes:  byteRangeJSON{Start: s, End: e},
		})
	}
	return out
}

func (f *fixtureCAS) handleRecon(w http.ResponseWriter, r *http.Request, version int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconRange = r.Header.Get("Range")
	if f.rateLimit {
		w.Header().Set("Retry-After", "17")
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	if got := r.Header.Get("Authorization"); got != "Bearer "+f.wantToken {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if version == 2 && f.v2Status != 0 && f.v2Status != http.StatusOK {
		w.WriteHeader(f.v2Status)
		return
	}
	// Every reconstruction serve issues a fresh URL generation and
	// invalidates the previous one — like real presigned-URL rotation.
	f.urlGen++
	f.latestGen = f.urlGen
	terms := make([]termJSON, len(f.terms))
	copy(terms, f.terms)
	if f.lieUnpacked {
		for i := range terms {
			terms[i].UnpackedLength++
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if version == 2 {
		resp := reconstructionV2JSON{
			OffsetIntoFirstRange: f.offsetFirst,
			Terms:                terms,
			Xorbs:                make(map[string][]multiRangeFetchJSON),
		}
		for hash, x := range f.xorbs {
			resp.Xorbs[hash] = []multiRangeFetchJSON{{
				URL:    fmt.Sprintf("%s/xorb/%s?g=%d", f.srv.URL, hash, f.urlGen),
				Ranges: f.reconEntries(x),
			}}
		}
		writeJSON(f.t, w, resp)
		return
	}
	resp := reconstructionV1JSON{
		OffsetIntoFirstRange: f.offsetFirst,
		Terms:                terms,
		FetchInfo:            make(map[string][]fetchInfoJSON),
	}
	for hash, x := range f.xorbs {
		for _, d := range f.reconEntries(x) {
			resp.FetchInfo[hash] = append(resp.FetchInfo[hash], fetchInfoJSON{
				Range:    d.Chunks,
				URL:      fmt.Sprintf("%s/xorb/%s?g=%d", f.srv.URL, hash, f.urlGen),
				URLRange: d.Bytes,
			})
		}
	}
	writeJSON(f.t, w, resp)
}

func (f *fixtureCAS) handleXorb(w http.ResponseWriter, r *http.Request, hash string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.xorbHits[hash]++
	if f.xorb403Left > 0 {
		f.xorb403Left--
		w.WriteHeader(http.StatusForbidden)
		return
	}
	// Only the latest URL generation is valid; stale URLs 403 (the
	// reacquire tests bump latestGen and rewrite urls via urlGen).
	if r.URL.Query().Get("g") != fmt.Sprint(f.latestGen) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	x, ok := f.xorbs[hash]
	if !ok {
		http.NotFound(w, r)
		return
	}
	// The Range header must match one of the authorized entries exactly.
	rh := r.Header.Get("Range")
	authorized := false
	var s, e int64
	for _, d := range f.reconEntries(x) {
		if rh == fmt.Sprintf("bytes=%d-%d", d.Bytes.Start, d.Bytes.End) {
			authorized, s, e = true, d.Bytes.Start, d.Bytes.End
			break
		}
	}
	if !authorized {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e, len(x.serialized)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(x.serialized[s : e+1])
}

func writeJSON(tb testing.TB, w http.ResponseWriter, v any) {
	tb.Helper()
	if err := json.NewEncoder(w).Encode(v); err != nil {
		tb.Fatal(err)
	}
}
