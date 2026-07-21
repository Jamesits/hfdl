package transfer

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
)

// newTestHTTPSource builds an httpSource against the given upstream URLs.
func newTestHTTPSource(task *FileTask, seed [32]byte) *httpSource {
	return newHTTPSource(slog.New(slog.DiscardHandler), &http.Client{}, task, 2*time.Second, seed)
}

func testSeed(b byte) [32]byte {
	var s [32]byte
	s[0] = b
	return s
}

// TestOpenValid206 checks the happy path and the response identity pin.
func TestOpenValid206(t *testing.T) {
	content := newContent(1, 300000)
	fx := newFixture(t, content, "blob-x")
	srv := fx.start()
	task := taskFor(nil, int64(len(content)), srv.URL)
	task.BlobID = "blob-x"
	src := newTestHTTPSource(task, testSeed(1))

	body, err := src.Open(t.Context(), 1000, 5000)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer body.Close()
	if got := upstreamOf(body); got != srv.URL {
		t.Fatalf("upstream %q, want %q", got, srv.URL)
	}
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(content[1000:6000]) {
		t.Fatalf("content mismatch: got %d bytes", len(got))
	}
	if src.pins[srv.URL] != "blob-x" {
		t.Fatalf("pin %q, want blob-x", src.pins[srv.URL])
	}
}

// TestValidate206Failures: every ranged-response invariant violation is a
// validation AttemptError (drop + requeue + penalize at the worker layer).
func TestValidate206Failures(t *testing.T) {
	content := newContent(2, 100000)
	cases := []struct {
		name   string
		mutate func(w http.ResponseWriter, off, end int64)
	}{
		{"wrong start", func(w http.ResponseWriter, off, end int64) {
			w.Header().Set("Content-Range", "bytes 0-9/100000")
		}},
		{"wrong end", func(w http.ResponseWriter, off, end int64) {
			w.Header().Set("Content-Range", "bytes 1000-1999/100000")
		}},
		{"wrong total", func(w http.ResponseWriter, off, end int64) {
			w.Header().Set("Content-Range", "bytes 1000-1099/99999")
		}},
		{"star total", func(w http.ResponseWriter, off, end int64) {
			w.Header().Set("Content-Range", "bytes 1000-1099/*")
		}},
		{"unparseable", func(w http.ResponseWriter, off, end int64) {
			w.Header().Set("Content-Range", "bananas")
		}},
		{"gzip encoding", func(w http.ResponseWriter, off, end int64) {
			w.Header().Set("Content-Encoding", "gzip")
		}},
		{"wrong content-length", func(w http.ResponseWriter, off, end int64) {
			w.Header().Set("Content-Length", "42")
		}},
		{"etag mismatch", func(w http.ResponseWriter, off, end int64) {
			w.Header().Set("ETag", "someone-else")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t, content, "blob-y")
			fx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
				tc.mutate(w, off, end)
				if w.Header().Get("Content-Encoding") == "" {
					// keep default range headers unless mutated
					if w.Header().Get("Content-Range") == "" {
						w.Header().Set("Content-Range", "bytes 1000-1099/100000")
					}
					if w.Header().Get("ETag") == "" {
						w.Header().Set("ETag", "blob-y")
					}
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(content[off : end+1])
				} else {
					w.Header().Set("Content-Range", "bytes 1000-1099/100000")
					w.Header().Set("ETag", "blob-y")
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(content[off : end+1])
				}
				return true
			}
			srv := fx.start()
			task := taskFor(nil, int64(len(content)), srv.URL)
			task.BlobID = "blob-y"
			src := newTestHTTPSource(task, testSeed(2))
			_, err := src.Open(t.Context(), 1000, 100)
			if err == nil {
				t.Fatal("expected validation error")
			}
			var ae *AttemptError
			if !errors.As(err, &ae) || ae.Kind != FailValidation {
				t.Fatalf("got %v, want AttemptError{FailValidation}", err)
			}
			if ae.Upstream != srv.URL {
				t.Fatalf("upstream %q, want %q", ae.Upstream, srv.URL)
			}
		})
	}
}

// TestIdentityMismatchAfterPin: once (upstream,file) identity is pinned, a
// changed ETag excludes the mirror for the file — it is never picked again.
func TestIdentityMismatchAfterPin(t *testing.T) {
	content := newContent(3, 100000)
	bad := newFixture(t, content, "blob-z")
	bad.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		etag := "blob-z"
		if n > 0 { // second response flips identity
			etag = "impostor"
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Range", "bytes 1000-1099/100000")
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[off : end+1])
		return true
	}
	badSrv := bad.start()
	good := newFixture(t, content, "blob-z")
	goodSrv := good.start()

	task := taskFor(nil, int64(len(content)), badSrv.URL, goodSrv.URL)
	task.BlobID = "blob-z"
	task.Policy = config.RoundRobin // guarantees the bad mirror is revisited
	src := newTestHTTPSource(task, testSeed(3))

	ctx := t.Context()
	if _, err := src.Open(ctx, 1000, 100); err != nil { // pick 1: bad, pins blob-z
		t.Fatalf("first open: %v", err)
	}
	if _, err := src.Open(ctx, 1000, 100); err != nil { // pick 2 (RR): good, pins blob-z
		t.Fatalf("second open: %v", err)
	}
	_, err := src.Open(ctx, 1000, 100) // pick 3 (RR): bad again, identity flipped
	if err == nil {
		t.Fatal("expected identity mismatch")
	}
	var ae *AttemptError
	if !errors.As(err, &ae) || ae.Kind != FailValidation {
		t.Fatalf("got %v, want AttemptError{FailValidation}", err)
	}
	if !src.excluded[badSrv.URL] {
		t.Fatal("bad mirror not excluded")
	}
	// Every later Open must come from the good mirror.
	for i := 0; i < 4; i++ {
		body, err := src.Open(ctx, 1000, 100)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if up := upstreamOf(body); up != goodSrv.URL {
			t.Fatalf("open %d served by excluded mirror %q", i, up)
		}
		_ = body.Close()
	}
}

// TestRangelessMarkAndFallback: 200 to a ranged request marks the upstream;
// when all upstreams are rangeless Open reports errAllRangeless, and the
// whole-file fallback stream works.
func TestRangelessMarkAndFallback(t *testing.T) {
	content := newContent(4, 50000)
	rangelessFx := newFixture(t, content, "blob-r")
	rangelessFx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		// pretend ranges are unsupported: full body, 200
		w.Header().Set("ETag", "blob-r")
		w.Header().Set("Content-Length", "50000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
		return true
	}
	rangelessSrv := rangelessFx.start()
	goodFx := newFixture(t, content, "blob-r")
	goodSrv := goodFx.start()

	ctx := t.Context()

	// Mixed: rangeless upstream is skipped after marking.
	task := taskFor(nil, int64(len(content)), rangelessSrv.URL, goodSrv.URL)
	task.BlobID = "blob-r"
	task.Policy = config.RoundRobin
	src := newTestHTTPSource(task, testSeed(4))
	_, err := src.Open(ctx, 0, 1000)
	var ae *AttemptError
	if !errors.As(err, &ae) || ae.Kind != FailRangeless {
		t.Fatalf("got %v, want AttemptError{FailRangeless}", err)
	}
	body, err := src.Open(ctx, 0, 1000)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if up := upstreamOf(body); up != goodSrv.URL {
		t.Fatalf("rangeless upstream reused: %q", up)
	}
	_ = body.Close()

	// All rangeless: errAllRangeless, then openWhole serves the full object.
	task2 := taskFor(nil, int64(len(content)), rangelessSrv.URL)
	task2.BlobID = "blob-r"
	src2 := newTestHTTPSource(task2, testSeed(5))
	_, err = src2.Open(ctx, 0, 1000)
	if !errors.Is(err, errAllRangeless) {
		t.Fatalf("got %v, want errAllRangeless", err)
	}
	whole, err := src2.Open(ctx, 0, int64(len(content))) // fallback path
	if err != nil {
		t.Fatalf("fallback open: %v", err)
	}
	got, err := io.ReadAll(whole)
	_ = whole.Close()
	if err != nil {
		t.Fatalf("fallback read: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("fallback content mismatch: %d bytes", len(got))
	}
	// The fallback GET must be unranged.
	for _, q := range rangelessFx.requestsSnapshot() {
		if q.off < 0 {
			return
		}
	}
	t.Fatal("no unranged fallback request logged")
}

// TestOpen416 surfaces the typed 416 error.
func TestOpen416(t *testing.T) {
	content := newContent(5, 1000)
	fx := newFixture(t, content, "blob-s")
	srv := fx.start()
	task := taskFor(nil, 1000, srv.URL)
	task.BlobID = "blob-s"
	src := newTestHTTPSource(task, testSeed(6))
	_, err := src.Open(t.Context(), 5000, 100) // out of range
	var rns *rangeNotSatisfiableError
	if !errors.As(err, &rns) {
		t.Fatalf("got %v, want *rangeNotSatisfiableError", err)
	}
}

// TestShortBody: correct headers but an early EOF is detected at read time
// (the stream layer counts bytes); Open itself succeeds.
func TestShortBody(t *testing.T) {
	content := newContent(6, 100000)
	fx := newFixture(t, content, "blob-b")
	fx.beforeRange = func(w http.ResponseWriter, r *http.Request, off, end int64, n int) bool {
		w.Header().Set("ETag", "blob-b")
		w.Header().Set("Content-Range", "bytes 0-999/100000")
		w.WriteHeader(http.StatusPartialContent)
		// Flush headers before the body so the server cannot coalesce the
		// short body into a Content-Length (which would fail validation at
		// Open instead of at read time).
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write(content[:500])
		return true
	}
	srv := fx.start()
	task := taskFor(nil, int64(len(content)), srv.URL)
	task.BlobID = "blob-b"
	src := newTestHTTPSource(task, testSeed(7))
	body, err := src.Open(t.Context(), 0, 1000)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 500 {
		t.Fatalf("got %d bytes, want 500 (short)", len(got))
	}
}

// TestRedirectStripsAuthorization cross-host: transfer pins a CheckRedirect
// on its client clone that drops Authorization when the host changes.
func TestRedirectStripsAuthorization(t *testing.T) {
	content := newContent(7, 100000)
	var gotAuth string
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("ETag", "blob-r")
		w.Header().Set("Content-Range", "bytes 0-99/100000")
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[:100])
	}))
	defer final.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL+r.URL.Path, http.StatusFound)
	}))
	defer redirector.Close()

	task := taskFor(nil, int64(len(content)), redirector.URL)
	task.BlobID = "blob-r"
	src := newTestHTTPSource(task, testSeed(8))
	// Simulate an auth-carrying request context: the redirect hop must strip it.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, src.resolveURL(redirector.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-99")
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := src.hc.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization leaked cross-host: %q", gotAuth)
	}
}

// TestHeaderTimeout: headers delayed beyond the deadline are a retriable
// FailHeaderTimeout.
func TestHeaderTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.Header().Set("Content-Range", "bytes 0-99/1000")
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(make([]byte, 100))
	}))
	defer slow.Close()
	task := taskFor(nil, 1000, slow.URL)
	src := newHTTPSource(slog.New(slog.DiscardHandler), &http.Client{}, task, 50*time.Millisecond, testSeed(9))
	_, err := src.Open(t.Context(), 0, 100)
	var ae *AttemptError
	if !errors.As(err, &ae) || ae.Kind != FailHeaderTimeout {
		t.Fatalf("got %v, want AttemptError{FailHeaderTimeout}", err)
	}
}

// TestTerminalStatuses: 401/403/404 are terminal, 5xx retriable.
func TestStatusClassification(t *testing.T) {
	for _, tc := range []struct {
		status   int
		terminal bool
	}{
		{401, true}, {403, true}, {404, true}, {500, false}, {502, false},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
		}))
		task := taskFor(nil, 1000, srv.URL)
		src := newTestHTTPSource(task, testSeed(byte(tc.status)))
		_, err := src.Open(t.Context(), 0, 100)
		if tc.terminal {
			var te *TerminalHTTPError
			if !errors.As(err, &te) || te.StatusCode != tc.status {
				t.Fatalf("status %d: got %v, want TerminalHTTPError", tc.status, err)
			}
		} else {
			var ae *AttemptError
			if !errors.As(err, &ae) || ae.Kind != FailStatus {
				t.Fatalf("status %d: got %v, want AttemptError{FailStatus}", tc.status, err)
			}
		}
		srv.Close()
	}
}
