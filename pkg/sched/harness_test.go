package sched

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/cache"
	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/stats"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/throttle"
	"github.com/jamesits/hfdl/pkg/transfer"
	"github.com/jamesits/hfdl/pkg/verify"
)

// fixtureFile is one repo file served by the fake Hub.
type fixtureFile struct {
	content []byte
	lfs     bool
}

func (f *fixtureFile) blobID() string {
	if f.lfs {
		sum := sha256.Sum256(f.content)
		return hex.EncodeToString(sum[:])
	}
	return gitBlobSHA1(f.content)
}

func gitBlobSHA1(content []byte) string {
	h := sha1.New()
	_, _ = fmt.Fprintf(h, "blob %d\x00", len(content))
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

// fixtureHub is a fake Hugging Face Hub: revision pin, recursive tree,
// paths-info and resolve with proper 206 semantics. Every request is
// counted so tests can assert exactly what the manager asked for.
type fixtureHub struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	repo     string // the one repo this Hub knows (default org/repo)
	files    map[string]*fixtureFile
	sha      string
	hits     map[string]int // route kind → count ("tree","paths-info","revision","resolve")
	resolveN map[string]int // repo path → resolve request count
	bytesOut int64          // payload bytes served by resolve
	active   map[string]int // in-flight resolve requests per path
	maxPaths int            // high-water of concurrently active distinct paths
	maxSame  int            // high-water of concurrent requests to one path

	// failure injection
	tree429Left  int            // number of 429s to emit for tree
	resolveWrong map[string]int // per path: serve wrongBytes for the next N resolve requests
	delay        time.Duration  // per-request resolve delay (resume/parallelism tests)
	hangLeft     map[string]int // per path: next N resolve GETs block until request ctx cancel
}

func newFixtureHub(t *testing.T, files map[string][]byte, lfsPaths ...string) *fixtureHub {
	t.Helper()
	h := &fixtureHub{
		t:            t,
		repo:         "org/repo",
		files:        make(map[string]*fixtureFile),
		sha:          "0123456789abcdef0123456789abcdef01234567",
		hits:         map[string]int{},
		resolveN:     map[string]int{},
		resolveWrong: map[string]int{},
		active:       map[string]int{},
		hangLeft:     map[string]int{},
	}
	lfs := map[string]bool{}
	for _, p := range lfsPaths {
		lfs[p] = true
	}
	for p, c := range files {
		h.files[p] = &fixtureFile{content: c, lfs: lfs[p]}
	}
	// Route manually: ServeMux wildcards for org/repo conflict between the
	// /api/models/... and /{org}/{repo}/resolve/... shapes.
	h.srv = httptest.NewServer(http.HandlerFunc(h.route))
	t.Cleanup(h.srv.Close)
	return h
}

// route dispatches:
//
//	GET  /api/models/{org}/{repo}/revision/{rev}
//	GET  /api/models/{org}/{repo}/tree/{rev}
//	POST /api/models/{org}/{repo}/paths-info/{rev}
//	GET  /{org}/{repo}/resolve/{rev}/{path...}
func (h *fixtureHub) route(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	if rest, ok := strings.CutPrefix(p, "api/models/"); ok {
		parts := strings.SplitN(rest, "/", 4)
		if len(parts) == 4 {
			switch {
			case r.Method == http.MethodGet && parts[2] == "revision":
				h.handleRevision(w, r)
				return
			case r.Method == http.MethodGet && parts[2] == "tree":
				h.handleTree(w, r)
				return
			case r.Method == http.MethodPost && parts[2] == "paths-info":
				h.handlePathsInfo(w, r)
				return
			}
		}
		http.NotFound(w, r)
		return
	}
	// resolve: {org}/{repo}/resolve/{rev}/{path...}
	parts := strings.SplitN(p, "/", 5)
	if len(parts) == 5 && parts[2] == "resolve" && r.Method == http.MethodGet {
		r.SetPathValue("path", parts[4])
		h.handleResolve(w, r)
		return
	}
	http.NotFound(w, r)
}

func (h *fixtureHub) hit(kind string) {
	h.mu.Lock()
	h.hits[kind]++
	h.mu.Unlock()
}

func (h *fixtureHub) hitsFor(kind string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hits[kind]
}

func (h *fixtureHub) resolveHits(path string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.resolveN[path]
}

func (h *fixtureHub) payloadBytes() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.bytesOut
}

// maxConcurrentPaths reports the high-water mark of files being downloaded
// at the same time (distinct paths with in-flight resolve requests).
func (h *fixtureHub) maxConcurrentPaths() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxPaths
}

// maxConcurrentSamePath reports the high-water mark of in-flight resolve
// requests to a single path — i.e. the peak per-file block-download
// concurrency the server actually saw. It is a durable latch, so a test can
// assert on it after the run without racing a transient gauge.
func (h *fixtureHub) maxConcurrentSamePath() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxSame
}

func (h *fixtureHub) repo404(w http.ResponseWriter, r *http.Request) bool {
	p := strings.TrimPrefix(r.URL.Path, "/")
	p = strings.TrimPrefix(p, "api/models/")
	if !strings.HasPrefix(p, h.repo+"/") {
		http.NotFound(w, r)
		return true
	}
	return false
}

func (h *fixtureHub) handleRevision(w http.ResponseWriter, r *http.Request) {
	h.hit("revision")
	if h.repo404(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"sha": h.sha})
}

func (h *fixtureHub) entryJSON(path string, f *fixtureFile) map[string]any {
	e := map[string]any{
		"type": "file",
		"path": path,
		"size": len(f.content),
		"oid":  gitBlobSHA1(f.content),
	}
	if f.lfs {
		sum := sha256.Sum256(f.content)
		e["lfs"] = map[string]any{"oid": hex.EncodeToString(sum[:]), "size": len(f.content)}
	}
	return e
}

func (h *fixtureHub) handleTree(w http.ResponseWriter, r *http.Request) {
	h.hit("tree")
	if h.repo404(w, r) {
		return
	}
	h.mu.Lock()
	if h.tree429Left > 0 {
		h.tree429Left--
		h.mu.Unlock()
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	h.mu.Unlock()
	var entries []map[string]any
	for p, f := range h.files {
		entries = append(entries, h.entryJSON(p, f))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entries)
}

func (h *fixtureHub) handlePathsInfo(w http.ResponseWriter, r *http.Request) {
	h.hit("paths-info")
	if h.repo404(w, r) {
		return
	}
	var req struct {
		Paths []string `json:"paths"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	var entries []map[string]any
	for _, p := range req.Paths {
		if f, ok := h.files[p]; ok {
			entries = append(entries, h.entryJSON(p, f))
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entries)
}

// handleResolve serves ranged GETs with strict 206 semantics (ServeContent)
// plus failure injection: per-path "wrong bytes" rounds and an optional
// per-request delay.
func (h *fixtureHub) handleResolve(w http.ResponseWriter, r *http.Request) {
	path := r.PathValue("path")
	h.mu.Lock()
	delay := h.delay
	wrong := h.resolveWrong[path]
	if wrong > 0 {
		h.resolveWrong[path]--
	}
	hang := h.hangLeft[path]
	if hang > 0 {
		h.hangLeft[path]--
	}
	h.resolveN[path]++
	h.hits["resolve"]++
	content := []byte(nil)
	if f, ok := h.files[path]; ok {
		content = f.content
	}
	h.mu.Unlock()

	if content == nil {
		http.NotFound(w, r)
		return
	}
	if hang > 0 {
		// Mid-body blackhole: exact 206 headers for the requested range,
		// a partial prefix, then silence — the idle-read deadline (stall
		// L2) kills the attempt mid-stream, a real EventStall. A header
		// blackhole classifies as FailHeaderTimeout instead.
		var start, end int64
		if n, _ := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); n != 2 {
			start, end = 0, int64(len(content))-1
		}
		length := end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.Header().Set("Content-Encoding", "identity")
		// The response must still prove identity: a ranged 206 without a
		// validator is a validation failure (never streamed), so a mid-body
		// blackhole that omitted the ETag would be rejected before it could
		// stall. Carry the matching blob ETag so this streams then stalls.
		if f, ok := h.files[path]; ok {
			w.Header().Set("ETag", `"`+f.blobID()+`"`)
		}
		w.WriteHeader(http.StatusPartialContent)
		prefix := content[start:min(int64(len(content)), start+min(length, 16<<10))]
		_, _ = w.Write(prefix)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
		return
	}
	h.mu.Lock()
	h.active[path]++
	if len(h.active) > h.maxPaths {
		h.maxPaths = len(h.active)
	}
	if h.active[path] > h.maxSame {
		h.maxSame = h.active[path]
	}
	h.mu.Unlock()
	defer func() {
		time.Sleep(5 * time.Millisecond) // widen the overlap window
		h.mu.Lock()
		if h.active[path] == 1 {
			delete(h.active, path)
		} else {
			h.active[path]--
		}
		h.mu.Unlock()
	}()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if wrong > 0 {
		content = bytes.Repeat([]byte{0xAB}, len(content))
	}

	// Count payload bytes from the Range header (ServeContent writes them).
	if rng := r.Header.Get("Range"); rng != "" {
		var start, end int64
		if n, _ := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); n == 2 {
			h.mu.Lock()
			h.bytesOut += end - start + 1
			h.mu.Unlock()
		}
	} else {
		h.mu.Lock()
		h.bytesOut += int64(len(content))
		h.mu.Unlock()
	}

	f := h.files[path]
	w.Header().Set("ETag", `"`+f.blobID()+`"`)
	http.ServeContent(w, r, "file", time.Now(), bytes.NewReader(content))
}

// logCapture is a slog.Handler that records (level, message) pairs for
// assertions on worker log output.
type logCapture struct {
	mu   sync.Mutex
	recs []capturedRecord
}

type capturedRecord struct {
	level slog.Level
	msg   string
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	c.recs = append(c.recs, capturedRecord{r.Level, r.Message})
	c.mu.Unlock()
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

// has reports whether a record with the given level (or higher) containing
// substr was captured.
func (c *logCapture) has(level slog.Level, substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.recs {
		if r.level >= level && strings.Contains(r.msg, substr) {
			return true
		}
	}
	return false
}

// newTestEnv builds the env with the default (error-level, or debug with
// HFDL_TEST_DEBUG) logger.
func newTestEnv(t *testing.T, hub *fixtureHub, opts ...envOption) *testEnv {
	t.Helper()
	logLevel := slog.LevelError
	if os.Getenv("HFDL_TEST_DEBUG") != "" {
		logLevel = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(testLogWriter{t}, &slog.HandlerOptions{Level: logLevel}))
	return newTestEnvLog(t, hub, log, opts...)
}

// testEnv bundles a fully wired real stack (store/fcio/cache/verify/
// transfer/manager) against a fixture Hub — the integration-style unit
// test this package exists for.
type testEnv struct {
	t       *testing.T
	dir     string
	cache   *cache.Store
	st      *store.Store
	hub     *fixtureHub
	manager *Manager
	bw      *throttle.Bucket
	api     *throttle.Bucket
	duty    *throttle.DutyLimiter
	reg     *stats.Registry
	dl      *transfer.Downloader
	limits  config.Limits
}

type envOption func(*testEnv)

func withLimits(mut func(*config.Limits)) envOption {
	return func(e *testEnv) { mut(&e.limits) }
}

// newTestEnvLog is newTestEnv with a caller-supplied logger (log-output
// assertions via logCapture).
func newTestEnvLog(t *testing.T, hub *fixtureHub, log *slog.Logger, opts ...envOption) *testEnv {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()

	st, err := store.Open(ctx, filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close(context.WithoutCancel(ctx)) })

	engine := fcio.NewEngine(log, st, fcio.TierAuto)
	pool := fcio.NewPool(0, 64<<20)
	volumes := fcio.NewVolumeSet()
	cacheRoot := filepath.Join(dir, "hf-cache")
	cs, err := cache.OpenStore(ctx, cacheRoot, engine, log)
	if err != nil {
		t.Fatalf("cache.OpenStore: %v", err)
	}

	bw := throttle.NewBucket(0, 0, 0) // unlimited
	api := throttle.NewBucket(1000, 1000, 1000)
	duty := throttle.NewDutyLimiter(100, throttle.MediaSSD)
	reg := stats.New()
	dl := transfer.NewDownloader(transfer.Config{
		Log: log, HTTP: hub.srv.Client(),
		Bandwidth: bw, Stats: reg, Engine: engine, Pool: pool,
		CheckpointInterval: 50 * time.Millisecond,
		HeaderTimeout:      5 * time.Second,
		// Collapse the retry pacing/blacklist so a single-upstream stall or
		// timeout recovers in milliseconds instead of waiting out the 5s
		// blacklist plus exponential backoff (CI wall-clock).
		RetryBackoffBase:     5 * time.Millisecond,
		UpstreamBlacklistTTL: 20 * time.Millisecond,
	})
	verifier := verify.NewChecker(engine, pool, duty, log, nil)
	installer := cache.NewInstaller(cs, engine, volumes, duty, log, nil)

	limits := config.DefaultLimits()
	limits.Conns = 4
	limits.MaxWorkers = 2
	limits.BlockSize = 1 << 20 // 1MiB blocks: multi-block even for small files

	e := &testEnv{
		t: t, dir: dir, cache: cs, st: st, hub: hub,
		bw: bw, api: api, duty: duty, reg: reg, dl: dl, limits: limits,
	}
	for _, o := range opts {
		o(e)
	}

	client := hfapi.NewClient(log, hub.srv.Client(), hub.srv.URL, "", 5*time.Second, 5*time.Second)
	m := NewManager(ManagerConfig{
		Store:      st,
		Clients:    map[string]*hfapi.Client{hub.srv.URL: client},
		Downloader: dl,
		Verifier:   verifier,
		Installer:  installer,
		Cache:      cs,
		Engine:     engine,
		Pool:       pool,
		Volumes:    volumes,
		Bandwidth:  bw,
		API:        api,
		Duty:       duty,
		Stats:      reg,
		Log:        log,
		Limits:     e.limits,
		CacheDir:   cacheRoot,
	})
	e.manager = m
	return e
}

// rebuild constructs a second manager over the same DB (restart flow).
func (e *testEnv) rebuild(t *testing.T) *Manager {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	if os.Getenv("HFDL_TEST_DEBUG") != "" {
		log = slog.New(slog.NewTextHandler(testLogWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	client := hfapi.NewClient(log, e.hub.srv.Client(), e.hub.srv.URL, "", 5*time.Second, 5*time.Second)
	engine := fcio.NewEngine(log, e.st, fcio.TierAuto)
	pool := fcio.NewPool(0, 64<<20)
	volumes := fcio.NewVolumeSet()
	dl := transfer.NewDownloader(transfer.Config{
		Log: log, HTTP: e.hub.srv.Client(),
		Bandwidth: e.bw, Stats: e.reg, Engine: engine, Pool: pool,
		CheckpointInterval: 50 * time.Millisecond,
		HeaderTimeout:      5 * time.Second,
		// Match newTestEnvLog: fast retry pacing so restart-flow tests do not
		// wait out the production blacklist/backoff.
		RetryBackoffBase:     5 * time.Millisecond,
		UpstreamBlacklistTTL: 20 * time.Millisecond,
	})
	return NewManager(ManagerConfig{
		Store:      e.st,
		Clients:    map[string]*hfapi.Client{e.hub.srv.URL: client},
		Downloader: dl,
		Verifier:   verify.NewChecker(engine, pool, e.duty, log, nil),
		Installer:  cache.NewInstaller(e.cache, engine, volumes, e.duty, log, nil),
		Cache:      e.cache,
		Engine:     engine,
		Pool:       pool,
		Volumes:    volumes,
		Bandwidth:  e.bw,
		API:        e.api,
		Duty:       e.duty,
		Stats:      e.reg,
		Log:        log,
		Limits:     e.limits,
		CacheDir:   e.cache.Root(),
	})
}

// testLogWriter keeps test logs out of the terminal unless they matter.
type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

// waitFor polls cond until true or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (e *testEnv) fileStatus(t *testing.T, path string) string {
	t.Helper()
	var status string
	err := e.st.DB().QueryRowContext(e.t.Context(),
		"SELECT status FROM files WHERE path = ?", path).Scan(&status)
	if err != nil {
		t.Fatalf("file status %s: %v", path, err)
	}
	return status
}

func (e *testEnv) jobStatus(t *testing.T, jobID int64) string {
	t.Helper()
	var status string
	err := e.st.DB().QueryRowContext(e.t.Context(),
		"SELECT status FROM jobs WHERE id = ?", jobID).Scan(&status)
	if err != nil {
		t.Fatalf("job status: %v", err)
	}
	return status
}

// contentOf reads the installed cache-mode file for repo org/repo at path.
func (e *testEnv) installedSnapshotPath(repo, rev, sha, path string) string {
	return filepath.Join(e.cache.Root(),
		"models--"+strings.ReplaceAll(repo, "/", "--"), "snapshots", sha, path)
}

// resolvedTempDir is t.TempDir() with symlinks/short-names resolved, matching
// how the manager canonicalizes reference paths (statReference stores the
// EvalSymlinks result). Without this a test that queries reference_files by the
// raw temp path finds no rows where the two forms diverge: macOS /var vs
// /private/var, and Windows 8.3 short names (%TEMP% is C:\Users\RUNNER~1\...
// but EvalSymlinks expands it to the long form).
func resolvedTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("resolve temp dir: %v", err)
	}
	return resolved
}

// makeContent builds deterministic pseudo-random content of n bytes.
func makeContent(n int, seedByte byte) []byte {
	b := make([]byte, n)
	s := seedByte
	for i := range b {
		s = s*31 + 17
		b[i] = s
	}
	return b
}
