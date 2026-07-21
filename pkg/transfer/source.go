package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
)

// Upstream is one resolve endpoint candidate for httpSource. CooldownUntil
// and BlacklistUntil are seeded from store.upstreams by sched; transfer
// treats them as read-only and keeps its own per-(upstream,file) state
// (rangeless marks, identity pins, EMA view) so concurrent file downloads
// never race on shared structs.
type Upstream struct {
	Endpoint                      string
	EMABps                        float64
	CooldownUntil, BlacklistUntil time.Time
}

// Block is the unit of scheduling: one contiguous byte range to fetch.
type Block struct {
	ID             int64
	Idx            int
	Offset, Length int64
}

// End returns the exclusive end offset of the block.
func (b Block) End() int64 { return b.Offset + b.Length }

// BlockLeaser is implemented by sched over the store (transfer stays
// DB-free). Lease returns ok=false when no more blocks are pending; blocks
// made pending again by Requeue must be returned by later Lease calls.
type BlockLeaser interface {
	Lease(ctx context.Context, fileID int64) (Block, bool, error) // false = no more pending
	Complete(ctx context.Context, b Block) error
	Requeue(ctx context.Context, b Block, backoff time.Duration, cause error) error
}

// BlockSource abstracts where a block's bytes come from. transfer owns
// scheduling, buffering, fcio writes, stall/timeout policy, and progress;
// sources own protocol. Implemented by transfer's own httpSource and by
// xet.Source (xet imports transfer; transfer never imports xet).
type BlockSource interface {
	// Boundaries may snap block edges (xet: term-aligned; http: identity).
	Boundaries(missing []Interval, blockSize int64) []Interval
	// Open streams the bytes of [off, off+len) — decoded file bytes,
	// whatever the wire format.
	Open(ctx context.Context, off, length int64) (io.ReadCloser, error)
}

// FailKind classifies a retriable attempt failure for requeue/penalty
// decisions and span events.
type FailKind int

const (
	FailNet           FailKind = iota // connection/transport error
	FailHeaderTimeout                 // response headers not received in time
	FailValidation                    // 206 invariants violated (Content-Range, encoding, identity, length)
	FailRangeless                     // 200 to a ranged request: upstream cannot range this object
	FailStatus                        // retriable HTTP status (5xx, 429/503 without cooldown handling)
	FailStall                         // stall policy killed the connection
	FailShortBody                     // body ended before the requested length
	FailNoHealthy                     // every upstream cooled down/blacklisted right now
)

// AttemptError is a retriable single-attempt failure; the worker requeues
// the block with backoff and (for most kinds) penalizes the upstream EMA.
// StatusCode and RetryAfter are populated for FailStatus responses (429/503/
// 5xx) so the scheduler can apply a typed, Retry-After-aware upstream cooldown
// instead of string-matching the error text. RetryAfter is 0 when the response
// carried no usable Retry-After header.
type AttemptError struct {
	Upstream   string
	Kind       FailKind
	StatusCode int
	RetryAfter time.Duration
	Err        error
}

func (e *AttemptError) Error() string {
	if e.Upstream == "" {
		return "transfer: attempt failed: " + e.Err.Error()
	}
	return fmt.Sprintf("transfer: attempt via %s failed: %s", e.Upstream, e.Err)
}

func (e *AttemptError) Unwrap() error { return e.Err }

// ResetFileError is terminal for the run: the file's persisted size/progress
// disagrees with what upstreams serve (416 on an incomplete file). sched
// resets the file to queued for a full redownload.
type ResetFileError struct {
	Reason string
}

func (e *ResetFileError) Error() string { return "transfer: file must be reset: " + e.Reason }

// TerminalHTTPError is a non-retriable HTTP status (401/403/404) — bad
// credentials, gated repo, or missing revision.
type TerminalHTTPError struct {
	Upstream   string
	StatusCode int
}

func (e *TerminalHTTPError) Error() string {
	return fmt.Sprintf("transfer: terminal HTTP %d from %s", e.StatusCode, e.Upstream)
}

// errAllRangeless signals that every upstream answered 200 to a ranged
// request: the object cannot be range-served, so the run switches to the
// single-stream fallback (one whole-file GET, no mid-file resume).
var errAllRangeless = errors.New("transfer: no upstream supports range requests for this file")

// rangeNotSatisfiableError carries a 416; the worker decides between
// benign (file already complete) and ResetFileError.
type rangeNotSatisfiableError struct{ upstream string }

func (e *rangeNotSatisfiableError) Error() string {
	return fmt.Sprintf("transfer: 416 range not satisfiable from %s", e.upstream)
}

// httpSource is the multi-upstream ranged-GET BlockSource. Per block start
// it picks an upstream by the configured policy, issues a ranged resolve
// GET, and validates the response before any byte is used.
type httpSource struct {
	log           *slog.Logger
	hc            *http.Client
	repo, sha     string
	path          string
	size          int64
	blobID        string
	upstreams     []*Upstream
	policy        config.UpstreamPolicy
	headerTimeout time.Duration
	blacklistTTL  time.Duration
	rng           *rand.Rand

	mu        sync.Mutex
	rr        uint64               // round-robin counter over the healthy set
	ema       map[string]float64   // per-file EMA view, seeded from Upstream.EMABps
	pins      map[string]string    // (upstream,file) identity pin: first validated ETag
	excluded  map[string]bool      // identity mismatch: rejected for this file
	rangeless map[string]bool      // answered 200 to a ranged request
	cooldown  map[string]time.Time // per-file cooldown (429/503, stall blacklist)
}

// newHTTPSource builds the source. seed drives Random/BestSpeed exploration;
// tests pass a fixed seed for deterministic distributions.
func newHTTPSource(log *slog.Logger, hc *http.Client, t *FileTask, headerTimeout, blacklistTTL time.Duration, seed [32]byte) *httpSource {
	s := &httpSource{
		log:           log,
		repo:          t.Repo,
		sha:           t.SHA,
		path:          t.Path,
		size:          t.Size,
		blobID:        t.BlobID,
		upstreams:     t.Upstreams,
		policy:        t.Policy,
		headerTimeout: headerTimeout,
		blacklistTTL:  blacklistTTL,
		rng:           rand.New(rand.NewChaCha8(seed)),
		ema:           make(map[string]float64, len(t.Upstreams)),
		pins:          make(map[string]string),
		excluded:      make(map[string]bool),
		rangeless:     make(map[string]bool),
		cooldown:      make(map[string]time.Time),
	}
	for _, u := range t.Upstreams {
		s.ema[u.Endpoint] = u.EMABps
	}
	// Clone the injected client so the redirect policy can be pinned without
	// mutating the caller's client: Authorization is stripped when a
	// redirect crosses hosts (hfapi's hubAuthTransport pattern, replicated
	// inline because transfer owns this client's usage). The transport is
	// shared, so connection pooling is preserved.
	c := *hc
	c.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("transfer: too many redirects")
		}
		if len(via) > 0 && !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
			req.Header.Del("Authorization")
		}
		return nil
	}
	s.hc = &c
	return s
}

// Boundaries is the identity mapping: http blocks need no edge snapping, the
// caller chunks the missing ranges at the current block size itself.
func (s *httpSource) Boundaries(missing []Interval, blockSize int64) []Interval {
	return missing
}

// resolveURL builds {upstream}/{repo}/resolve/{sha}/{path} with per-segment
// escaping (repo paths may contain spaces or UTF-8; slashes are structure).
func (s *httpSource) resolveURL(endpoint string) string {
	esc := func(p string) string {
		segs := strings.Split(p, "/")
		for i := range segs {
			segs[i] = url.PathEscape(segs[i])
		}
		return strings.Join(segs, "/")
	}
	return strings.TrimRight(endpoint, "/") + "/" + esc(s.repo) + "/resolve/" + esc(s.sha) + "/" + esc(s.path)
}

// upstreamCarrier lets the worker recover which upstream an Open served,
// for events, stats and EMA accounting, without widening the BlockSource
// interface xet implements.
type upstreamCarrier interface{ UpstreamEndpoint() string }

// blockReader wraps a response body with its serving upstream and the
// request-context cancel (header deadline cancel must survive header
// validation but fire on body close).
type blockReader struct {
	io.ReadCloser
	upstream string
	cancel   context.CancelFunc
}

func (r *blockReader) Close() error {
	err := r.ReadCloser.Close()
	r.cancel()
	return err
}

func (r *blockReader) UpstreamEndpoint() string { return r.upstream }

// upstreamOf extracts the serving upstream from an Open result ("" for
// non-http sources such as xet).
func upstreamOf(rc io.ReadCloser) string {
	if c, ok := rc.(upstreamCarrier); ok {
		return c.UpstreamEndpoint()
	}
	return ""
}

// Open streams [off, off+length) from a policy-selected upstream. The
// response is fully validated before the body is handed out; every failure
// is a typed error the worker classifies.
func (s *httpSource) Open(ctx context.Context, off, length int64) (io.ReadCloser, error) {
	if s.allRangeless() {
		// Every upstream answered 200 to a ranged request: ranged scheduling
		// is abandoned. Fail so the worker requeues the block and the run
		// switches to the single-stream fallback (openWhole, driven by
		// runFallback). A ranged worker must NEVER receive a whole-file
		// stream here — it would write file-offset-0 bytes at the block
		// offset and record bogus slabs before the length mismatch surfaces.
		// The whole-file GET is reachable only through openWhole.
		return nil, errAllRangeless
	}
	up, err := s.pick(time.Now())
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.resolveURL(up.Endpoint), nil)
	if err != nil {
		return nil, fmt.Errorf("transfer: build request: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+length-1))
	req.Header.Set("Accept-Encoding", "identity")
	resp, cancel, err := s.do(ctx, req, up.Endpoint)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if err := s.validate206(resp, up.Endpoint, off, length); err != nil {
			_ = resp.Body.Close()
			cancel()
			return nil, err
		}
		return &blockReader{ReadCloser: resp.Body, upstream: up.Endpoint, cancel: cancel}, nil
	case http.StatusOK:
		_ = resp.Body.Close()
		cancel()
		// This upstream cannot serve ranges for this object: discard the
		// attempt (no byte was written), mark (upstream,file) rangeless for
		// the run and let the block go elsewhere.
		s.markRangeless(up.Endpoint)
		if s.allRangeless() {
			return nil, errAllRangeless
		}
		return nil, &AttemptError{
			Upstream: up.Endpoint,
			Kind:     FailRangeless,
			Err:      errors.New("200 to a ranged request"),
		}
	case http.StatusRequestedRangeNotSatisfiable:
		_ = resp.Body.Close()
		cancel()
		return nil, &rangeNotSatisfiableError{upstream: up.Endpoint}
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		ra := parseRetryAfter(resp.Header.Get("Retry-After"))
		st := resp.StatusCode
		_ = resp.Body.Close()
		cancel()
		s.setCooldown(up.Endpoint, time.Now().Add(upstreamCooldownTTL))
		return nil, &AttemptError{
			Upstream:   up.Endpoint,
			Kind:       FailStatus,
			StatusCode: st,
			RetryAfter: ra,
			Err:        fmt.Errorf("HTTP %d (upstream cooled down)", st),
		}
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		_ = resp.Body.Close()
		cancel()
		return nil, &TerminalHTTPError{Upstream: up.Endpoint, StatusCode: resp.StatusCode}
	default:
		_ = resp.Body.Close()
		cancel()
		if resp.StatusCode >= 500 {
			return nil, &AttemptError{
				Upstream:   up.Endpoint,
				Kind:       FailStatus,
				StatusCode: resp.StatusCode,
				Err:        fmt.Errorf("HTTP %d", resp.StatusCode),
			}
		}
		return nil, &TerminalHTTPError{Upstream: up.Endpoint, StatusCode: resp.StatusCode}
	}
}

// openWhole serves the single-stream fallback: a plain GET of the whole
// object from any usable upstream (rangeless marks ignored — that is the
// point — but identity-excluded mirrors stay excluded).
func (s *httpSource) openWhole(ctx context.Context) (io.ReadCloser, error) {
	up := s.pickWhole(time.Now())
	if up == nil {
		return nil, &AttemptError{Kind: FailNoHealthy, Err: errors.New("no upstream available for fallback")}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.resolveURL(up.Endpoint), nil)
	if err != nil {
		return nil, fmt.Errorf("transfer: build request: %w", err)
	}
	resp, cancel, err := s.do(ctx, req, up.Endpoint)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		if resp.ContentLength >= 0 && resp.ContentLength != s.size {
			_ = resp.Body.Close()
			cancel()
			return nil, &AttemptError{Upstream: up.Endpoint, Kind: FailValidation,
				Err: fmt.Errorf("fallback Content-Length %d != file size %d", resp.ContentLength, s.size)}
		}
	case http.StatusPartialContent:
		start, end, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok || start != 0 || end != s.size-1 || total != s.size {
			_ = resp.Body.Close()
			cancel()
			return nil, &AttemptError{Upstream: up.Endpoint, Kind: FailValidation,
				Err: fmt.Errorf("fallback Content-Range %q mismatch", resp.Header.Get("Content-Range"))}
		}
	default:
		st := resp.StatusCode
		_ = resp.Body.Close()
		cancel()
		if st == http.StatusTooManyRequests || st == http.StatusServiceUnavailable || st >= 500 {
			return nil, &AttemptError{Upstream: up.Endpoint, Kind: FailStatus, Err: fmt.Errorf("HTTP %d", st)}
		}
		return nil, &TerminalHTTPError{Upstream: up.Endpoint, StatusCode: st}
	}
	// Fallback whole-file GET: a plain 200 may legitimately lack a strong
	// validator, so identity is checked opportunistically (no requireValidator)
	// — a present ETag must still match the blob/pin, but a missing one does
	// not fail the last-resort fetch.
	if err := s.checkIdentity(up.Endpoint, etagOf(resp), false); err != nil {
		_ = resp.Body.Close()
		cancel()
		return nil, err
	}
	return &blockReader{ReadCloser: resp.Body, upstream: up.Endpoint, cancel: cancel}, nil
}

// do issues req with the header deadline layered on top of the caller's
// context: response headers must arrive within headerTimeout, then the
// timer is disarmed and the body streams under the caller's ctx (stall
// policy). The returned cancel releases the request context; it is wired
// into the blockReader so it fires exactly when the body closes.
func (s *httpSource) do(ctx context.Context, req *http.Request, upstream string) (*http.Response, context.CancelFunc, error) {
	reqCtx, cancel := context.WithCancel(ctx)
	var timer *time.Timer
	if s.headerTimeout > 0 {
		timer = time.AfterFunc(s.headerTimeout, cancel)
	}
	resp, err := s.hc.Do(req.WithContext(reqCtx))
	if timer != nil {
		timer.Stop()
	}
	if err != nil {
		cancel()
		if ctx.Err() == nil && reqCtx.Err() != nil {
			return nil, nil, &AttemptError{Upstream: upstream, Kind: FailHeaderTimeout,
				Err: fmt.Errorf("headers not received within %s: %w", s.headerTimeout, err)}
		}
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		return nil, nil, &AttemptError{Upstream: upstream, Kind: FailNet, Err: err}
	}
	return resp, cancel, nil
}

// validate206 enforces the ranged-response invariants: status 206 with the
// exact requested Content-Range, identity Content-Encoding, exact announced
// body length, and an ETag consistent with the file's BlobID — the first
// validated response pins (upstream,file) identity, and a mirror serving
// different content is rejected for that file.
func (s *httpSource) validate206(resp *http.Response, upstream string, off, length int64) error {
	fail := func(format string, args ...any) error {
		return &AttemptError{Upstream: upstream, Kind: FailValidation, Err: fmt.Errorf(format, args...)}
	}
	start, end, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
	if !ok {
		return fail("unparseable Content-Range %q", resp.Header.Get("Content-Range"))
	}
	if start != off || end != off+length-1 || total != s.size {
		return fail("Content-Range bytes %d-%d/%d != requested %d-%d/%d", start, end, total, off, off+length-1, s.size)
	}
	if ce := resp.Header.Get("Content-Encoding"); ce != "" && !strings.EqualFold(ce, "identity") {
		return fail("Content-Encoding %q, want identity", ce)
	}
	if resp.ContentLength >= 0 && resp.ContentLength != length {
		return fail("Content-Length %d != requested %d", resp.ContentLength, length)
	}
	if err := s.checkIdentity(upstream, etagOf(resp), true); err != nil {
		return err
	}
	return nil
}

// parseContentRange parses "bytes START-END/TOTAL".
func parseContentRange(v string) (start, end, total int64, ok bool) {
	if !strings.HasPrefix(v, "bytes ") {
		return 0, 0, 0, false
	}
	spec := strings.TrimPrefix(v, "bytes ")
	dash := strings.IndexByte(spec, '-')
	slash := strings.LastIndexByte(spec, '/')
	if dash <= 0 || slash <= dash+1 || slash == len(spec)-1 {
		return 0, 0, 0, false
	}
	start, err1 := strconv.ParseInt(spec[:dash], 10, 64)
	end, err2 := strconv.ParseInt(spec[dash+1:slash], 10, 64)
	total, err3 := strconv.ParseInt(spec[slash+1:], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, 0, 0, false
	}
	return start, end, total, true
}

// parseRetryAfter parses a Retry-After header: delta-seconds or an HTTP-date.
// Returns 0 when absent or unparseable. Kept local (not hfapi.ParseRetryAfter)
// so the transfer layer stays independent of hfapi — transfer builds its own
// resolve URLs and never imports the Hub API client, by design.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// etagOf extracts the object identity: X-Linked-Etag wins over ETag (HF
// resolve responses surface the LFS oid there), weak validators and quotes
// stripped.
func etagOf(resp *http.Response) string {
	v := resp.Header.Get("X-Linked-Etag")
	if v == "" {
		v = resp.Header.Get("ETag")
	}
	v = strings.TrimPrefix(v, "W/")
	return strings.Trim(v, `"`)
}

// checkIdentity enforces the (upstream,file) identity pin. requireValidator is
// true for ranged 206 responses, which must prove identity: the plan requires
// every ranged response to carry a validator and the first to pin it, so a
// missing ETag/X-Linked-Etag is itself a validation failure (a mirror that
// drops the validator can no longer be trusted to be serving the same object).
// It is false for the last-resort whole-file fallback, where a present
// validator is still checked but a missing one is tolerated. A mirror serving
// content inconsistent with BlobID — or with its own pin — is excluded for
// this file.
func (s *httpSource) checkIdentity(upstream, etag string, requireValidator bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pin, ok := s.pins[upstream]; ok && pin != "" {
		if etag == "" {
			if requireValidator {
				s.excluded[upstream] = true
				return &AttemptError{Upstream: upstream, Kind: FailValidation,
					Err: fmt.Errorf("ranged response dropped its validator; cannot confirm pinned identity %q", pin)}
			}
			return nil
		}
		if !strings.EqualFold(etag, pin) {
			s.excluded[upstream] = true
			return &AttemptError{Upstream: upstream, Kind: FailValidation,
				Err: fmt.Errorf("ETag %q no longer matches pinned identity %q", etag, pin)}
		}
		return nil
	}
	// No pin yet: this response must establish identity.
	if etag == "" {
		if requireValidator {
			return &AttemptError{Upstream: upstream, Kind: FailValidation,
				Err: errors.New("ranged response carried no ETag/X-Linked-Etag to prove identity")}
		}
		return nil
	}
	if s.blobID != "" && !strings.EqualFold(etag, s.blobID) {
		s.excluded[upstream] = true
		return &AttemptError{Upstream: upstream, Kind: FailValidation,
			Err: fmt.Errorf("ETag %q inconsistent with blob %q", etag, s.blobID)}
	}
	s.pins[upstream] = etag
	return nil
}

// markRangeless records that an upstream answered 200 to a ranged request.
func (s *httpSource) markRangeless(endpoint string) {
	s.mu.Lock()
	s.rangeless[endpoint] = true
	s.mu.Unlock()
}

// allRangeless reports whether every configured upstream is rangeless for
// this file — the single-stream fallback trigger.
func (s *httpSource) allRangeless() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.upstreams) == 0 {
		return false
	}
	for _, u := range s.upstreams {
		if !s.rangeless[u.Endpoint] {
			return false
		}
	}
	return true
}

// setCooldown parks an upstream for this file (429/503 or stall blacklist).
func (s *httpSource) setCooldown(endpoint string, until time.Time) {
	s.mu.Lock()
	s.cooldown[endpoint] = until
	s.mu.Unlock()
}

// blacklist temporarily parks an upstream after a stall or transport/
// validation error so the next block for this file goes elsewhere.
// It never shortens a longer existing park (e.g. a 429 30s cooldown).
func (s *httpSource) blacklist(endpoint string) {
	until := time.Now().Add(s.blacklistTTL)
	s.mu.Lock()
	if cur, ok := s.cooldown[endpoint]; !ok || until.After(cur) {
		s.cooldown[endpoint] = until
	}
	s.mu.Unlock()
}

// reportRate folds a measured per-block rate into the per-file EMA view
// (α=0.2, matching stats.Registry so both views agree).
func (s *httpSource) reportRate(endpoint string, bps float64) {
	s.mu.Lock()
	s.ema[endpoint] = emaAlpha*bps + (1-emaAlpha)*s.ema[endpoint]
	s.mu.Unlock()
}

// penalize halves the per-file EMA view of an upstream (stall/error).
func (s *httpSource) penalize(endpoint string) {
	s.mu.Lock()
	s.ema[endpoint] *= emaPenaltyFactor
	s.mu.Unlock()
}
