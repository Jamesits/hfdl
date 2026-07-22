package hfapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Client talks to one Hub endpoint. Construct with NewClient; it is safe
// for concurrent use once SetTracer/SetRequestCounter (if used) have been
// called.
type Client struct {
	log *slog.Logger
	hc  *http.Client // follows redirects; transport attaches auth per hop
	// hcNoFollow serves HEAD resolve calls: the Hub puts X-Xet-* metadata on
	// its own (possibly 302) response, which following the redirect would lose.
	hcNoFollow     *http.Client
	endpoint       string
	endpointOrigin string
	etagTimeout    time.Duration
	tracer         trace.Tracer        // nil = no spans; set before concurrent use
	reqCounter     metric.Int64Counter // nil = no request metric; set before concurrent use
}

// NewClient wraps hc so that the bearer token and User-Agent are attached by
// the shared config.NewAuthTransport RoundTripper on every request —
// including redirect hops — with the token sent to the endpoint's host only.
// hc is shallow-copied; the caller's client is left untouched. etagTimeout is
// the response-header deadline for metadata calls issued by this client (the
// body then streams under the caller, unbounded by it).
func NewClient(log *slog.Logger, hc *http.Client, endpoint, token string, etagTimeout time.Duration) *Client {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	endpoint = strings.TrimRight(endpoint, "/")
	endpointOrigin := ""
	if u, err := url.Parse(endpoint); err == nil && u.Scheme != "" && u.Host != "" {
		u.User, u.Path, u.RawPath, u.RawQuery, u.Fragment = nil, "", "", "", ""
		endpointOrigin = u.String()
	}
	inner := *hc
	inner.Transport = config.NewAuthTransport(hc.Transport, endpoint, token, config.UserAgent())
	noFollow := inner
	noFollow.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Client{
		log:            log,
		hc:             &inner,
		hcNoFollow:     &noFollow,
		endpoint:       endpoint,
		endpointOrigin: endpointOrigin,
		etagTimeout:    etagTimeout,
	}
}

// SetTracer installs t to wrap each public API method in a span named
// "hfapi.<Op>". nil (the default) disables tracing. Call before concurrent
// use.
func (c *Client) SetTracer(t trace.Tracer) { c.tracer = t }

// SetRequestCounter installs the hfdl.api.requests counter, incremented once
// per HTTP request this client issues and labeled {endpoint,result}. nil (the
// default) disables the metric. Call before concurrent use.
func (c *Client) SetRequestCounter(counter metric.Int64Counter) { c.reqCounter = counter }

// startSpan begins a "hfapi.<op>" span when a tracer is installed; the
// returned span is nil (and EndSpan a no-op) otherwise.
func (c *Client) startSpan(ctx context.Context, op string) (context.Context, trace.Span) {
	if c.tracer == nil {
		return ctx, nil
	}
	return c.tracer.Start(ctx, "hfapi."+op)
}

func endSpan(sp trace.Span) {
	if sp != nil {
		sp.End()
	}
}

// Bounded result labels for hfdl.api.requests (never per-file/high-cardinality).
const (
	resultOK    = "ok"
	resultError = "error"
)

// recordRequest increments hfdl.api.requests for one issued request. The
// endpoint attribute is this client's Hub endpoint — a coarse, code-controlled
// value, never a repo/path — and result is the bounded ok/error label.
func (c *Client) recordRequest(ctx context.Context, result string) {
	if c.reqCounter == nil {
		return
	}
	c.reqCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("endpoint", c.endpointOrigin),
		attribute.String("result", result),
	))
}

// withEtagTimeout bounds the HEAD resolve call (ResolveXet) by the etag
// timeout. That call reads only response headers and closes its body before
// returning, so a whole-call deadline is safe here — unlike do, which streams
// the body to the caller and uses a header-only deadline instead. timeout <= 0
// means unbounded (ctx still applies).
func (c *Client) withEtagTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.etagTimeout > 0 {
		return context.WithTimeout(ctx, c.etagTimeout)
	}
	return ctx, func() {}
}

// do issues one API request and maps Hub error statuses to typed errors.
// repo/rev only feed NotFoundError/GatedError. The caller owns resp.Body on
// success.
//
// etagTimeout is a response-header deadline, not a body timeout: it is armed
// before the round trip and disarmed the instant headers arrive, so it can
// abort a hung request but never cuts the body stream mid-decode. The request
// context stays live under the caller and is released only when the caller
// closes resp.Body (cancel is wired into Body.Close).
func (c *Client) do(ctx context.Context, method, reqURL string, body []byte, repo, rev string) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(ctx)

	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, reqURL, rdr)
	if err != nil {
		cancel(nil)
		return nil, fmt.Errorf("hfapi: build request %s %s: %w", method, reqURL, err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	disarm := c.armEtagDeadline(cancel)
	resp, err := c.hc.Do(req)
	disarm()
	if err != nil {
		c.recordRequest(ctx, resultError)
		cancel(nil) // no-op if the deadline already cancelled with a cause
		return nil, fmt.Errorf("hfapi: %s %s: %w", method, reqURL, err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		c.recordRequest(ctx, resultOK)
		// Body streams under the caller; its Close releases the request ctx.
		resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: cancel}
		return resp, nil
	}
	c.recordRequest(ctx, resultError)
	// statusError may read the error body, so it runs (in the return
	// expression) before these deferred closes: body first, then cancel.
	defer cancel(nil)
	defer resp.Body.Close()
	return nil, statusError(resp, repo, rev)
}

// armEtagDeadline starts the response-header deadline. If headers do not
// arrive within etagTimeout the request context is cancelled with a
// DeadlineExceeded cause (net/http surfaces the cause), aborting the in-flight
// round trip; the returned disarm stops it once headers are in so the body
// stream is never cut. A non-positive timeout arms nothing (caller ctx still
// applies).
func (c *Client) armEtagDeadline(cancel context.CancelCauseFunc) (disarm func()) {
	if c.etagTimeout <= 0 {
		return func() {}
	}
	t := time.AfterFunc(c.etagTimeout, func() { cancel(context.DeadlineExceeded) })
	return func() { t.Stop() }
}

// cancelBody ties the request context's cancel to Body.Close: do disarms the
// header deadline once headers arrive, so the request context must stay live
// while the caller streams the body and be released only when the caller
// closes it.
type cancelBody struct {
	io.ReadCloser
	cancel context.CancelCauseFunc
}

func (b *cancelBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel(nil)
	return err
}

// statusError maps a non-2xx response to the typed error set.
func statusError(resp *http.Response, repo, rev string) error {
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		msg := readErrMsg(resp)
		if msg == "" {
			msg = resp.Status
		}
		return &AuthError{Msg: msg}
	case http.StatusForbidden:
		return &GatedError{Repo: repo}
	case http.StatusNotFound:
		return &NotFoundError{Repo: repo, Revision: rev}
	case http.StatusTooManyRequests:
		return &RateLimitError{RetryAfter: ParseRetryAfter(resp.Header.Get("Retry-After"))}
	}
	msg := readErrMsg(resp)
	return fmt.Errorf("hfapi: unexpected status %s: %s", resp.Status, msg)
}

// linkRel extracts the URL of the given rel from an RFC 8288 Link header,
// returned verbatim (the Hub's pagination/xet-auth URLs are followed as-is).
func linkRel(header, rel string) string {
	want := `rel="` + rel + `"`
	for seg := range strings.SplitSeq(header, ",") {
		if !strings.Contains(seg, want) {
			continue
		}
		i := strings.IndexByte(seg, '<')
		j := strings.IndexByte(seg, '>')
		if i >= 0 && j > i+1 {
			return seg[i+1 : j]
		}
	}
	return ""
}
