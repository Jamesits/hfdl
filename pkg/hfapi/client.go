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
	"go.opentelemetry.io/otel/trace"
)

// Client talks to one Hub endpoint. Construct with NewClient; it is safe
// for concurrent use once SetTracer (if used) has been called.
type Client struct {
	log *slog.Logger
	hc  *http.Client // follows redirects; transport attaches auth per hop
	// hcNoFollow serves HEAD resolve calls: the Hub puts X-Xet-* metadata on
	// its own (possibly 302) response, which following the redirect would lose.
	hcNoFollow      *http.Client
	endpoint        string
	hubHost         string
	token           string
	etagTimeout     time.Duration
	downloadTimeout time.Duration
	tracer          trace.Tracer // nil = no spans; set before concurrent use
}

// NewClient wraps hc so that the bearer token and User-Agent are attached by
// a RoundTripper (hubAuthTransport) on every request — including redirect
// hops — with the token sent to the endpoint's host only. hc is shallow-copied;
// the caller's client is left untouched. etagTimeout is the response-header
// deadline for metadata calls issued by this client (the body then streams
// under the caller, unbounded by it); downloadTimeout is only reported by
// DownloadTimeout for the transfer package.
func NewClient(log *slog.Logger, hc *http.Client, endpoint, token string, etagTimeout, downloadTimeout time.Duration) *Client {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	endpoint = strings.TrimRight(endpoint, "/")
	hubHost := ""
	if u, err := url.Parse(endpoint); err == nil {
		hubHost = u.Host
	}
	inner := *hc
	base := hc.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	inner.Transport = &hubAuthTransport{
		base:    base,
		hubHost: hubHost,
		token:   token,
		ua:      config.UserAgent(),
	}
	noFollow := inner
	noFollow.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Client{
		log:             log,
		hc:              &inner,
		hcNoFollow:      &noFollow,
		endpoint:        endpoint,
		hubHost:         hubHost,
		token:           token,
		etagTimeout:     etagTimeout,
		downloadTimeout: downloadTimeout,
	}
}

// SetTracer installs t to wrap each public API method in a span named
// "hfapi.<Op>". nil (the default) disables tracing. Call before concurrent
// use.
func (c *Client) SetTracer(t trace.Tracer) { c.tracer = t }

// Endpoint returns the Hub endpoint this client talks to.
func (c *Client) Endpoint() string { return c.endpoint }

// DownloadTimeout is the response timeout transfer should apply to download
// GETs. hfapi itself never issues download GETs.
func (c *Client) DownloadTimeout() time.Duration { return c.downloadTimeout }

// hubAuthTransport attaches Authorization (when a token is configured and
// the destination is the Hub host) and the hfdl User-Agent on every request.
// Because redirects re-enter RoundTrip for each hop, the header is naturally
// re-evaluated per hop: kept on the Hub host, stripped anywhere else
// (CDN/object storage). This intentionally does not rely on net/http's
// redirect header-forwarding rules.
type hubAuthTransport struct {
	base    http.RoundTripper
	hubHost string
	token   string
	ua      string
}

func (t *hubAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header = req.Header.Clone()
	if r.Header.Get("User-Agent") == "" {
		r.Header.Set("User-Agent", t.ua)
	}
	if t.token != "" && strings.EqualFold(r.URL.Host, t.hubHost) {
		r.Header.Set("Authorization", "Bearer "+t.token)
	} else {
		r.Header.Del("Authorization")
	}
	return t.base.RoundTrip(r)
}

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
		cancel(nil) // no-op if the deadline already cancelled with a cause
		return nil, fmt.Errorf("hfapi: %s %s: %w", method, reqURL, err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Body streams under the caller; its Close releases the request ctx.
		resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: cancel}
		return resp, nil
	}
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
	for _, seg := range strings.Split(header, ",") {
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
