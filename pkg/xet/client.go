package xet

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// DefaultCacheMaxBytes is the chunk-cache LRU cap applied when
// Config.CacheMaxBytes is 0: 10 GiB.
const DefaultCacheMaxBytes = int64(10) << 30 // 10 GiB

// tokenRefreshSkew mirrors hf_xet's REFRESH_BUFFER_SEC
// (xet_client/src/cas_client/auth.rs): a cached token is treated as expired
// this long before its stated exp.
const tokenRefreshSkew = 30 * time.Second

// Config controls a Client. The zero value is valid.
type Config struct {
	CasURL        string // HF_XET_ENDPOINT override; "" = use the token's casUrl
	CacheDir      string // HF_XET_CACHE override; "" = chunk cache disabled (cmd resolves the <hf cache>/xet default)
	CacheMaxBytes int64  // LRU cap; 0 = DefaultCacheMaxBytes
}

// TokenSource fetches a CAS bearer token for refreshRoute
// (hfapi.Client.XetToken's signature). Called lazily and cached until exp.
type TokenSource func(ctx context.Context, refreshRoute string) (*hfapi.XetToken, error)

// Client owns CAS auth state and the shared chunk cache. Safe for
// concurrent use; Sources created from it may be Opened in parallel.
type Client struct {
	log    *slog.Logger
	hc     *http.Client
	cfg    Config
	tok    TokenSource
	tracer trace.Tracer
	cache  *chunkCache // nil when cfg.CacheDir == ""

	// Single cached token: in practice every file of a transfer shares one
	// refresh route; a differing route simply forces a refresh.
	mu          sync.Mutex
	token       *hfapi.XetToken
	tokenRoute  string
	tokenCasURL string // casUrl captured with the token (base for CAS API)
}

// NewClient builds a Client. prov may be nil (noop telemetry). The chunk
// cache is opened best-effort: an unusable CacheDir disables caching rather
// than failing construction, since every cached byte is re-fetchable.
func NewClient(log *slog.Logger, hc *http.Client, cfg Config, tok TokenSource, prov *otel.Providers) *Client {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if prov == nil {
		prov = otel.Noop()
	}
	if cfg.CacheMaxBytes <= 0 {
		cfg.CacheMaxBytes = DefaultCacheMaxBytes
	}
	c := &Client{
		log:    log,
		hc:     hc,
		cfg:    cfg,
		tok:    tok,
		tracer: prov.Tracer("hfdl.xet"),
	}
	if cfg.CacheDir != "" {
		if cc, err := openChunkCache(cfg.CacheDir, cfg.CacheMaxBytes); err != nil {
			log.Warn("xet chunk cache disabled", "dir", cfg.CacheDir, "err", err)
		} else {
			c.cache = cc
		}
	}
	return c
}

// NewSource binds a xet-backed file to this client. sched Prepares the
// returned Source and hands it to transfer as FileTask.Source.
func (c *Client) NewSource(xetHash string, size int64, refreshRoute string) *Source {
	return &Source{
		client: c,
		log:    c.log,
		fileID: xetHash,
		size:   size,
		route:  refreshRoute,
	}
}

// casBase returns the CAS API base URL: the HF_XET_ENDPOINT-style override
// wins over the token's casUrl. The CAS base URL comes from the token
// response, never hardcoded.
func (c *Client) casBase(tok *hfapi.XetToken) string {
	if c.cfg.CasURL != "" {
		return strings.TrimRight(c.cfg.CasURL, "/")
	}
	return strings.TrimRight(tok.CasURL, "/")
}

// token returns a cached unexpired token for route, refreshing (with a
// xet.token_refresh span) when missing, expired within the skew window, or
// invalidated after a CAS 401.
func (c *Client) casToken(ctx context.Context, route string) (*hfapi.XetToken, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != nil && c.tokenRoute == route && time.Until(c.token.Exp) > tokenRefreshSkew {
		return c.token, nil
	}
	ctx, sp := c.tracer.Start(ctx, "xet.token_refresh")
	defer sp.End()
	tok, err := c.tok(ctx, route)
	if err != nil {
		sp.RecordError(err)
		return nil, fmt.Errorf("xet: token refresh: %w", err)
	}
	c.token = tok
	c.tokenRoute = route
	c.tokenCasURL = c.casBase(tok)
	if u, err := url.Parse(c.tokenCasURL); err == nil {
		sp.SetAttributes(attribute.String("hfdl.xet.cas_host", u.Host))
	}
	sp.SetAttributes(attribute.Int64("hfdl.xet.token_expiry", tok.Exp.Unix()))
	return tok, nil
}

// invalidateToken drops the cached token for route so the next call
// refreshes. Called on CAS 401.
func (c *Client) invalidateToken(route string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tokenRoute == route {
		c.token = nil
	}
}

// doCAS issues one authenticated GET against the CAS API. On 401 the token
// is invalidated, refreshed, and the request retried exactly once; a second
// 401 is a terminal AuthError. 429 surfaces as *hfapi.RateLimitError so
// sched can apply its cooldown policy (contract: reuse hfapi's type).
// The caller owns the returned Body on any 2xx; other statuses are drained
// and closed here.
func (c *Client) doCAS(ctx context.Context, route, reqURL, rangeHeader string) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		tok, err := c.casToken(ctx, route)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("xet: build CAS request %s: %w", reqURL, err)
		}
		req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
		req.Header.Set("User-Agent", config.UserAgent())
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		resp, err := c.hc.Do(req)
		if err != nil {
			return nil, fmt.Errorf("xet: CAS GET %s: %w", reqURL, err)
		}
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			_ = resp.Body.Close()
			c.invalidateToken(route)
			if attempt > 0 {
				return nil, &AuthError{Msg: "token refresh did not clear 401"}
			}
			continue
		case http.StatusTooManyRequests:
			ra := parseRetryAfter(resp.Header.Get("Retry-After"))
			_ = resp.Body.Close()
			return nil, &hfapi.RateLimitError{RetryAfter: ra}
		default:
			return resp, nil
		}
	}
}
