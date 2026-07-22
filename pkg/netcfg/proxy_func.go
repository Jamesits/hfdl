package netcfg

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"

	"golang.org/x/net/http/httpproxy"
)

// ProxyFunc resolves the Config into an http.Transport.Proxy callback; nil
// means direct. System-mode resolution happens once here, not per request:
// the environment wins, and only when no *_PROXY variable is set is the
// platform store queried (ctx bounds that query, e.g. the scutil call on
// macOS). A failed platform lookup degrades to direct with a warning rather
// than failing the run — direct is exactly the pre-proxy-support behavior.
func (c Proxy) ProxyFunc(ctx context.Context, log *slog.Logger) func(*http.Request) (*url.URL, error) {
	switch c.Mode {
	case ProxyDirect:
		return nil
	case ProxyURL:
		return http.ProxyURL(c.URL)
	}

	if env := httpproxy.FromEnvironment(); env.HTTPProxy != "" || env.HTTPSProxy != "" {
		log.LogAttrs(ctx, slog.LevelDebug, "proxy resolved from environment",
			slog.String("http", env.HTTPProxy), slog.String("https", env.HTTPSProxy))
		return requestProxy(env)
	}

	sys, err := systemProxyConfig(ctx, log)
	if err != nil {
		log.LogAttrs(ctx, slog.LevelWarn, "system proxy detection failed; going direct",
			slog.Any("err", err))
		return nil
	}
	if sys == nil || (sys.HTTPProxy == "" && sys.HTTPSProxy == "") {
		return nil
	}
	log.LogAttrs(ctx, slog.LevelDebug, "proxy resolved from platform settings",
		slog.String("http", sys.HTTPProxy), slog.String("https", sys.HTTPSProxy),
		slog.String("no_proxy", sys.NoProxy))
	return requestProxy(sys)
}

// requestProxy adapts httpproxy's URL-based callback to the request-based
// signature http.Transport.Proxy wants.
func requestProxy(cfg *httpproxy.Config) func(*http.Request) (*url.URL, error) {
	fn := cfg.ProxyFunc()
	return func(req *http.Request) (*url.URL, error) { return fn(req.URL) }
}
