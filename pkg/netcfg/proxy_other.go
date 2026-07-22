//go:build !windows && !darwin

package netcfg

import (
	"context"
	"log/slog"

	"golang.org/x/net/http/httpproxy"
)

// systemProxyConfig: these platforms have no proxy store beyond the
// environment (desktop Linux proxy settings are exported as *_PROXY into the
// session), so system mode degrades to environment-only resolution.
func systemProxyConfig(_ context.Context, _ *slog.Logger) (*httpproxy.Config, error) {
	return nil, nil
}
