//go:build darwin

package netcfg

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"

	"golang.org/x/net/http/httpproxy"
)

// systemProxyConfig shells out to `scutil --proxy`, the stable CLI view of
// the SystemConfiguration proxy dictionary. This avoids cgo/CFNetwork, which
// would break the cross-compiled release builds. PAC files and WPAD are
// reported but not evaluated.
func systemProxyConfig(ctx context.Context, log *slog.Logger) (*httpproxy.Config, error) {
	out, err := exec.CommandContext(ctx, "scutil", "--proxy").Output()
	if err != nil {
		return nil, fmt.Errorf("scutil --proxy: %w", err)
	}
	cfg, pac := scutilConfig(string(out))
	if pac && cfg.HTTPProxy == "" && cfg.HTTPSProxy == "" {
		log.LogAttrs(ctx, slog.LevelWarn,
			"macOS proxy is PAC/auto-discovery only, which hfdl does not evaluate; going direct")
	}
	return cfg, nil
}
