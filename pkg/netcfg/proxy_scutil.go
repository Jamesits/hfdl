package netcfg

import (
	"net"
	"strings"

	"golang.org/x/net/http/httpproxy"
)

// scutilConfig parses `scutil --proxy` output (the SystemConfiguration proxy
// dictionary rendered as "Key : Value" lines with ExceptionsList as a nested
// array) into an httpproxy.Config. pac reports whether a PAC file or WPAD
// auto-discovery is enabled, which hfdl does not evaluate; the caller decides
// whether that deserves a warning. Kept free of build tags so the parsing is
// unit-testable on every platform.
func scutilConfig(out string) (cfg *httpproxy.Config, pac bool) {
	kv := map[string]string{}
	var exceptions []string
	inExceptions := false
	for line := range strings.SplitSeq(out, "\n") {
		line = strings.TrimSpace(line)
		if inExceptions {
			if line == "}" {
				inExceptions = false
				continue
			}
			if _, v, ok := strings.Cut(line, ":"); ok {
				exceptions = append(exceptions, strings.TrimSpace(v))
			}
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if k == "ExceptionsList" {
			inExceptions = strings.HasSuffix(v, "{")
			continue
		}
		kv[k] = v
	}

	hostport := func(key string) string {
		if kv[key+"Enable"] != "1" {
			return ""
		}
		host := kv[key+"Proxy"]
		if host == "" {
			return ""
		}
		if port := kv[key+"Port"]; port != "" {
			return net.JoinHostPort(host, port)
		}
		return host
	}

	cfg = &httpproxy.Config{
		HTTPProxy:  hostport("HTTP"),
		HTTPSProxy: hostport("HTTPS"),
		NoProxy:    strings.Join(exceptions, ","),
	}
	// SOCKS applies only to protocols without a dedicated proxy, matching
	// how macOS network clients prioritize the dictionary entries.
	if socks := hostport("SOCKS"); socks != "" {
		if cfg.HTTPProxy == "" {
			cfg.HTTPProxy = "socks5://" + socks
		}
		if cfg.HTTPSProxy == "" {
			cfg.HTTPSProxy = "socks5://" + socks
		}
	}
	pac = kv["ProxyAutoConfigEnable"] == "1" || kv["ProxyAutoDiscoveryEnable"] == "1"
	return cfg, pac
}
