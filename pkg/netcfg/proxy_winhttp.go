package netcfg

import (
	"strings"
	"unicode"

	"golang.org/x/net/http/httpproxy"
)

// winhttpConfig translates a WinHTTP/WinINet proxy server list plus bypass
// list (the strings behind the Windows "Internet Options" proxy settings)
// into an httpproxy.Config. Kept free of build tags so the parsing rules are
// unit-testable on every platform.
//
// Server list grammar: entries separated by ";" (or whitespace), each either
// "host[:port]" (applies to every protocol) or "scheme=host[:port]" with
// scheme http, https or socks. Bypass list: host patterns separated the same
// way; the magic "<local>" entry means "hosts without a dot", which NO_PROXY
// cannot express — loopback names are the closest approximation.
func winhttpConfig(proxyList, bypassList string) *httpproxy.Config {
	cfg := &httpproxy.Config{}
	var all, socks string
	for _, entry := range splitWinList(proxyList) {
		scheme, val, ok := strings.Cut(entry, "=")
		if !ok {
			all = entry
			continue
		}
		switch strings.ToLower(scheme) {
		case "http":
			cfg.HTTPProxy = val
		case "https":
			cfg.HTTPSProxy = val
		case "socks":
			socks = "socks5://" + val
		}
	}
	// Precedence per protocol: explicit scheme entry > bare "host:port"
	// entry > socks entry (WinINet uses SOCKS only when no per-protocol
	// proxy matches).
	for _, fallback := range []string{all, socks} {
		if fallback == "" {
			continue
		}
		if cfg.HTTPProxy == "" {
			cfg.HTTPProxy = fallback
		}
		if cfg.HTTPSProxy == "" {
			cfg.HTTPSProxy = fallback
		}
	}

	var noProxy []string
	for _, entry := range splitWinList(bypassList) {
		if strings.EqualFold(entry, "<local>") {
			noProxy = append(noProxy, "localhost", "127.0.0.1", "::1")
			continue
		}
		// httpproxy understands "*.example.com" wildcards natively.
		noProxy = append(noProxy, entry)
	}
	cfg.NoProxy = strings.Join(noProxy, ",")
	return cfg
}

// splitWinList splits a WinHTTP list string on semicolons, commas and
// whitespace — all separators observed in real registry values — dropping
// empty entries.
func splitWinList(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == ';' || r == ',' || unicode.IsSpace(r)
	})
}
