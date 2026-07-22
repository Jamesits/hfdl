package netcfg

import (
	"fmt"
	"net/url"
	"strings"
)

// ProxyMode selects how the outbound proxy is determined.
type ProxyMode int

const (
	// ProxySystem resolves the proxy from the environment, then from the
	// platform proxy configuration.
	ProxySystem ProxyMode = iota
	// ProxyDirect disables proxying entirely.
	ProxyDirect
	// ProxyURL routes every request through one fixed proxy URL.
	ProxyURL
)

// Keyword specs accepted by ParseProxy besides explicit proxy URLs.
const (
	SpecDirect = "direct"
	SpecSystem = "system"
)

// proxySchemes are the proxy URL schemes net/http's Transport can dial
// through. socks5 and socks5h are equivalent there: hostnames are always
// resolved by the proxy (curl's socks5h semantics).
var proxySchemes = map[string]bool{
	"http":    true,
	"https":   true,
	"socks5":  true,
	"socks5h": true,
}

// Proxy is a parsed --hfdl-proxy value.
type Proxy struct {
	Mode ProxyMode
	URL  *url.URL // set only for ProxyURL

	spec string
}

// String returns the normalized spec the Config was parsed from, for echoing
// in logs and job records. The zero Config renders as SpecSystem.
func (c Proxy) String() string {
	if c.spec == "" {
		return SpecSystem
	}
	return c.spec
}

// ParseProxy validates a --hfdl-proxy spec: "direct", "system" (also the empty
// string, so the zero flag value means the default), or a proxy URL with an
// http, https, socks5 or socks5h scheme.
func ParseProxy(spec string) (Proxy, error) {
	switch strings.ToLower(strings.TrimSpace(spec)) {
	case "", SpecSystem:
		return Proxy{Mode: ProxySystem, spec: SpecSystem}, nil
	case SpecDirect:
		return Proxy{Mode: ProxyDirect, spec: SpecDirect}, nil
	}
	u, err := url.Parse(strings.TrimSpace(spec))
	if err != nil {
		return Proxy{}, fmt.Errorf("invalid proxy %q: %w", spec, err)
	}
	if !proxySchemes[strings.ToLower(u.Scheme)] {
		return Proxy{}, fmt.Errorf(
			"invalid proxy %q: want %s|%s|http://…|https://…|socks5://…|socks5h://…",
			spec, SpecDirect, SpecSystem)
	}
	if u.Host == "" {
		return Proxy{}, fmt.Errorf("invalid proxy %q: missing host", spec)
	}
	return Proxy{Mode: ProxyURL, URL: u, spec: spec}, nil
}
