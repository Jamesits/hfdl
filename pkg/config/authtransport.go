package config

import (
	"net/http"
	"net/url"
	"strings"
)

// authTransport attaches the hfdl User-Agent to every request and, when a
// token is configured, the bearer token — but only to requests whose
// origin (scheme + host + effective port) matches the configured Hub
// endpoint. Authorization is stripped on every other origin, so a redirect
// to a CDN/object store — or an https→http downgrade on the same host —
// never leaks the token.
//
// Because redirects re-enter RoundTrip once per hop, the decision is
// re-evaluated per hop: the token is kept on the Hub origin and stripped
// anywhere else. This deliberately does not rely on net/http's own
// redirect header-forwarding rules.
//
// An empty hub endpoint matches no destination, so every request is stripped
// of Authorization: the defensive configuration for a client that must never
// carry credentials at all (transfer's payload downloads authenticate via
// presigned URLs, so they need — and must leak — no bearer token).
type authTransport struct {
	base      http.RoundTripper
	hubScheme string
	hubHost   string // lowercase hostname, no port
	hubPort   string // effective port (scheme default filled in)
	token     string
	ua        string
}

// NewAuthTransport wraps base with the shared Hub auth/User-Agent policy so
// hfapi and transfer never re-implement the security-sensitive token-strip
// decision independently. hubEndpoint is the configured Hub base URL; an
// empty or unparseable value means "authenticate nowhere". A nil base uses
// http.DefaultTransport.
func NewAuthTransport(base http.RoundTripper, hubEndpoint, token, ua string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	t := &authTransport{base: base, token: token, ua: ua}
	if u, err := url.Parse(hubEndpoint); err == nil && u.Scheme != "" && u.Host != "" {
		t.hubScheme = strings.ToLower(u.Scheme)
		t.hubHost = strings.ToLower(u.Hostname())
		t.hubPort = effectivePort(u)
	}
	return t
}

// effectivePort resolves the port a URL actually targets: an explicit port
// wins, otherwise the scheme default. Origins differing only in an implicit
// vs explicit default port ("https://h" vs "https://h:443") are the same.
func effectivePort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return "443"
	case "http":
		return "80"
	}
	return ""
}

// sameHubOrigin reports whether the request targets exactly the configured
// Hub origin. Comparing the full origin (not just the host) is what blocks
// a same-host https→http downgrade redirect from carrying the token onto
// plaintext HTTP.
func (t *authTransport) sameHubOrigin(u *url.URL) bool {
	if t.hubHost == "" {
		return false
	}
	return strings.ToLower(u.Scheme) == t.hubScheme &&
		strings.ToLower(u.Hostname()) == t.hubHost &&
		effectivePort(u) == t.hubPort
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone before mutating headers: RoundTrippers must not modify the caller's
	// request, and the redirect machinery reuses it across hops.
	r := req.Clone(req.Context())
	r.Header = req.Header.Clone()
	if r.Header.Get("User-Agent") == "" {
		r.Header.Set("User-Agent", t.ua)
	}
	if t.token != "" && t.sameHubOrigin(r.URL) {
		r.Header.Set("Authorization", "Bearer "+t.token)
	} else {
		r.Header.Del("Authorization")
	}
	return t.base.RoundTrip(r)
}
