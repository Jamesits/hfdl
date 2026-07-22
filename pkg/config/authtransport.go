package config

import (
	"net/http"
	"strings"
)

// authTransport attaches the hfdl User-Agent to every request and, when a
// token is configured, the bearer token — but only to requests whose host
// matches hubHost. Authorization is stripped on every other host, so a
// redirect to a CDN/object store never leaks the token.
//
// Because redirects re-enter RoundTrip once per hop, the decision is
// re-evaluated per hop: the token is kept on the Hub host and stripped
// anywhere else. This deliberately does not rely on net/http's own
// redirect header-forwarding rules.
//
// A hubHost of "" matches no destination, so every request is stripped of
// Authorization: the defensive configuration for a client that must never
// carry credentials at all (transfer's payload downloads authenticate via
// presigned URLs, so they need — and must leak — no bearer token).
type authTransport struct {
	base    http.RoundTripper
	hubHost string
	token   string
	ua      string
}

// NewAuthTransport wraps base with the shared Hub auth/User-Agent policy so
// hfapi and transfer never re-implement the security-sensitive token-strip
// decision independently. A nil base uses http.DefaultTransport.
func NewAuthTransport(base http.RoundTripper, hubHost, token, ua string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &authTransport{base: base, hubHost: hubHost, token: token, ua: ua}
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone before mutating headers: RoundTrippers must not modify the caller's
	// request, and the redirect machinery reuses it across hops.
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
