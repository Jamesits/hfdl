package hfapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ResolveURL builds {endpoint}/{repo}/resolve/{rev}/{path} (datasets/spaces
// gain their /datasets//spaces/ website prefix). rev is percent-encoded as
// ONE path component (refs/pr/3 -> refs%2Fpr%2F3) so refs containing slashes
// survive; path keeps its separators with each segment escaped.
func (c *Client) ResolveURL(rt RepoType, repo, rev, path string) string {
	return fmt.Sprintf("%s/%s%s/resolve/%s/%s",
		c.endpoint, rt.urlPrefix(), escPath(repo), url.PathEscape(rev), escPath(path))
}

// ResolveXet issues a HEAD against the resolve URL and reports xet CAS
// metadata. The redirect is deliberately NOT followed: the Hub attaches
// X-Xet-Hash and the refresh route to its own response (usually a 302 to a
// CDN), which the final hop would no longer carry. RefreshRoute prefers the
// Link header's rel="xet-auth" URL over X-Xet-Refresh-Route.
func (c *Client) ResolveXet(ctx context.Context, rt RepoType, repo, rev, path string) (*XetFileData, error) {
	ctx, sp := c.startSpan(ctx, "ResolveXet")
	defer endSpan(sp)

	ctx, cancel := c.withEtagTimeout(ctx)
	defer cancel()

	u := c.ResolveURL(rt, repo, rev, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u, nil)
	if err != nil {
		return nil, fmt.Errorf("hfapi: build request HEAD %s: %w", u, err)
	}
	resp, err := c.hcNoFollow.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hfapi: HEAD %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, statusError(resp, repo, rev)
	}
	data := &XetFileData{Hash: resp.Header.Get("X-Xet-Hash")}
	if route := linkRel(resp.Header.Get("Link"), "xet-auth"); route != "" {
		data.RefreshRoute = route
	} else {
		data.RefreshRoute = resp.Header.Get("X-Xet-Refresh-Route")
	}
	return data, nil
}

// XetToken fetches a CAS bearer token: GET {endpoint}{route} for path-style
// routes (X-Xet-Refresh-Route), or the route verbatim when it is absolute
// (Link rel="xet-auth" URLs). The caller caches the token until Exp.
func (c *Client) XetToken(ctx context.Context, refreshRoute string) (*XetToken, error) {
	ctx, sp := c.startSpan(ctx, "XetToken")
	defer endSpan(sp)

	u := refreshRoute
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		if !strings.HasPrefix(u, "/") {
			u = "/" + u
		}
		u = c.endpoint + u
	}
	resp, err := c.do(ctx, http.MethodGet, u, nil, "", "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var raw struct {
		AccessToken string `json:"accessToken"`
		Exp         int64  `json:"exp"` // unix seconds
		CasURL      string `json:"casUrl"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("hfapi: decode xet token: %w", err)
	}
	return &XetToken{
		AccessToken: raw.AccessToken,
		Exp:         time.Unix(raw.Exp, 0),
		CasURL:      raw.CasURL,
	}, nil
}
