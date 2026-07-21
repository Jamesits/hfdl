package hfapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// maxPathsInfoChunk is the Hub's per-request paths-info path limit; larger
// input is chunked client-side and merged in request order.
const maxPathsInfoChunk = 1000

// RepoInfo fetches GET /api/{type}s/{repo}.
func (c *Client) RepoInfo(ctx context.Context, rt RepoType, repo string) (*RepoInfo, error) {
	ctx, sp := c.startSpan(ctx, "RepoInfo")
	defer endSpan(sp)

	plural, err := rt.plural()
	if err != nil {
		return nil, err
	}
	u := fmt.Sprintf("%s/api/%s/%s", c.endpoint, plural, escPath(repo))
	resp, err := c.do(ctx, http.MethodGet, u, nil, repo, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var raw repoInfoJSON
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("hfapi: decode repo info %s: %w", repo, err)
	}
	return raw.toRepoInfo(), nil
}

// Revision pins rev -> commit_sha (GET /api/{type}s/{repo}/revision/{rev}).
// Callers use the immutable sha for all subsequent listing/resolve calls so
// a moving branch cannot mix file versions mid-run.
func (c *Client) Revision(ctx context.Context, rt RepoType, repo, rev string) (commitSHA string, err error) {
	ctx, sp := c.startSpan(ctx, "Revision")
	defer endSpan(sp)

	plural, err := rt.plural()
	if err != nil {
		return "", err
	}
	u := fmt.Sprintf("%s/api/%s/%s/revision/%s", c.endpoint, plural, escPath(repo), url.PathEscape(rev))
	resp, err := c.do(ctx, http.MethodGet, u, nil, repo, rev)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var raw struct {
		SHA string `json:"sha"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return "", fmt.Errorf("hfapi: decode revision %s@%s: %w", repo, rev, err)
	}
	return raw.SHA, nil
}

// Tree lists a repo recursively (GET .../tree/{rev}/{path}?recursive=true).
// expand=false is deliberate: expanded pages are capped at 50 entries vs
// 1000 plain, and expansion only adds last-commit/security metadata hfdl
// never reads. Pagination follows the `Link: <...>; rel="next"` URL verbatim.
// Only files are returned; directory rows are dropped.
func (c *Client) Tree(ctx context.Context, rt RepoType, repo, rev, path string) ([]FileEntry, error) {
	ctx, sp := c.startSpan(ctx, "Tree")
	defer endSpan(sp)

	plural, err := rt.plural()
	if err != nil {
		return nil, err
	}
	u := fmt.Sprintf("%s/api/%s/%s/tree/%s", c.endpoint, plural, escPath(repo), url.PathEscape(rev))
	if path != "" {
		u += "/" + escPath(path)
	}
	u += "?recursive=true&expand=false"

	var out []FileEntry
	for u != "" {
		resp, err := c.do(ctx, http.MethodGet, u, nil, repo, rev)
		if err != nil {
			return nil, err
		}
		var page []entryJSON
		decErr := json.NewDecoder(resp.Body).Decode(&page)
		next := linkRel(resp.Header.Get("Link"), "next")
		closeErr := resp.Body.Close()
		if decErr != nil {
			return nil, fmt.Errorf("hfapi: decode tree page %s: %w", u, decErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("hfapi: close tree page %s: %w", u, closeErr)
		}
		for i := range page {
			if page[i].Type == "directory" {
				continue
			}
			out = append(out, page[i].toFileEntry())
		}
		u = next
	}
	return out, nil
}

// PathsInfo backfills hashes for known paths (POST .../paths-info/{rev}),
// e.g. entries discovered before a crash. More than maxPathsInfoChunk paths
// are chunked into multiple requests and merged in request order.
func (c *Client) PathsInfo(ctx context.Context, rt RepoType, repo, rev string, paths []string) ([]FileEntry, error) {
	ctx, sp := c.startSpan(ctx, "PathsInfo")
	defer endSpan(sp)

	plural, err := rt.plural()
	if err != nil {
		return nil, err
	}
	u := fmt.Sprintf("%s/api/%s/%s/paths-info/%s", c.endpoint, plural, escPath(repo), url.PathEscape(rev))

	out := make([]FileEntry, 0, len(paths))
	for start := 0; start < len(paths); start += maxPathsInfoChunk {
		chunk := paths[start:min(start+maxPathsInfoChunk, len(paths))]
		body, err := json.Marshal(struct {
			Paths []string `json:"paths"`
		}{Paths: chunk})
		if err != nil {
			return nil, fmt.Errorf("hfapi: encode paths-info request: %w", err)
		}
		resp, err := c.do(ctx, http.MethodPost, u, body, repo, rev)
		if err != nil {
			return nil, err
		}
		var entries []entryJSON
		decErr := json.NewDecoder(resp.Body).Decode(&entries)
		closeErr := resp.Body.Close()
		if decErr != nil {
			return nil, fmt.Errorf("hfapi: decode paths-info %s@%s: %w", repo, rev, decErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("hfapi: close paths-info %s@%s: %w", repo, rev, closeErr)
		}
		for i := range entries {
			if entries[i].Type == "directory" {
				continue
			}
			out = append(out, entries[i].toFileEntry())
		}
	}
	return out, nil
}
