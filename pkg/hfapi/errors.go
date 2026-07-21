package hfapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxErrorBody caps how much of an error response body is read for messages.
const maxErrorBody = 4 << 10

// ErrInvalidRepoRef is wrapped by every ParseRepoRef failure.
var ErrInvalidRepoRef = errors.New("hfapi: invalid repo reference")

// RateLimitError is returned on HTTP 429. RetryAfter is parsed from the
// Retry-After header (delta-seconds or HTTP-date); 0 means absent/unparseable.
// The caller (sched) owns cooldown policy — hfapi only reports the hit.
type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("hfapi: rate limited (retry after %s)", e.RetryAfter)
}

// NotFoundError is returned on HTTP 404.
type NotFoundError struct {
	Repo, Revision string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("hfapi: not found: %s@%s", e.Repo, e.Revision)
}

// GatedError is returned on HTTP 403 (gated repo, or terms not accepted).
type GatedError struct {
	Repo string
}

func (e *GatedError) Error() string {
	return fmt.Sprintf("hfapi: gated repo: %s", e.Repo)
}

// AuthError is returned on HTTP 401.
type AuthError struct {
	Msg string
}

func (e *AuthError) Error() string {
	return "hfapi: authentication failed: " + e.Msg
}

// parseRetryAfter interprets a Retry-After header value: either
// delta-seconds or an HTTP-date. Unparseable/absent yields 0.
func parseRetryAfter(h string) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	if n, err := strconv.Atoi(h); err == nil {
		if n < 0 {
			return 0
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		return max(time.Until(t), 0)
	}
	return 0
}

// readErrMsg extracts a human-readable message from an error body: the
// Hub's {"error": "..."} shape when present, else the trimmed raw body.
func readErrMsg(resp *http.Response) string {
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	if err != nil || len(b) == 0 {
		return ""
	}
	var v struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(b, &v) == nil && v.Error != "" {
		return v.Error
	}
	return strings.TrimSpace(string(b))
}
