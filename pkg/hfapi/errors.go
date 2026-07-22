package hfapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
)

// maxErrorBody caps how much of an error response body is read for messages.
const maxErrorBody = 4 << 10

var (
	// These deliberately redact common credential shapes while retaining the
	// surrounding upstream diagnostic text.
	bearerValue = regexp.MustCompile(`(?i)\bBearer\s+[^\s"'<>]+`)
	namedSecret = regexp.MustCompile(`(?i)((?:token|key|signature|credential)[^=&\s]{0,16}=)[A-Za-z0-9_+./%:-]{33,}`)
	urlQuery    = regexp.MustCompile(`(https?://[^\s?"'<>]+)\?[^\s"'<>]+`)
)

// SanitizeErrorText removes credential-like values and URL query strings
// from bounded upstream diagnostics while preserving useful context.
func SanitizeErrorText(s string) string {
	s = bearerValue.ReplaceAllString(s, "Bearer [REDACTED]")
	s = namedSecret.ReplaceAllString(s, `${1}[REDACTED]`)
	return urlQuery.ReplaceAllString(s, "$1?[REDACTED]")
}

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

// ParseRetryAfter interprets a Retry-After header value: either delta-seconds
// or an HTTP-date, clamped to [0, config.MaxRetryAfter]. Exported so xet
// (which maps CAS 429s onto hfapi.RateLimitError) keeps working against this
// package's API; the single implementation lives in config so transfer (which
// deliberately does not import hfapi) shares it too.
func ParseRetryAfter(h string) time.Duration {
	return config.ParseRetryAfter(h)
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
		return SanitizeErrorText(v.Error)
	}
	return SanitizeErrorText(strings.TrimSpace(string(b)))
}
