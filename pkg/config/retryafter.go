package config

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// MaxRetryAfter caps how long a server-supplied Retry-After is honored.
// Unbounded values are both a DoS vector (a hostile/broken upstream parking
// the client for years) and an overflow hazard: seconds × time.Second
// overflows time.Duration past ~292 years, flipping cooldowns negative and
// defeating them entirely.
const MaxRetryAfter = 24 * time.Hour

// ParseRetryAfter interprets a Retry-After header value: either delta-seconds
// or an HTTP-date. Unparseable/absent yields 0; the result is clamped to
// [0, MaxRetryAfter]. One implementation shared by hfapi, xet and transfer so
// the clamping policy cannot drift.
func ParseRetryAfter(h string) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	if n, err := strconv.ParseInt(h, 10, 64); err == nil {
		if n <= 0 {
			return 0
		}
		if n > int64(MaxRetryAfter/time.Second) {
			return MaxRetryAfter
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		return min(max(time.Until(t), 0), MaxRetryAfter)
	}
	return 0
}
