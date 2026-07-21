package config

import (
	"fmt"
	"math"
	"strings"

	"github.com/dustin/go-humanize"
)

// ParseSize parses human sizes accepted by hf-style flags: "32KiB", "8MiB",
// "1GB", bare bytes. Binary (IEC) and SI units both accepted.
func ParseSize(s string) (int64, error) {
	v := strings.TrimSpace(s)
	if v == "" {
		return 0, fmt.Errorf("empty size")
	}
	n, err := humanize.ParseBytes(v)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", s, err)
	}
	if n > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("size %q overflows int64", s)
	}
	return int64(n), nil
}

// ParseRate parses a bandwidth limit: a size with an optional "/s" suffix
// ("500MiB/s", "100MB", "1048576"). Returns bytes/sec.
func ParseRate(s string) (int64, error) {
	v := strings.TrimSuffix(strings.TrimSpace(s), "/s")
	return ParseSize(v)
}

// FormatSize renders bytes for display (TUI, logs).
func FormatSize(n int64) string {
	return humanize.IBytes(uint64(n))
}
