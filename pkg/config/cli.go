package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// DefaultEndpoint is the Hugging Face Hub base URL.
const DefaultEndpoint = "https://huggingface.co"

// CLI is the parsed flag/env model for `hfdl download`: the flag surface
// mirrors `hf download` plus hfdl extensions (multi-endpoint, references,
// state DB, log level, no-TUI).
// Env resolution helpers take an explicit getenv so they stay testable and
// packages below cmd never touch os.Getenv themselves.
type CLI struct {
	// positional
	RepoID    string // may be an hf:// URI; parsed by hfapi.ParseRepoRef
	Filenames []string

	// upstream parity flags
	RepoType      string // model|dataset|space
	Revision      string
	Include       []string
	Exclude       []string
	LocalDir      string
	CacheDir      string
	Token         string
	Quiet         bool
	ForceDownload bool
	DryRun        bool
	MaxWorkers    int

	// hfdl extensions
	Endpoints  []string
	References []string
	StateDB    string
	LogLevel   string
	NoTUI      bool
}

// CacheDir resolves --cache-dir → HF_HUB_CACHE → <HF_HOME>/hub (huggingface_hub
// parity; see constants.py HF_HUB_CACHE / HF_HOME).
func CacheDir(flag string, getenv func(string) string) string {
	if flag != "" {
		return flag
	}
	if v := getenv("HF_HUB_CACHE"); v != "" {
		return v
	}
	return filepath.Join(hfHome(getenv), "hub")
}

// hfHome mirrors huggingface_hub constants.HF_HOME: $HF_HOME →
// $XDG_CACHE_HOME/huggingface → ~/.cache/huggingface. The XDG_CACHE_HOME step
// matters on Linux, where a user may relocate the whole cache tree via XDG
// without setting any HF_* variable.
func hfHome(getenv func(string) string) string {
	if home := getenv("HF_HOME"); home != "" {
		return home
	}
	if xdg := getenv("XDG_CACHE_HOME"); xdg != "" {
		return filepath.Join(xdg, "huggingface")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".cache", "huggingface")
	}
	return filepath.Join(".cache", "huggingface")
}

// Endpoints resolves repeatable --endpoint → HF_ENDPOINT → DefaultEndpoint.
func Endpoints(flag []string, getenv func(string) string) []string {
	if len(flag) > 0 {
		return flag
	}
	if v := getenv("HF_ENDPOINT"); v != "" {
		return []string{strings.TrimRight(v, "/")}
	}
	return []string{DefaultEndpoint}
}

// StateDBPath resolves --state-db → <cache>/.hfdl/state.db.
func StateDBPath(flag, cacheDir string) string {
	if flag != "" {
		return flag
	}
	return filepath.Join(cacheDir, ".hfdl", "state.db")
}

// Offline reports HF_HUB_OFFLINE truthiness (cache hits served, network fails).
func Offline(getenv func(string) string) bool {
	return truthy(getenv("HF_HUB_OFFLINE"))
}

func truthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// envDuration parses a huggingface_hub-style timeout env var: a bare float
// number of seconds ("10", "2.5") or a Go duration string ("10s").
func envDuration(getenv func(string) string, key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(getenv(key))
	if v == "" {
		return def
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return time.Duration(f * float64(time.Second))
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	return def
}

// ETagTimeout is the metadata HEAD timeout (HF_HUB_ETAG_TIMEOUT, default 10s).
func ETagTimeout(getenv func(string) string) time.Duration {
	return envDuration(getenv, "HF_HUB_ETAG_TIMEOUT", 10*time.Second)
}

// DownloadTimeout is the download response timeout (HF_HUB_DOWNLOAD_TIMEOUT,
// default 10s).
func DownloadTimeout(getenv func(string) string) time.Duration {
	return envDuration(getenv, "HF_HUB_DOWNLOAD_TIMEOUT", 10*time.Second)
}

// ParseLogLevel converts the --log-level flag to an slog level value.
func ParseLogLevel(s string) (int, error) {
	switch strings.ToLower(s) {
	case "debug":
		return -4, nil
	case "info":
		return 0, nil
	case "warn", "warning":
		return 4, nil
	case "error":
		return 8, nil
	}
	return 0, fmt.Errorf("invalid log-level %q: want debug|info|warn|error", s)
}
