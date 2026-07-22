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
	Proxy      string // normalized --hfdl-proxy spec (netcfg.ParseProxy)
	IPQoS      string // normalized --hfdl-ipqos spec (netcfg.ParseIPQoS)
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
		out := make([]string, len(flag))
		for i, endpoint := range flag {
			out[i] = strings.TrimRight(endpoint, "/")
		}
		return out
	}
	if v := getenv("HF_ENDPOINT"); v != "" {
		return []string{strings.TrimRight(v, "/")}
	}
	return []string{DefaultEndpoint}
}

// WorkRoot is the per-download bookkeeping root: the directory the state DB and
// the blob store/staging are rooted at. In cache mode it is cacheDir; in
// --local-dir mode it is <localDir>/.cache/huggingface — hf already git-ignores
// and tags that directory, and it shares the local dir's filesystem, so the
// blob store's link(2)/reflink stay cheap. Rooting the DB (and its exclusive
// process lock) here is what lets independent downloads to different local dirs
// run concurrently. localDir, when non-empty, should already be absolute.
func WorkRoot(localDir, cacheDir string) string {
	if localDir != "" {
		return filepath.Join(localDir, ".cache", "huggingface")
	}
	return cacheDir
}

// StateDBPath resolves --state-db → <dest>/.hfdl/state.db, where dest is the
// per-download work root (see WorkRoot). Rooting the DB — and its exclusive
// process lock — under the destination is what lets independent downloads to
// different local dirs run concurrently.
func StateDBPath(flag, destDir string) string {
	if flag != "" {
		return flag
	}
	return filepath.Join(destDir, ".hfdl", "state.db")
}

// Offline reports HF_HUB_OFFLINE truthiness (cache hits served, network fails).
func Offline(getenv func(string) string) bool {
	return truthy(getenv("HF_HUB_OFFLINE"))
}

// DisableXet reports HF_HUB_DISABLE_XET truthiness (huggingface_hub parity):
// when set, xet transfers are disabled and every file is fetched from the CDN.
// It is the env-level equivalent of --hfdl-source-priority=cdn; an explicit
// flag still wins (see ResolveSourcePriority).
func DisableXet(getenv func(string) string) bool {
	return truthy(getenv("HF_HUB_DISABLE_XET"))
}

// ResolveSourcePriority combines the --hfdl-source-priority flag with the
// HF_HUB_DISABLE_XET env var, mirroring the flag-beats-env precedence of the
// other resolvers here: an explicitly set flag wins; otherwise
// HF_HUB_DISABLE_XET forces the CDN; otherwise xet is preferred when available.
func ResolveSourcePriority(flag string, getenv func(string) string) (SourcePriority, error) {
	if flag != "" {
		return ParseSourcePriority(flag)
	}
	if DisableXet(getenv) {
		return PreferCDN, nil
	}
	return PreferXet, nil
}

// TraceFCIODetail reports HFDL_TRACE_FCIO_DETAIL truthiness: the opt-in gate
// for fine-grained fcio.read/fcio.fsync spans. Off by default because those
// spans are per-file-pass diagnostic detail whose volume is unwanted in
// normal runs; they only emit when telemetry is also enabled.
func TraceFCIODetail(getenv func(string) string) bool {
	return truthy(getenv("HFDL_TRACE_FCIO_DETAIL"))
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
