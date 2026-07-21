package hfapi

import (
	"os"
	"path/filepath"
	"strings"
)

// ResolveToken resolves the Hub token in order: --token flag -> HF_TOKEN ->
// HF_TOKEN_PATH (file, trimmed) -> <HF home>/token. The implicit file lookup
// is skipped when HF_HUB_DISABLE_IMPLICIT_TOKEN is truthy. Unreadable files
// fall through to the next source; "" means anonymous. getenv is injected so
// tests never touch the real environment.
func ResolveToken(flag string, getenv func(string) string) string {
	if flag != "" {
		return flag
	}
	if t := getenv("HF_TOKEN"); t != "" {
		return t
	}
	if p := getenv("HF_TOKEN_PATH"); p != "" {
		if b, err := os.ReadFile(p); err == nil {
			if t := strings.TrimSpace(string(b)); t != "" {
				return t
			}
		}
	}
	if isTruthyEnv(getenv("HF_HUB_DISABLE_IMPLICIT_TOKEN")) {
		return ""
	}
	if home := hfHome(getenv); home != "" {
		if b, err := os.ReadFile(filepath.Join(home, "token")); err == nil {
			return strings.TrimSpace(string(b))
		}
	}
	return ""
}

// hfHome resolves the huggingface_hub HF_HOME directory the same way the pin
// does: $HF_HOME, else $XDG_CACHE_HOME/huggingface, else ~/.cache/huggingface.
// Without HF_HOME the implicit token still lives at the default cache location
// — anything else leaves typical users anonymous. Returns "" when no home can
// be derived (so a fully empty environment reads nothing). Computed purely
// from getenv, never os.UserHomeDir, so injected-env tests stay hermetic.
func hfHome(getenv func(string) string) string {
	if home := getenv("HF_HOME"); home != "" {
		return home
	}
	cacheBase := getenv("XDG_CACHE_HOME")
	if cacheBase == "" {
		if h := userHome(getenv); h != "" {
			cacheBase = filepath.Join(h, ".cache")
		}
	}
	if cacheBase == "" {
		return ""
	}
	return filepath.Join(cacheBase, "huggingface")
}

// userHome mirrors os.UserHomeDir but through the injected getenv: $HOME on
// unix, %USERPROFILE% on Windows.
func userHome(getenv func(string) string) string {
	if h := getenv("HOME"); h != "" {
		return h
	}
	if h := getenv("USERPROFILE"); h != "" {
		return h
	}
	return ""
}

func isTruthyEnv(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
