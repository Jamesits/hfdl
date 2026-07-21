package hfapi

import (
	"os"
	"path/filepath"
	"strings"
)

// ResolveToken resolves the Hub token in order: --token flag -> HF_TOKEN ->
// HF_TOKEN_PATH (file, trimmed) -> $HF_HOME/token. The implicit file lookup
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
	if home := getenv("HF_HOME"); home != "" {
		if b, err := os.ReadFile(filepath.Join(home, "token")); err == nil {
			return strings.TrimSpace(string(b))
		}
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
