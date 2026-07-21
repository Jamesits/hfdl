package hfapi

import (
	"os"
	"path/filepath"
	"testing"
)

func envFunc(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolveToken(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token-file")
	if err := os.WriteFile(tokenPath, []byte("  path-tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	hfHome := filepath.Join(dir, "hf-home")
	if err := os.MkdirAll(hfHome, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hfHome, "token"), []byte("home-tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// $XDG_CACHE_HOME/huggingface/token — the implicit fallback when HF_HOME is unset.
	xdgCache := filepath.Join(dir, "xdg-cache")
	if err := os.MkdirAll(filepath.Join(xdgCache, "huggingface"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(xdgCache, "huggingface", "token"), []byte("xdg-tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// ~/.cache/huggingface/token — the default when neither HF_HOME nor XDG_CACHE_HOME is set.
	homeDir := filepath.Join(dir, "home")
	if err := os.MkdirAll(filepath.Join(homeDir, ".cache", "huggingface"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(homeDir, ".cache", "huggingface", "token"), []byte("default-home-tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		flag string
		env  map[string]string
		want string
	}{
		{name: "flag wins", flag: "flag-tok",
			env:  map[string]string{"HF_TOKEN": "env-tok", "HF_TOKEN_PATH": tokenPath},
			want: "flag-tok"},
		{name: "env wins over path file",
			env:  map[string]string{"HF_TOKEN": "env-tok", "HF_TOKEN_PATH": tokenPath},
			want: "env-tok"},
		{name: "path file trimmed",
			env:  map[string]string{"HF_TOKEN_PATH": tokenPath},
			want: "path-tok"},
		{name: "unreadable path file falls through to HF_HOME",
			env:  map[string]string{"HF_TOKEN_PATH": filepath.Join(dir, "missing"), "HF_HOME": hfHome},
			want: "home-tok"},
		{name: "HF_HOME token trimmed",
			env:  map[string]string{"HF_HOME": hfHome},
			want: "home-tok"},
		{name: "implicit falls back to XDG_CACHE_HOME/huggingface when HF_HOME unset",
			env:  map[string]string{"XDG_CACHE_HOME": xdgCache},
			want: "xdg-tok"},
		{name: "implicit falls back to ~/.cache/huggingface when HF_HOME and XDG unset",
			env:  map[string]string{"HOME": homeDir},
			want: "default-home-tok"},
		{name: "HF_HOME wins over XDG_CACHE_HOME",
			env:  map[string]string{"HF_HOME": hfHome, "XDG_CACHE_HOME": xdgCache},
			want: "home-tok"},
		{name: "default implicit lookup honors disable flag",
			env:  map[string]string{"HOME": homeDir, "HF_HUB_DISABLE_IMPLICIT_TOKEN": "1"},
			want: ""},
		{name: "implicit token disabled",
			env:  map[string]string{"HF_HOME": hfHome, "HF_HUB_DISABLE_IMPLICIT_TOKEN": "true"},
			want: ""},
		{name: "implicit token disabled=1",
			env:  map[string]string{"HF_HOME": hfHome, "HF_HUB_DISABLE_IMPLICIT_TOKEN": "1"},
			want: ""},
		{name: "explicit sources win over disable flag",
			env:  map[string]string{"HF_TOKEN": "env-tok", "HF_HOME": hfHome, "HF_HUB_DISABLE_IMPLICIT_TOKEN": "true"},
			want: "env-tok"},
		{name: "nothing", env: map[string]string{}, want: ""},
		{name: "empty flag falls through", flag: "",
			env:  map[string]string{"HF_TOKEN": "env-tok"},
			want: "env-tok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveToken(tc.flag, envFunc(tc.env)); got != tc.want {
				t.Errorf("ResolveToken() = %q, want %q", got, tc.want)
			}
		})
	}
}
