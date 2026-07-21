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
