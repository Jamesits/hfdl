package config

import (
	"path/filepath"
	"testing"
	"time"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestBandwidthBurst(t *testing.T) {
	if got := BandwidthBurst(0); got != 0 {
		t.Fatalf("unlimited burst = %d, want 0", got)
	}
	if got := BandwidthBurst(4 << 20); got != bandwidthMinBurst {
		t.Fatalf("small limit burst = %d, want floor %d", got, bandwidthMinBurst)
	}
	if got := BandwidthBurst(64 << 20); got != 32<<20 {
		t.Fatalf("burst = %d, want limit/2", got)
	}
}

func TestCacheDirPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag string
		env  map[string]string
		want string
	}{
		{"flag wins", "/flag", map[string]string{"HF_HUB_CACHE": "/env", "HF_HOME": "/home"}, "/flag"},
		{"HF_HUB_CACHE", "", map[string]string{"HF_HUB_CACHE": "/env", "HF_HOME": "/home"}, "/env"},
		{"HF_HOME hub subdir", "", map[string]string{"HF_HOME": "/home"}, filepath.Join("/home", "hub")},
		{"HF_HOME beats XDG_CACHE_HOME", "", map[string]string{"HF_HOME": "/home", "XDG_CACHE_HOME": "/xdg"}, filepath.Join("/home", "hub")},
		{"XDG_CACHE_HOME fallback", "", map[string]string{"XDG_CACHE_HOME": "/xdg"}, filepath.Join("/xdg", "huggingface", "hub")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := CacheDir(tc.flag, envFrom(tc.env)); got != tc.want {
				t.Fatalf("CacheDir(%q) = %q, want %q", tc.flag, got, tc.want)
			}
		})
	}
	// No env: falls under the user cache dir; must contain huggingface/hub.
	got := CacheDir("", envFrom(nil))
	if filepath.Base(filepath.Dir(got)) != "huggingface" || filepath.Base(got) != "hub" {
		t.Fatalf("default cache dir shape: %q", got)
	}
}

func TestEndpointsResolution(t *testing.T) {
	if got := Endpoints([]string{"https://a/", "https://b///"}, envFrom(map[string]string{"HF_ENDPOINT": "https://env"})); len(got) != 2 || got[0] != "https://a" || got[1] != "https://b" {
		t.Fatalf("flag endpoints: %v", got)
	}
	if got := Endpoints(nil, envFrom(map[string]string{"HF_ENDPOINT": "https://env/"})); len(got) != 1 || got[0] != "https://env" {
		t.Fatalf("env endpoint (trailing slash trimmed): %v", got)
	}
	if got := Endpoints(nil, envFrom(nil)); len(got) != 1 || got[0] != DefaultEndpoint {
		t.Fatalf("default endpoint: %v", got)
	}
}

func TestStateDBPath(t *testing.T) {
	if got := StateDBPath("/x.db", "/cache"); got != "/x.db" {
		t.Fatalf("flag: %q", got)
	}
	want := filepath.Join("/cache", ".hfdl", "state.db")
	if got := StateDBPath("", "/cache"); got != want {
		t.Fatalf("default: %q want %q", got, want)
	}
}

func TestEnvDurations(t *testing.T) {
	env := envFrom(map[string]string{
		"HF_HUB_ETAG_TIMEOUT":     "2.5",
		"HF_HUB_DOWNLOAD_TIMEOUT": "30s",
	})
	if got := ETagTimeout(env); got != 2500*time.Millisecond {
		t.Fatalf("etag timeout float-seconds: %v", got)
	}
	if got := DownloadTimeout(env); got != 30*time.Second {
		t.Fatalf("download timeout duration string: %v", got)
	}
	if got := ETagTimeout(envFrom(map[string]string{"HF_HUB_ETAG_TIMEOUT": "bogus"})); got != 10*time.Second {
		t.Fatalf("invalid falls back to default: %v", got)
	}
	if got := DownloadTimeout(envFrom(nil)); got != 10*time.Second {
		t.Fatalf("default: %v", got)
	}
}

func TestOffline(t *testing.T) {
	for v, want := range map[string]bool{"1": true, "true": true, "TRUE": true, "yes": true, "0": false, "false": false, "": false} {
		if got := Offline(envFrom(map[string]string{"HF_HUB_OFFLINE": v})); got != want {
			t.Fatalf("Offline(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestParseSizeAndRate(t *testing.T) {
	for in, want := range map[string]int64{
		"32KiB": 32 * 1024, "8MiB": 8 << 20, "1GB": 1_000_000_000, "1048576": 1 << 20, "512": 512,
	} {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Fatalf("ParseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	if got, err := ParseRate("500MiB/s"); err != nil || got != 500<<20 {
		t.Fatalf("ParseRate: %d, %v", got, err)
	}
	if got, err := ParseRate("100MB"); err != nil || got != 100_000_000 {
		t.Fatalf("ParseRate no suffix: %d, %v", got, err)
	}
	if _, err := ParseSize("lots"); err == nil {
		t.Fatal("invalid size accepted")
	}
	if _, err := ParseSize(""); err == nil {
		t.Fatal("empty size accepted")
	}
}

func TestParseEnums(t *testing.T) {
	if m, err := ParseIOMode("direct"); err != nil || m != IODirect {
		t.Fatalf("io-mode: %v %v", m, err)
	}
	if _, err := ParseIOMode("warp"); err == nil {
		t.Fatal("bad io-mode accepted")
	}
	if p, err := ParseUpstreamPolicy("round-robin"); err != nil || p != RoundRobin {
		t.Fatalf("policy: %v %v", p, err)
	}
	if _, err := ParseUpstreamPolicy("chaos"); err == nil {
		t.Fatal("bad policy accepted")
	}
	if s, err := ParseSourcePriority("cdn"); err != nil || s != PreferCDN {
		t.Fatalf("source-priority: %v %v", s, err)
	}
	if s, err := ParseSourcePriority("xet"); err != nil || s != PreferXet {
		t.Fatalf("source-priority: %v %v", s, err)
	}
	if _, err := ParseSourcePriority("torrent"); err == nil {
		t.Fatal("bad source-priority accepted")
	}
	noenv := func(string) string { return "" }
	disabled := func(k string) string {
		if k == "HF_HUB_DISABLE_XET" {
			return "1"
		}
		return ""
	}
	// flag empty + no env → default xet; env disables → cdn; explicit flag wins.
	if s, err := ResolveSourcePriority("", noenv); err != nil || s != PreferXet {
		t.Fatalf("resolve default: %v %v", s, err)
	}
	if s, err := ResolveSourcePriority("", disabled); err != nil || s != PreferCDN {
		t.Fatalf("resolve HF_HUB_DISABLE_XET: %v %v", s, err)
	}
	if s, err := ResolveSourcePriority("xet", disabled); err != nil || s != PreferXet {
		t.Fatalf("resolve flag beats env: %v %v", s, err)
	}
	if _, err := ResolveSourcePriority("torrent", noenv); err == nil {
		t.Fatal("bad source-priority flag accepted")
	}
	if l, err := ParseLogLevel("warn"); err != nil || l != 4 {
		t.Fatalf("level: %v %v", l, err)
	}
	if _, err := ParseLogLevel("shout"); err == nil {
		t.Fatal("bad level accepted")
	}
}

func TestLimitsValidate(t *testing.T) {
	ok := DefaultLimits()
	if err := ok.Validate(); err != nil {
		t.Fatalf("defaults valid: %v", err)
	}
	bad := DefaultLimits()
	bad.DiskActivePct = 101
	if err := bad.Validate(); err == nil {
		t.Fatal("disk-active 101 accepted")
	}
	bad = DefaultLimits()
	bad.Conns = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("connections 0 accepted")
	}
	bad = DefaultLimits()
	bad.DiskWorkers = -1
	if err := bad.Validate(); err == nil {
		t.Fatal("disk-workers -1 accepted")
	}
	// 0 (auto) and positive overrides are valid; the runtime cap, not
	// Validate, bounds the upper end.
	ok = DefaultLimits()
	ok.DiskWorkers = 16
	if err := ok.Validate(); err != nil {
		t.Fatalf("disk-workers 16 rejected: %v", err)
	}
}
