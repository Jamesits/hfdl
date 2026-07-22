package netcfg

import (
	"log/slog"
	"net/http"
	"testing"
)

func TestParse(t *testing.T) {
	cases := []struct {
		spec     string
		mode     ProxyMode
		wantURL  string
		wantSpec string
		wantErr  bool
	}{
		{spec: "system", mode: ProxySystem, wantSpec: "system"},
		{spec: "", mode: ProxySystem, wantSpec: "system"},
		{spec: "  SYSTEM ", mode: ProxySystem, wantSpec: "system"},
		{spec: "direct", mode: ProxyDirect, wantSpec: "direct"},
		{spec: "Direct", mode: ProxyDirect, wantSpec: "direct"},
		{spec: "http://proxy.example:8080", mode: ProxyURL, wantURL: "http://proxy.example:8080", wantSpec: "http://proxy.example:8080"},
		{spec: "https://user:pass@proxy.example", mode: ProxyURL, wantURL: "https://user:pass@proxy.example", wantSpec: "https://user:pass@proxy.example"},
		{spec: "socks5://127.0.0.1:1080", mode: ProxyURL, wantURL: "socks5://127.0.0.1:1080", wantSpec: "socks5://127.0.0.1:1080"},
		{spec: "socks5h://proxy.example:1080", mode: ProxyURL, wantURL: "socks5h://proxy.example:1080", wantSpec: "socks5h://proxy.example:1080"},
		{spec: "ftp://proxy.example", wantErr: true},
		{spec: "proxy.example:8080", wantErr: true}, // no scheme
		{spec: "http://", wantErr: true},            // no host
		{spec: "socks4://proxy.example", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.spec, func(t *testing.T) {
			c, err := ParseProxy(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseProxy(%q): expected error, got %+v", tc.spec, c)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseProxy(%q): %v", tc.spec, err)
			}
			if c.Mode != tc.mode {
				t.Fatalf("mode = %v, want %v", c.Mode, tc.mode)
			}
			if c.String() != tc.wantSpec {
				t.Fatalf("String() = %q, want %q", c.String(), tc.wantSpec)
			}
			if tc.wantURL != "" && c.URL.String() != tc.wantURL {
				t.Fatalf("URL = %q, want %q", c.URL, tc.wantURL)
			}
		})
	}
}

func discardLog() *slog.Logger { return slog.New(slog.DiscardHandler) }

func mustReq(t *testing.T, u string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestProxyFuncDirect(t *testing.T) {
	c, err := ParseProxy("direct")
	if err != nil {
		t.Fatal(err)
	}
	if fn := c.ProxyFunc(t.Context(), discardLog()); fn != nil {
		t.Fatal("direct: expected nil proxy func")
	}
}

func TestProxyFuncURL(t *testing.T) {
	c, err := ParseProxy("socks5h://proxy.example:1080")
	if err != nil {
		t.Fatal(err)
	}
	fn := c.ProxyFunc(t.Context(), discardLog())
	if fn == nil {
		t.Fatal("expected proxy func")
	}
	got, err := fn(mustReq(t, "https://huggingface.co/x"))
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != "socks5h://proxy.example:1080" {
		t.Fatalf("proxy = %v", got)
	}
}

func TestProxyFuncSystemEnv(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://envproxy.example:3128")
	t.Setenv("HTTPS_PROXY", "http://envproxy.example:3128")
	t.Setenv("NO_PROXY", "internal.example")

	c, err := ParseProxy("system")
	if err != nil {
		t.Fatal(err)
	}
	fn := c.ProxyFunc(t.Context(), discardLog())
	if fn == nil {
		t.Fatal("expected proxy func from environment")
	}
	got, err := fn(mustReq(t, "https://huggingface.co/x"))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Host != "envproxy.example:3128" {
		t.Fatalf("proxy = %v", got)
	}
	got, err = fn(mustReq(t, "https://internal.example/x"))
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("NO_PROXY host should bypass, got %v", got)
	}
}

// requestProxy behavior for translated platform configs: bypass matching and
// socks fallback flow through httpproxy.
func TestRequestProxyFromWinhttpConfig(t *testing.T) {
	fn := requestProxy(winhttpConfig("http=p1:80;https=p2:443", "*.example.com;<local>"))
	u, err := fn(mustReq(t, "https://huggingface.co/x"))
	if err != nil {
		t.Fatal(err)
	}
	if u == nil || u.Host != "p2:443" {
		t.Fatalf("https proxy = %v", u)
	}
	u, err = fn(mustReq(t, "http://sub.example.com/x"))
	if err != nil {
		t.Fatal(err)
	}
	if u != nil {
		t.Fatalf("bypassed host got proxy %v", u)
	}
	u, err = fn(mustReq(t, "http://localhost:8080/x"))
	if err != nil {
		t.Fatal(err)
	}
	if u != nil {
		t.Fatalf("<local> localhost got proxy %v", u)
	}
}

func TestWinhttpConfig(t *testing.T) {
	cases := []struct {
		name              string
		proxy, bypass     string
		wantHTTP, wantTLS string
		wantNoProxy       string
	}{
		{
			name:  "bare host applies to all",
			proxy: "proxy:8080", wantHTTP: "proxy:8080", wantTLS: "proxy:8080",
		},
		{
			name:     "per scheme",
			proxy:    "http=p1:80;https=p2:443;socks=p3:1080",
			wantHTTP: "p1:80", wantTLS: "p2:443",
		},
		{
			name:     "socks fallback fills gaps",
			proxy:    "http=p1:80;socks=p3:1080",
			wantHTTP: "p1:80", wantTLS: "socks5://p3:1080",
		},
		{
			name:     "explicit beats bare",
			proxy:    "https=p2:443;proxy:8080",
			wantHTTP: "proxy:8080", wantTLS: "p2:443",
		},
		{
			name:  "bypass list",
			proxy: "proxy:8080", bypass: "*.internal.example;10.0.0.1 <local>",
			wantHTTP: "proxy:8080", wantTLS: "proxy:8080",
			wantNoProxy: "*.internal.example,10.0.0.1,localhost,127.0.0.1,::1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := winhttpConfig(tc.proxy, tc.bypass)
			if cfg.HTTPProxy != tc.wantHTTP || cfg.HTTPSProxy != tc.wantTLS {
				t.Fatalf("proxies = %q/%q, want %q/%q", cfg.HTTPProxy, cfg.HTTPSProxy, tc.wantHTTP, tc.wantTLS)
			}
			if cfg.NoProxy != tc.wantNoProxy {
				t.Fatalf("NoProxy = %q, want %q", cfg.NoProxy, tc.wantNoProxy)
			}
		})
	}
}

func TestScutilConfig(t *testing.T) {
	const out = `<dictionary> {
  ExceptionsList : <array> {
    0 : *.local
    1 : 169.254/16
  }
  FTPPassive : 1
  HTTPEnable : 1
  HTTPPort : 3128
  HTTPProxy : 192.168.1.10
  HTTPSEnable : 0
  HTTPSPort : 8443
  HTTPSProxy : ignored.example
  SOCKSEnable : 1
  SOCKSPort : 1080
  SOCKSProxy : 192.168.1.11
}`
	cfg, pac := scutilConfig(out)
	if pac {
		t.Fatal("pac = true")
	}
	if cfg.HTTPProxy != "192.168.1.10:3128" {
		t.Fatalf("HTTPProxy = %q", cfg.HTTPProxy)
	}
	// HTTPS proxy disabled → SOCKS fills the gap.
	if cfg.HTTPSProxy != "socks5://192.168.1.11:1080" {
		t.Fatalf("HTTPSProxy = %q", cfg.HTTPSProxy)
	}
	if cfg.NoProxy != "*.local,169.254/16" {
		t.Fatalf("NoProxy = %q", cfg.NoProxy)
	}
}

func TestScutilConfigPAC(t *testing.T) {
	const out = `<dictionary> {
  ProxyAutoConfigEnable : 1
  ProxyAutoConfigURLString : http://wpad.example/wpad.dat
}`
	cfg, pac := scutilConfig(out)
	if !pac {
		t.Fatal("pac = false")
	}
	if cfg.HTTPProxy != "" || cfg.HTTPSProxy != "" {
		t.Fatalf("cfg = %+v", cfg)
	}
}
