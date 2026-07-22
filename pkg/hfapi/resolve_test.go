package hfapi

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResolveURL(t *testing.T) {
	c := NewClient(slog.New(slog.DiscardHandler), &http.Client{}, "https://hf.example.co", "", 0)
	cases := []struct {
		rt              RepoType
		repo, rev, path string
		want            string
	}{
		{RepoTypeModel, "org/repo", "main", "f.bin", "https://hf.example.co/org/repo/resolve/main/f.bin"},
		// rev with slashes is ONE percent-encoded path component
		{RepoTypeModel, "org/repo", "refs/pr/3", "f.bin", "https://hf.example.co/org/repo/resolve/refs%2Fpr%2F3/f.bin"},
		// path keeps its separators, segments escaped
		{RepoTypeModel, "org/repo", "main", "sub dir/f.bin", "https://hf.example.co/org/repo/resolve/main/sub%20dir/f.bin"},
		{RepoTypeDataset, "org/repo", "main", "f.bin", "https://hf.example.co/datasets/org/repo/resolve/main/f.bin"},
		{RepoTypeSpace, "org/repo", "main", "f.bin", "https://hf.example.co/spaces/org/repo/resolve/main/f.bin"},
	}
	for _, tc := range cases {
		if got := c.ResolveURL(tc.rt, tc.repo, tc.rev, tc.path); got != tc.want {
			t.Errorf("ResolveURL(%s, %s, %s, %s) = %q, want %q", tc.rt, tc.repo, tc.rev, tc.path, got, tc.want)
		}
	}
}

func TestResolveXetRefreshRoutePrecedence(t *testing.T) {
	ctx := t.Context()
	mux := http.NewServeMux()
	mux.HandleFunc("/o/r/resolve/main/both.bin", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method = %s, want HEAD", r.Method)
		}
		w.Header().Set("X-Xet-Hash", "xethash1")
		w.Header().Set("Link", `<https://cas-auth.example/route-a>; rel="xet-auth"`)
		w.Header().Set("X-Xet-Refresh-Route", "/route-b")
		w.Header().Set("Location", "/follow-target")
		w.WriteHeader(http.StatusFound) // hub-style 302; must NOT be followed
	})
	mux.HandleFunc("/o/r/resolve/main/header-only.bin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Xet-Hash", "xethash2")
		w.Header().Set("X-Xet-Refresh-Route", "/route-b")
	})
	mux.HandleFunc("/o/r/resolve/main/plain.bin", func(w http.ResponseWriter, r *http.Request) {
		// no xet headers at all
	})
	mux.HandleFunc("/follow-target", func(w http.ResponseWriter, r *http.Request) {
		t.Error("redirect was followed; xet headers live on the hub response")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := newTestClient(t, srv.URL, "tok", 0)

	d, err := c.ResolveXet(ctx, RepoTypeModel, "o/r", "main", "both.bin")
	if err != nil {
		t.Fatal(err)
	}
	if d.Hash != "xethash1" {
		t.Errorf("Hash = %q", d.Hash)
	}
	// Link rel="xet-auth" wins over X-Xet-Refresh-Route
	if d.RefreshRoute != "https://cas-auth.example/route-a" {
		t.Errorf("RefreshRoute = %q", d.RefreshRoute)
	}

	d, err = c.ResolveXet(ctx, RepoTypeModel, "o/r", "main", "header-only.bin")
	if err != nil {
		t.Fatal(err)
	}
	if d.RefreshRoute != "/route-b" {
		t.Errorf("RefreshRoute = %q, want header fallback", d.RefreshRoute)
	}

	d, err = c.ResolveXet(ctx, RepoTypeModel, "o/r", "main", "plain.bin")
	if err != nil {
		t.Fatal(err)
	}
	if d.Hash != "" || d.RefreshRoute != "" {
		t.Errorf("plain file got xet data %+v", d)
	}
}

func TestXetToken(t *testing.T) {
	ctx := t.Context()
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("xet token request missing auth: %q", r.Header.Get("Authorization"))
		}
		writeBody(t, w, `{"accessToken":"cas-tok","exp":1700000000,"casUrl":"https://cas.example"}`)
	}
	mux.HandleFunc("/xet-route", handler)
	mux.HandleFunc("/abs-route", handler)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := newTestClient(t, srv.URL, "tok", 0)

	tok, err := c.XetToken(ctx, "/xet-route")
	if err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken != "cas-tok" || tok.CasURL != "https://cas.example" {
		t.Errorf("tok = %+v", tok)
	}
	if !tok.Exp.Equal(time.Unix(1700000000, 0)) {
		t.Errorf("Exp = %v", tok.Exp)
	}

	// absolute routes (from Link rel="xet-auth") are used verbatim
	tok, err = c.XetToken(ctx, srv.URL+"/abs-route")
	if err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken != "cas-tok" {
		t.Errorf("tok = %+v", tok)
	}
}
