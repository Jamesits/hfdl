package hfapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTreePaginationFollowsVerbatimNextLinks(t *testing.T) {
	ctx := t.Context()
	mux := http.NewServeMux()
	var srv *httptest.Server

	mux.HandleFunc("/api/models/o/r/tree/main", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("recursive") != "true" || r.URL.Query().Get("expand") != "false" {
			t.Errorf("query = %q, want recursive=true&expand=false", r.URL.RawQuery)
		}
		w.Header().Set("Link", fmt.Sprintf(`<%s/page-two?cursor=abc%%2Fdef>; rel="next"`, srv.URL))
		writeBody(t, w, `[
			{"type":"file","path":"a.bin","size":10,"oid":"sha1a"},
			{"type":"directory","path":"sub"}
		]`)
	})
	// page 2 is served ONLY at this exact URL: the client must follow the
	// Link target verbatim (escaped cursor included).
	mux.HandleFunc("/page-two", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "cursor=abc%2Fdef" {
			t.Errorf("page 2 RawQuery = %q, Link URL not honored verbatim", r.URL.RawQuery)
		}
		w.Header().Set("Link", fmt.Sprintf(`<%s/page-three>; rel="next"`, srv.URL))
		writeBody(t, w, `[{"type":"file","path":"b.bin","size":20,"oid":"sha1b","lfs":{"oid":"sha256b","size":20},"xetHash":"xetb"}]`)
	})
	mux.HandleFunc("/page-three", func(w http.ResponseWriter, r *http.Request) {
		writeBody(t, w, `[{"type":"file","path":"c.bin","size":30,"oid":"sha1c"}]`)
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	c := newTestClient(t, srv.URL, "tok", 0)
	entries, err := c.Tree(ctx, RepoTypeModel, "o/r", "main", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3 (directory dropped): %+v", len(entries), entries)
	}
	wantPaths := []string{"a.bin", "b.bin", "c.bin"}
	for i, p := range wantPaths {
		if entries[i].Path != p {
			t.Errorf("entries[%d].Path = %q, want %q", i, entries[i].Path, p)
		}
	}
	lfs := entries[1]
	if !lfs.IsLFS || lfs.SHA256 != "sha256b" || lfs.XetHash != "xetb" || lfs.GitOID != "sha1b" {
		t.Errorf("lfs entry = %+v", lfs)
	}
	if entries[0].IsLFS || entries[0].SHA256 != "" {
		t.Errorf("non-lfs entry = %+v", entries[0])
	}
}

func TestTreeSubPathAndRevisionEscaping(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/api/datasets/o/r/tree/refs%2Fpr%2F3/sub/dir" {
			t.Errorf("EscapedPath = %q", r.URL.EscapedPath())
		}
		writeBody(t, w, `[]`)
	}))
	defer srv.Close()
	c := newTestClient(t, srv.URL, "", 0)
	if _, err := c.Tree(t.Context(), RepoTypeDataset, "o/r", "refs/pr/3", "sub/dir"); err != nil {
		t.Fatal(err)
	}
}

func TestPathsInfoChunking(t *testing.T) {
	ctx := t.Context()
	var chunkSizes []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/api/models/o/r/paths-info/sha123") {
			t.Errorf("path = %q", r.URL.Path)
		}
		var req struct {
			Paths []string `json:"paths"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode body: %v", err)
		}
		chunkSizes = append(chunkSizes, len(req.Paths))
		var entries []map[string]any
		for _, p := range req.Paths {
			entries = append(entries, map[string]any{"type": "file", "path": p, "oid": "oid-" + p, "size": 1})
		}
		if err := json.NewEncoder(w).Encode(entries); err != nil {
			t.Errorf("encode paths-info entries: %v", err)
		}
	}))
	defer srv.Close()

	paths := make([]string, 2500)
	for i := range paths {
		paths[i] = fmt.Sprintf("f%04d", i)
	}
	c := newTestClient(t, srv.URL, "", 0)
	entries, err := c.PathsInfo(ctx, RepoTypeModel, "o/r", "sha123", paths)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(chunkSizes) != "[1000 1000 500]" {
		t.Fatalf("chunk sizes = %v", chunkSizes)
	}
	if len(entries) != 2500 {
		t.Fatalf("merged %d entries", len(entries))
	}
	for i := range paths {
		if entries[i].Path != paths[i] || entries[i].GitOID != "oid-"+paths[i] {
			t.Fatalf("entries[%d] = %+v, want path %q merged in order", i, entries[i], paths[i])
		}
	}
}

func TestRepoInfoParsing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/models/o/r":
			writeBody(t, w, `{"id":"o/r","sha":"deadbeef","private":true,"gated":"auto","siblings":[{"rfilename":"a"},{"rfilename":"b"}]}`)
		case "/api/models/o/public":
			writeBody(t, w, `{"id":"o/public","sha":"cafe","private":false,"gated":false}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c := newTestClient(t, srv.URL, "", 0)

	ri, err := c.RepoInfo(t.Context(), RepoTypeModel, "o/r")
	if err != nil {
		t.Fatal(err)
	}
	if ri.ID != "o/r" || ri.SHA != "deadbeef" || !ri.Private || !ri.Gated {
		t.Errorf("ri = %+v", ri)
	}
	if fmt.Sprint(ri.Siblings) != "[a b]" {
		t.Errorf("siblings = %v", ri.Siblings)
	}

	pub, err := c.RepoInfo(t.Context(), RepoTypeModel, "o/public")
	if err != nil {
		t.Fatal(err)
	}
	if pub.Gated {
		t.Errorf("gated=false parsed as true: %+v", pub)
	}
}

func TestRevision(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/api/spaces/o/r/revision/refs%2Fpr%2F3" {
			t.Errorf("EscapedPath = %q", r.URL.EscapedPath())
		}
		writeBody(t, w, `{"sha":"c0ffee"}`)
	}))
	defer srv.Close()
	c := newTestClient(t, srv.URL, "", 0)
	sha, err := c.Revision(t.Context(), RepoTypeSpace, "o/r", "refs/pr/3")
	if err != nil {
		t.Fatal(err)
	}
	if sha != "c0ffee" {
		t.Errorf("sha = %q", sha)
	}
}
