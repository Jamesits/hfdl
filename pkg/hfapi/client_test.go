package hfapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace/noop"
)

// writeBody writes a test response body, surfacing write failures from
// handler goroutines (t.Errorf is safe off the test goroutine; t.Fatal is not).
func writeBody(t *testing.T, w http.ResponseWriter, s string) {
	t.Helper()
	if _, err := io.WriteString(w, s); err != nil {
		t.Errorf("write response body: %v", err)
	}
}

func newTestClient(t *testing.T, endpoint, token string, etagTimeout time.Duration) *Client {
	t.Helper()
	return NewClient(slog.New(slog.DiscardHandler), &http.Client{}, endpoint, token, etagTimeout)
}

// getJSON runs Tree against srv and returns the error (Tree is the simplest
// GET entry point for status-mapping tests).
func getErr(t *testing.T, c *Client) error {
	t.Helper()
	_, err := c.Tree(t.Context(), RepoTypeModel, "o/r", "main", "")
	return err
}

func TestRedirectAuthStripping(t *testing.T) {
	ctx := t.Context()

	// cross-host redirect: hub A -> "CDN" B; B must NOT see the token.
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("cross-host redirect carried Authorization %q", got)
		}
		writeBody(t, w, `[]`)
	}))
	defer b.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/models/o/cross/tree/main", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, b.URL+"/cdn/file", http.StatusFound)
	})
	mux.HandleFunc("/api/models/o/same/tree/main", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/final-page", http.StatusFound)
	})
	mux.HandleFunc("/final-page", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("same-host redirect lost Authorization, got %q", got)
		}
		if got := r.Header.Get("User-Agent"); got == "" {
			t.Error("User-Agent not set")
		}
		writeBody(t, w, `[]`)
	})
	a := httptest.NewServer(mux)
	defer a.Close()

	c := newTestClient(t, a.URL, "tok", 0)

	if _, err := c.Tree(ctx, RepoTypeModel, "o/cross", "main", ""); err != nil {
		t.Fatalf("cross-host tree: %v", err)
	}
	if _, err := c.Tree(ctx, RepoTypeModel, "o/same", "main", ""); err != nil {
		t.Fatalf("same-host tree: %v", err)
	}
}

func TestNoTokenNoAuthorization(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("anonymous client sent Authorization %q", got)
		}
		writeBody(t, w, `[]`)
	}))
	defer srv.Close()
	c := newTestClient(t, srv.URL, "", 0)
	if _, err := c.Tree(t.Context(), RepoTypeModel, "o/r", "main", ""); err != nil {
		t.Fatal(err)
	}
}

func TestTypedStatusErrors(t *testing.T) {
	ctx := t.Context()
	cases := []struct {
		name   string
		status int
		body   string
		header http.Header
		check  func(t *testing.T, err error)
	}{
		{
			name:   "404",
			status: http.StatusNotFound,
			check: func(t *testing.T, err error) {
				var nf *NotFoundError
				if !errors.As(err, &nf) {
					t.Fatalf("want NotFoundError, got %T: %v", err, err)
				}
				if nf.Repo != "o/r" || nf.Revision != "main" {
					t.Errorf("got %+v", nf)
				}
			},
		},
		{
			name:   "403",
			status: http.StatusForbidden,
			check: func(t *testing.T, err error) {
				var g *GatedError
				if !errors.As(err, &g) {
					t.Fatalf("want GatedError, got %T: %v", err, err)
				}
				if g.Repo != "o/r" {
					t.Errorf("got %+v", g)
				}
			},
		},
		{
			name:   "401 json msg",
			status: http.StatusUnauthorized,
			body:   `{"error":"Invalid username or password"}`,
			check: func(t *testing.T, err error) {
				var a *AuthError
				if !errors.As(err, &a) {
					t.Fatalf("want AuthError, got %T: %v", err, err)
				}
				if a.Msg != "Invalid username or password" {
					t.Errorf("msg = %q", a.Msg)
				}
			},
		},
		{
			name:   "429 retry-after seconds",
			status: http.StatusTooManyRequests,
			header: http.Header{"Retry-After": []string{"7"}},
			check: func(t *testing.T, err error) {
				var rl *RateLimitError
				if !errors.As(err, &rl) {
					t.Fatalf("want RateLimitError, got %T: %v", err, err)
				}
				if rl.RetryAfter != 7*time.Second {
					t.Errorf("RetryAfter = %v", rl.RetryAfter)
				}
			},
		},
		{
			name:   "429 retry-after http-date",
			status: http.StatusTooManyRequests,
			header: http.Header{"Retry-After": []string{time.Now().Add(120 * time.Second).UTC().Format(http.TimeFormat)}},
			check: func(t *testing.T, err error) {
				var rl *RateLimitError
				if !errors.As(err, &rl) {
					t.Fatalf("want RateLimitError, got %T: %v", err, err)
				}
				if rl.RetryAfter <= 100*time.Second || rl.RetryAfter > 121*time.Second {
					t.Errorf("RetryAfter = %v, want ~120s", rl.RetryAfter)
				}
			},
		},
		{
			name:   "429 no header",
			status: http.StatusTooManyRequests,
			check: func(t *testing.T, err error) {
				var rl *RateLimitError
				if !errors.As(err, &rl) || rl.RetryAfter != 0 {
					t.Errorf("got %v", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, vs := range tc.header {
					for _, v := range vs {
						w.Header().Add(k, v)
					}
				}
				w.WriteHeader(tc.status)
				writeBody(t, w, tc.body)
			}))
			defer srv.Close()
			c := newTestClient(t, srv.URL, "", 0)
			_ = ctx
			tc.check(t, getErr(t, c))
		})
	}
}

func TestResponseTimeoutEnforced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(500 * time.Millisecond):
		}
		writeBody(t, w, `[]`)
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL, "", 30*time.Millisecond)
	err := getErr(t, c)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want wrapped context.DeadlineExceeded, got %v", err)
	}
}

func TestSetTracerNilSafe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeBody(t, w, `{"id":"o/r","sha":"abc"}`)
	}))
	defer srv.Close()
	c := newTestClient(t, srv.URL, "", 0)

	// nil tracer (default): no spans, still works.
	if _, err := c.RepoInfo(t.Context(), RepoTypeModel, "o/r"); err != nil {
		t.Fatal(err)
	}
	// installed tracer wraps the call without changing behavior.
	c.SetTracer(noop.NewTracerProvider().Tracer("test"))
	ri, err := c.RepoInfo(t.Context(), RepoTypeModel, "o/r")
	if err != nil {
		t.Fatal(err)
	}
	if ri.SHA != "abc" {
		t.Errorf("sha = %q", ri.SHA)
	}
	// reset to nil is safe.
	c.SetTracer(nil)
	if _, err := c.RepoInfo(t.Context(), RepoTypeModel, "o/r"); err != nil {
		t.Fatal(err)
	}
}
