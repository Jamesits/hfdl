package logging

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestJSONValue(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"slice with spaces", []string{"a b", "c"}, `["a b","c"]`},
		{"map", map[string]int{"a": 1, "b": 2}, `{"a":1,"b":2}`},
		{"string", "x", `"x"`},
		{"nil slice", []string(nil), `null`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := JSONValue(tc.in).String(); got != tc.want {
				t.Fatalf("JSONValue(%v) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestJSONValueUnmarshalable(t *testing.T) {
	v := JSONValue(func() {})
	if v.Kind() != slog.KindString || !strings.Contains(v.String(), "json") {
		t.Fatalf("expected fallback string mentioning json, got %v", v)
	}
}

func TestComponent(t *testing.T) {
	ring := NewRing(10)
	h := NewHandler(slog.LevelDebug, nil, ring)
	log := Component(slog.New(h), "api")
	log.Info("hello", "x", 1)

	recs := ring.All()
	if len(recs) != 1 {
		t.Fatalf("ring has %d records, want 1", len(recs))
	}
	if recs[0].Source != "api" {
		t.Fatalf("Source = %q, want api", recs[0].Source)
	}
	if recs[0].Attrs != `{"x":1}` {
		t.Fatalf("Attrs = %q, source must be lifted out of the JSON blob", recs[0].Attrs)
	}
}
