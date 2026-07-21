package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func TestLevelFloorAppliedAtRoot(t *testing.T) {
	ring := NewRing(10)
	var stderr bytes.Buffer
	h := NewHandler(slog.LevelWarn, &stderr, ring)
	log := slog.New(h)

	log.Debug("d1")
	log.Info("i1")
	log.Warn("w1")
	log.Error("e1")

	if got := len(ring.All()); got != 2 {
		t.Fatalf("ring has %d records, want 2 (floor at root)", got)
	}
	out := stderr.String()
	if strings.Contains(out, "d1") || strings.Contains(out, "i1") {
		t.Fatalf("stderr contains sub-floor records: %q", out)
	}
	if !strings.Contains(out, "w1") || !strings.Contains(out, "e1") {
		t.Fatalf("stderr missing warn/error records: %q", out)
	}
}

func TestStderrSilencedWithNilWriter(t *testing.T) {
	ring := NewRing(10)
	h := NewHandler(slog.LevelDebug, nil, ring)
	log := slog.New(h)
	log.Info("only-in-ring")

	recs := ring.All()
	if len(recs) != 1 || recs[0].Msg != "only-in-ring" {
		t.Fatalf("ring = %+v", recs)
	}
}

func TestWithAttrsAndGroupsShareSinks(t *testing.T) {
	tests := []struct {
		name      string
		log       func(l *slog.Logger)
		wantAttrs string
	}{
		{
			"attrs then group",
			func(l *slog.Logger) { l.With("a", 1).WithGroup("g").Info("m", "b", 2) },
			`{"a":1,"g":{"b":2}}`,
		},
		{
			"group then attrs",
			func(l *slog.Logger) { l.WithGroup("g").With("k", "v").Info("m") },
			`{"g":{"k":"v"}}`,
		},
		{
			"nested groups",
			func(l *slog.Logger) { l.WithGroup("g1").WithGroup("g2").Info("m", "x", true) },
			`{"g1":{"g2":{"x":true}}}`,
		},
		{
			"group value attr",
			func(l *slog.Logger) { l.Info("m", slog.Group("gv", "n", 5)) },
			`{"gv":{"n":5}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ring := NewRing(10)
			var stderr bytes.Buffer
			h := NewHandler(slog.LevelDebug, &stderr, ring)
			tc.log(slog.New(h))

			recs := ring.All()
			if len(recs) != 1 {
				t.Fatalf("ring has %d records, want 1", len(recs))
			}
			if recs[0].Attrs != tc.wantAttrs {
				t.Fatalf("Attrs = %q, want %q", recs[0].Attrs, tc.wantAttrs)
			}
			// The text sink must see the same record (fan-out shares sinks).
			if !strings.Contains(stderr.String(), "msg=m") {
				t.Fatalf("stderr missing record: %q", stderr.String())
			}
		})
	}
}

func TestChildHandlersWriteToSameRing(t *testing.T) {
	ring := NewRing(10)
	h := NewHandler(slog.LevelDebug, nil, ring)
	base := slog.New(h)
	child := base.With("child", true)

	base.Info("from-base")
	child.Info("from-child")

	if got := len(ring.All()); got != 2 {
		t.Fatalf("ring has %d records, want 2 — children must share the ring", got)
	}
}

func TestConcurrentHandle(t *testing.T) {
	ring := NewRing(64 * 200)
	var stderr bytes.Buffer // TextHandler serializes internally; buffer must not be touched concurrently elsewhere.
	h := NewHandler(slog.LevelInfo, &stderr, ring)

	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			log := slog.New(h).With("g", g)
			for i := 0; i < 200; i++ {
				log.Info("tick", "i", i)
			}
		}(g)
	}
	wg.Wait()

	all := ring.All()
	if len(all) != 64*200 {
		t.Fatalf("ring has %d records, want %d", len(all), 64*200)
	}
	for i, rec := range all {
		if rec.Seq != uint64(i+1) {
			t.Fatalf("all[%d].Seq = %d, want %d", i, rec.Seq, i+1)
		}
	}
	lines := bytes.Count(stderr.Bytes(), []byte("\n"))
	if lines != 64*200 {
		t.Fatalf("stderr has %d lines, want %d (every sink sees every record)", lines, 64*200)
	}
}

func TestAttrsJSONValueKinds(t *testing.T) {
	ring := NewRing(10)
	h := NewHandler(slog.LevelDebug, nil, ring)
	log := slog.New(h)
	log.Info("kinds",
		"s", "str",
		"i", 42,
		"f", 1.5,
		"b", true,
	)
	recs := ring.All()
	if len(recs) != 1 {
		t.Fatalf("ring has %d records", len(recs))
	}
	want := `{"s":"str","i":42,"f":1.5,"b":true}`
	if recs[0].Attrs != want {
		t.Fatalf("Attrs = %q, want %q", recs[0].Attrs, want)
	}
}
