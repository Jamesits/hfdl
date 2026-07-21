package main

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/logging"
	"github.com/jamesits/hfdl/pkg/store"
)

// seedLogs writes rows into a fresh state DB and closes it (the exclusive
// lock must be released before `hfdl logs` reopens the same file).
func seedLogs(t *testing.T, dbPath string) {
	t.Helper()
	ctx := t.Context()
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rows := []logging.LogRow{
		{Time: time.Now().Add(-2 * time.Hour), Level: int(slog.LevelDebug), Source: "old", Msg: "ancient debug"},
		{Time: time.Now().Add(-5 * time.Minute), Level: int(slog.LevelInfo), Source: "sched", Msg: "job started"},
		{Time: time.Now(), Level: int(slog.LevelWarn), Source: "transfer", Msg: "stall detected", Attrs: `{"upstream":"u1"}`},
	}
	if err := st.InsertLogs(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunLogs(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "state.db")
	seedLogs(t, dbPath)

	t.Run("all rows", func(t *testing.T) {
		var out bytes.Buffer
		f := &logsFlags{levelStr: "debug", stateDBFlag: dbPath}
		if err := runLogs(t.Context(), f, getenvMap(nil), &out); err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(out.String()), "\n")
		if len(lines) != 3 {
			t.Fatalf("lines = %d: %q", len(lines), out.String())
		}
		if !strings.Contains(lines[1], "INFO sched job started") {
			t.Fatalf("line = %q", lines[1])
		}
		if !strings.Contains(lines[2], `WARN transfer stall detected {"upstream":"u1"}`) {
			t.Fatalf("attrs not rendered: %q", lines[2])
		}
	})

	t.Run("level filter", func(t *testing.T) {
		var out bytes.Buffer
		f := &logsFlags{levelStr: "warn", stateDBFlag: dbPath}
		if err := runLogs(t.Context(), f, getenvMap(nil), &out); err != nil {
			t.Fatal(err)
		}
		if got := strings.TrimSpace(out.String()); !strings.Contains(got, "WARN") || strings.Contains(got, "INFO") {
			t.Fatalf("out = %q", got)
		}
	})

	t.Run("since filter", func(t *testing.T) {
		var out bytes.Buffer
		f := &logsFlags{levelStr: "debug", sinceStr: "10m", stateDBFlag: dbPath}
		if err := runLogs(t.Context(), f, getenvMap(nil), &out); err != nil {
			t.Fatal(err)
		}
		if got := out.String(); strings.Contains(got, "ancient") {
			t.Fatalf("since not applied: %q", got)
		}
	})

	t.Run("bad level", func(t *testing.T) {
		f := &logsFlags{levelStr: "trace", stateDBFlag: dbPath}
		if err := runLogs(t.Context(), f, getenvMap(nil), &bytes.Buffer{}); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("bad since", func(t *testing.T) {
		f := &logsFlags{sinceStr: "yesterday", stateDBFlag: dbPath}
		if err := runLogs(t.Context(), f, getenvMap(nil), &bytes.Buffer{}); err == nil {
			t.Fatal("expected error")
		}
	})
}
