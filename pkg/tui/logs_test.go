package tui

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/jamesits/hfdl/pkg/logging"
)

func writeRecs(ring *logging.Ring, level slog.Level, msgs ...string) {
	for _, msg := range msgs {
		ring.Write(logging.Record{Time: time.Now(), Level: level, Source: "test", Msg: msg})
	}
}

func TestRingRecordsAppearAfterTick(t *testing.T) {
	ring := logging.NewRing(100)
	m := newModel(func() *Snapshot { return testSnapshot() }, ring, "x", Callbacks{})
	m = press(m, "l")
	if strings.Contains(stripANSI(m.render()), "hello-ring") {
		t.Fatal("record visible before tick")
	}
	writeRecs(ring, slog.LevelInfo, "hello-ring")
	m = tick(m)
	if !strings.Contains(stripANSI(m.render()), "hello-ring") {
		t.Fatalf("ring record missing after tick:\n%s", stripANSI(m.render()))
	}
}

func TestLevelFilterCycle(t *testing.T) {
	ring := logging.NewRing(100)
	writeRecs(ring, slog.LevelDebug, "dbg-msg")
	writeRecs(ring, slog.LevelInfo, "info-msg")
	writeRecs(ring, slog.LevelWarn, "warn-msg")
	writeRecs(ring, slog.LevelError, "err-msg")
	m := newModel(func() *Snapshot { return testSnapshot() }, ring, "x", Callbacks{})
	m = press(m, "l")

	// initial filter = all
	v := stripANSI(m.render())
	for _, want := range []string{"dbg-msg", "info-msg", "warn-msg", "err-msg"} {
		if !strings.Contains(v, want) {
			t.Fatalf("filter=all missing %q", want)
		}
	}

	m = press(m, "e") // errors-only
	v = stripANSI(m.render())
	if !strings.Contains(v, "err-msg") || strings.Contains(v, "warn-msg") ||
		strings.Contains(v, "info-msg") || strings.Contains(v, "dbg-msg") {
		t.Fatalf("filter=errors wrong set:\n%s", v)
	}
	if !strings.Contains(v, "level: errors") {
		t.Errorf("filter label missing")
	}

	m = press(m, "e") // warn+
	v = stripANSI(m.render())
	if !strings.Contains(v, "err-msg") || !strings.Contains(v, "warn-msg") ||
		strings.Contains(v, "info-msg") || strings.Contains(v, "dbg-msg") {
		t.Fatalf("filter=warn+ wrong set:\n%s", v)
	}

	m = press(m, "e") // back to all
	v = stripANSI(m.render())
	if !strings.Contains(v, "dbg-msg") {
		t.Fatalf("filter did not cycle back to all:\n%s", v)
	}
}

func TestWarnBadgeClearsOnSwitch(t *testing.T) {
	ring := logging.NewRing(100)
	writeRecs(ring, slog.LevelWarn, "w1", "w2")
	m := newModel(func() *Snapshot { return testSnapshot() }, ring, "x", Callbacks{})

	v := stripANSI(m.render())
	if !strings.Contains(v, "⚠2") {
		t.Fatalf("badge should count ring WARN+ while on dashboard:\n%s", v)
	}

	m = press(m, "l") // enter logs: badge baseline snapshots
	m = press(m, "l") // back to dashboard
	if strings.Contains(stripANSI(m.render()), "⚠") {
		t.Fatal("badge must clear after visiting the logs tab")
	}

	writeRecs(ring, slog.LevelWarn, "w3")
	m = tick(m)
	if !strings.Contains(stripANSI(m.render()), "⚠1") {
		t.Fatal("badge counts only records after the logs visit")
	}
}

func TestFollowScrollTransitions(t *testing.T) {
	ring := logging.NewRing(100)
	for i := range 30 {
		writeRecs(ring, slog.LevelInfo, fmt.Sprintf("rec-%02d", i))
	}
	m := newModel(func() *Snapshot { return testSnapshot() }, ring, "x", Callbacks{})
	tm, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 12}) // page = 10
	m = tm.(model)
	m = press(m, "l")

	// follow mode: last page visible
	v := stripANSI(m.render())
	if !strings.Contains(v, "rec-29") || strings.Contains(v, "rec-00") {
		t.Fatalf("follow should pin to bottom:\n%s", v)
	}
	if !strings.Contains(v, "[follow]") {
		t.Errorf("follow tag missing")
	}

	// scroll up: follow drops, window anchored near the bottom
	m = press(m, "up")
	if m.logs.follow {
		t.Fatal("scroll key must drop follow mode")
	}
	v = stripANSI(m.render())
	if !strings.Contains(v, "rec-28") || strings.Contains(v, "rec-29") {
		t.Fatalf("one scroll up from bottom shows rec-19..28:\n%s", v)
	}

	// page up twice → top
	m = press(m, "pgup", "pgup")
	v = stripANSI(m.render())
	if !strings.Contains(v, "rec-00") {
		t.Fatalf("pgup x2 should reach the top:\n%s", v)
	}

	// G → follow again, bottom visible
	m = press(m, "G")
	if !m.logs.follow {
		t.Fatal("G re-engages follow")
	}
	v = stripANSI(m.render())
	if !strings.Contains(v, "rec-29") {
		t.Fatalf("G should jump to bottom:\n%s", v)
	}

	// new record while following stays pinned to bottom
	writeRecs(ring, slog.LevelInfo, "rec-30")
	m = tick(m)
	if !strings.Contains(stripANSI(m.render()), "rec-30") {
		t.Fatal("follow mode must track new records")
	}
}

func TestConcurrentRingWritesRaceClean(t *testing.T) {
	ring := logging.NewRing(2048)
	m := newModel(func() *Snapshot { return testSnapshot() }, ring, "x", Callbacks{})

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				ring.Write(logging.Record{Time: time.Now(), Level: slog.LevelWarn,
					Source: "load", Msg: fmt.Sprintf("g%d-%d", g, i)})
			}
		}(g)
	}
	// Hammer the read paths (poll + render + badge) while writers run.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 400 {
			tm, _ := m.Update(tickMsg(time.Now()))
			m = tm.(model)
			_ = m.render()
			_ = press(m, "l").render()
			_ = m.warnBadge()
		}
	}()
	wg.Wait()
	<-done
	if got := ring.Count(slog.LevelWarn); got != 1600 {
		t.Fatalf("ring lost records: %d", got)
	}
}
