package tui

import (
	"regexp"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/jamesits/hfdl/pkg/config"
)

var ansiRE = regexp.MustCompile("\x1b\\[[0-9;]*m")

func stripANSI(s string) string { return ansiRE.ReplaceAllString(s, "") }

func keyMsg(s string) tea.KeyMsg {
	switch s {
	case "ctrl+c":
		return tea.KeyMsg{Type: tea.KeyCtrlC}
	case "tab":
		return tea.KeyMsg{Type: tea.KeyTab}
	case "up":
		return tea.KeyMsg{Type: tea.KeyUp}
	case "down":
		return tea.KeyMsg{Type: tea.KeyDown}
	case "pgup":
		return tea.KeyMsg{Type: tea.KeyPgUp}
	case "pgdown":
		return tea.KeyMsg{Type: tea.KeyPgDown}
	default:
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
	}
}

// press drives model.Update with the given keys, threading the model value.
func press(m model, keys ...string) model {
	for _, k := range keys {
		tm, _ := m.Update(keyMsg(k))
		m = tm.(model)
	}
	return m
}

func tick(m model) model {
	tm, _ := m.Update(tickMsg(time.Now()))
	return tm.(model)
}

func testSnapshot() *Snapshot {
	return &Snapshot{
		Running:    true,
		Repo:       "org/repo",
		Revision:   "main",
		CommitSHA:  "a1b2c3d4e5f6",
		BytesDone:  12_400_000_000,
		BytesTotal: 38_100_000_000,
		FilesDone:  3, FilesTotal: 140,
		PendingCount: 137,
		PendingNext:  []string{"config.json", "generation_config.json"},
		GlobalRate:   412 * 1024 * 1024,
		ETA:          67 * time.Second,
		Queues: [4]QueueStat{
			{Depth: 2, InFlight: 1},
			{Depth: 1204, InFlight: 48, Detail: "blk"},
			{Depth: 5, Detail: "v3 s2"},
			{Depth: 1},
		},
		Active: []FileProgress{
			{FileID: 1, Path: "model.safetensors", Done: 8_200_000_000, Total: 9_900_000_000,
				Conns: 8, Rate: 301 * 1024 * 1024, Upstreams: []string{"hf.co", "mirror"}, Status: "downloading"},
		},
		Limits:          config.DefaultLimits(),
		BandwidthRate:   412 * 1024 * 1024,
		APIRate:         3.2,
		DutyLevel:       80,
		DutyActiveRatio: 0.61,
		DutyMedia:       "hdd",
		Stalls:          2,
		Retries:         5,
		SalvagedBytes:   1_200_000_000,
	}
}

func TestInitialRenderContainsRepoBytesSpeed(t *testing.T) {
	s := testSnapshot()
	m := newModel(func() *Snapshot { return s }, nil, "0.1.0", Callbacks{})
	v := stripANSI(m.View())
	for _, want := range []string{
		"hfdl 0.1.0",
		"org/repo@main",
		"(a1b2c3d)",
		config.FormatSize(s.BytesDone) + "/" + config.FormatSize(s.BytesTotal),
		humanRate(s.GlobalRate),
		"ETA 1m07s",
		"meta:2 (1 run)",
		"download:1204 blk (48 run)",
		"model.safetensors",
		"pending: 137 files",
		"config.json",
		"stalls 2",
		"retries 5",
		"salvaged " + strings.Replace(config.FormatSize(s.SalvagedBytes), " ", "", 1),
		"(hdd)",
	} {
		if !strings.Contains(v, want) {
			t.Errorf("dashboard missing %q\n---\n%s", want, v)
		}
	}
}

func TestTickUpdatesSnapshot(t *testing.T) {
	s := testSnapshot()
	m := newModel(func() *Snapshot { return s }, nil, "x", Callbacks{})
	s.BytesDone = 30_000_000_000
	s.GlobalRate = 1
	m = tick(m)
	v := stripANSI(m.View())
	if !strings.Contains(v, config.FormatSize(30_000_000_000)) {
		t.Errorf("tick did not refresh bytes:\n%s", v)
	}
}

func TestNilSnapshotKeepsFrame(t *testing.T) {
	s := testSnapshot()
	calls := 0
	m := newModel(func() *Snapshot {
		calls++
		if calls > 1 {
			return nil
		}
		return s
	}, nil, "x", Callbacks{})
	m = tick(m)
	if m.current == nil || m.current.Repo != "org/repo" {
		t.Fatalf("nil snapshot clobbered the frame: %+v", m.current)
	}
}

func TestPauseToggleCallback(t *testing.T) {
	var got []bool
	m := newModel(func() *Snapshot { return testSnapshot() }, nil, "x",
		Callbacks{OnPauseToggle: func(paused bool) { got = append(got, paused) }})
	m = press(m, "p")
	if len(got) != 1 || got[0] != true {
		t.Fatalf("one press = one call with true, got %v", got)
	}
	if !strings.Contains(stripANSI(m.View()), "paused") {
		t.Error("paused state not reflected in header")
	}
	m = press(m, "p")
	if len(got) != 2 || got[1] != false {
		t.Fatalf("second press toggles back, got %v", got)
	}
	if strings.Contains(stripANSI(m.View()), "[paused]") {
		t.Error("paused flag should clear after second toggle")
	}
}

func TestBandwidthDeltaCallbacks(t *testing.T) {
	var got []float64
	m := newModel(func() *Snapshot { return testSnapshot() }, nil, "x",
		Callbacks{OnBandwidthDelta: func(f float64) { got = append(got, f) }})
	_ = press(m, "+", "-")
	if len(got) != 2 || got[0] != 1.1 || got[1] != 0.9 {
		t.Fatalf("+/- factors = [1.1 0.9], got %v", got)
	}
}

func TestNilCallbacksSafe(t *testing.T) {
	m := newModel(func() *Snapshot { return testSnapshot() }, nil, "x", Callbacks{})
	_ = press(m, "p", "+", "-", "e") // must not panic
}

func TestTabSwitchToLogs(t *testing.T) {
	m := newModel(func() *Snapshot { return testSnapshot() }, nil, "x", Callbacks{})
	if m.tab != tabDashboard {
		t.Fatal("starts on dashboard")
	}
	m = press(m, "l")
	if m.tab != tabLogs {
		t.Fatal("l switches to logs")
	}
	if !strings.Contains(stripANSI(m.View()), "Logs") {
		t.Error("logs view not rendered")
	}
	m = press(m, "tab")
	if m.tab != tabDashboard {
		t.Fatal("tab switches back to dashboard")
	}
}

func TestQuitKeys(t *testing.T) {
	for _, k := range []string{"q", "ctrl+c"} {
		m := newModel(func() *Snapshot { return testSnapshot() }, nil, "x", Callbacks{})
		_, cmd := m.Update(keyMsg(k))
		if cmd == nil {
			t.Fatalf("%s: no command returned", k)
		}
		if _, ok := cmd().(tea.QuitMsg); !ok {
			t.Fatalf("%s: expected tea.QuitMsg", k)
		}
	}
}

func TestResizeKeepsRenderInBounds(t *testing.T) {
	s := testSnapshot()
	for i := 0; i < 30; i++ {
		s.Active = append(s.Active, FileProgress{
			FileID: int64(100 + i), Path: strings.Repeat("deep/", 10) + "file.bin",
			Done: 1, Total: 100, Conns: 2, Rate: 1e6,
		})
	}
	for _, size := range [][2]int{{20, 5}, {200, 60}, {1, 1}, {40, 3}} {
		w, h := size[0], size[1]
		m := newModel(func() *Snapshot { return s }, nil, "0.1.0", Callbacks{})
		tm, _ := m.Update(tea.WindowSizeMsg{Width: w, Height: h})
		m = tm.(model)
		for _, tabKey := range []string{"", "l"} {
			if tabKey != "" {
				m = press(m, tabKey)
			}
			v := m.View()
			lines := strings.Split(v, "\n")
			if len(lines) > h {
				t.Errorf("%dx%d tab=%q: %d lines > height", w, h, tabKey, len(lines))
			}
			for _, ln := range lines {
				if lw := lipgloss.Width(ln); lw > w {
					t.Errorf("%dx%d tab=%q: line width %d > %d: %q", w, h, tabKey, lw, w, ln)
				}
			}
		}
	}
}

func TestCooldownAndENOSPCRendering(t *testing.T) {
	s := testSnapshot()
	s.ENOSPCPaused = true
	s.Cooldowns = []CooldownInfo{
		{Target: "mirror", Kind: "conn", Remaining: 3 * time.Second},
		{Target: "hf.co", Kind: "http_429", Remaining: 12 * time.Second},
	}
	m := newModel(func() *Snapshot { return s }, nil, "x", Callbacks{})
	v := stripANSI(m.View())
	if !strings.Contains(v, "429: hf.co 12s") {
		t.Errorf("429 cooldown countdown missing:\n%s", v)
	}
	if !strings.Contains(v, "[ENOSPC]") {
		t.Errorf("ENOSPC flag missing:\n%s", v)
	}
	// 429 hidden when nothing is cooling down
	s.Cooldowns = nil
	m = tick(m)
	if strings.Contains(stripANSI(m.View()), "429:") {
		t.Error("empty cooldown should omit the 429 segment")
	}
}

func TestTruncTailKeepsTail(t *testing.T) {
	got := truncTail("very/deeply/nested/path/model.safetensors", 20)
	if lipgloss.Width(got) > 20 {
		t.Fatalf("width %d > 20", lipgloss.Width(got))
	}
	if !strings.HasSuffix(got, "safetensors") || !strings.HasPrefix(got, "…") {
		t.Fatalf("tail lost: %q", got)
	}
}

func TestRenderBarBounds(t *testing.T) {
	if got := renderBar(5, 10, 10); got != "▓▓▓▓▓░░░░░" {
		t.Fatalf("50%% bar = %q", got)
	}
	if got := renderBar(1, 1<<40, 10); !strings.HasPrefix(got, "▓") {
		t.Fatalf("tiny progress must show one cell: %q", got)
	}
	if got := renderBar(0, 0, 10); got != "░░░░░░░░░░" {
		t.Fatalf("zero total bar = %q", got)
	}
	if got := renderBar(11, 10, 10); got != "▓▓▓▓▓▓▓▓▓▓" {
		t.Fatalf("overrun clamps: %q", got)
	}
}

func TestFormatETA(t *testing.T) {
	if got := formatETA(-1); got != "—" {
		t.Fatalf("unknown ETA = %q", got)
	}
	if got := formatETA(67 * time.Second); got != "1m07s" {
		t.Fatalf("67s = %q", got)
	}
	if got := formatETA(5 * time.Second); got != "5s" {
		t.Fatalf("5s = %q", got)
	}
	if got := formatETA(2*time.Hour + 3*time.Minute); got != "2h03m" {
		t.Fatalf("2h3m = %q", got)
	}
}
