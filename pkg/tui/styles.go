package tui

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/jamesits/hfdl/pkg/config"
)

const (
	defaultWidth  = 80
	defaultHeight = 24

	// progressBarWidth caps the ▓░ bar; it shrinks on narrow terminals.
	progressBarWidth = 10
	// shaShortLen mirrors git's default abbreviation.
	shaShortLen = 7
	// maxPendingNames caps the "next:" preview on the pending line.
	maxPendingNames = 3
	// minPathWidth is the smallest file-path column before the right-hand
	// detail starts shedding.
	minPathWidth = 12
)

var (
	styleHeader = lipgloss.NewStyle().Bold(true)
	styleDim    = lipgloss.NewStyle().Faint(true)
	styleWarn   = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	styleErr    = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
)

// clamp forces a rendered line within the terminal width; lipgloss MaxWidth
// is ANSI-aware, so styled segments survive truncation.
func (m model) clamp(s string) string {
	return lipgloss.NewStyle().MaxWidth(m.width).Render(s)
}

func levelStyle(l slog.Level) lipgloss.Style {
	switch {
	case l >= slog.LevelError:
		return styleErr
	case l >= slog.LevelWarn:
		return styleWarn
	default:
		return styleDim
	}
}

// fitLines keeps the first head and last tail lines when the frame is taller
// than the terminal, trimming the middle (file rows, log scrollback) first.
func fitLines(lines []string, height, head, tail int) []string {
	if height <= 0 || len(lines) <= height {
		return lines
	}
	if height <= head+tail {
		return lines[:height]
	}
	out := make([]string, 0, height)
	out = append(out, lines[:head]...)
	out = append(out, lines[head:head+height-head-tail]...)
	out = append(out, lines[len(lines)-tail:]...)
	return out
}

// truncTail shortens s to w cells keeping the tail (repo paths disambiguate
// at the end: "…/layers/model.safetensors").
func truncTail(s string, w int) string {
	if w <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= w {
		return s
	}
	if w == 1 {
		return "…"
	}
	rs := []rune(s)
	budget := w - 1 // leading ellipsis
	i := len(rs)
	for i > 0 {
		rw := lipgloss.Width(string(rs[i-1]))
		if rw > budget {
			break
		}
		budget -= rw
		i--
	}
	return "…" + string(rs[i:])
}

// renderBar draws a ▓░ progress bar width cells wide; any progress shows at
// least one filled cell so tiny files don't look stalled at 0.
func renderBar(done, total int64, width int) string {
	if width < 1 {
		return ""
	}
	filled := 0
	if total > 0 {
		filled = int(done * int64(width) / total)
		filled = max(0, min(filled, width))
	}
	if done > 0 && filled == 0 {
		filled = 1
	}
	return strings.Repeat("▓", filled) + strings.Repeat("░", width-filled)
}

// humanRate renders compactly ("412MiB/s") — the footer packs six segments
// into 80 columns, so the humanize space goes.
func humanRate(bps float64) string {
	if bps <= 0 {
		return "0B/s"
	}
	return strings.Replace(config.FormatSize(int64(bps)), " ", "", 1) + "/s"
}

// formatETA renders "1m07s"-style durations; negative = unknown (—).
func formatETA(d time.Duration) string {
	if d < 0 {
		return "—"
	}
	d = d.Round(time.Second)
	if h := int(d.Hours()); h > 0 {
		return fmt.Sprintf("%dh%02dm", h, int(d.Minutes())%60)
	}
	if m := int(d.Minutes()); m > 0 {
		return fmt.Sprintf("%dm%02ds", m, int(d.Seconds())%60)
	}
	return fmt.Sprintf("%ds", int(d.Seconds()))
}

func shortSHA(sha string) string {
	if len(sha) > shaShortLen {
		return sha[:shaShortLen]
	}
	return sha
}
