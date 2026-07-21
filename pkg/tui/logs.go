package tui

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/jamesits/hfdl/pkg/logging"
)

// logRetainCap bounds ingested scrollback; the source ring is already
// bounded (2k default), this only guards against pathological test rings.
const logRetainCap = 8192

// levelFilter cycles errors-only → warn+ → all on the logs tab 'e' key.
type levelFilter int

const (
	filterErrors levelFilter = iota
	filterWarn
	filterAll
)

func (f levelFilter) next() levelFilter { return (f + 1) % 3 }

func (f levelFilter) String() string {
	switch f {
	case filterErrors:
		return "errors"
	case filterWarn:
		return "warn+"
	}
	return "all"
}

// floor is the minimum visible level; LevelAll equivalent sits below Debug.
func (f levelFilter) floor() slog.Level {
	switch f {
	case filterErrors:
		return slog.LevelError
	case filterWarn:
		return slog.LevelWarn
	}
	return slog.Level(-128)
}

// logView is the logs-tab state: ingested ring records, filter, and the
// scroll window. Filtering applies at render time so cycling 'e' reveals
// already-ingested records without re-polling the ring.
type logView struct {
	recs    []logging.Record
	lastSeq uint64
	filter  levelFilter
	follow  bool
	offset  int // top visible index into filtered(), used when !follow
}

func newLogView() logView {
	return logView{filter: filterAll, follow: true}
}

func (l *logView) ingest(recs []logging.Record) {
	if len(recs) == 0 {
		return
	}
	l.recs = append(l.recs, recs...)
	l.lastSeq = recs[len(recs)-1].Seq
	if len(l.recs) > logRetainCap {
		l.recs = append([]logging.Record(nil), l.recs[len(l.recs)-logRetainCap:]...)
	}
}

func (l *logView) filtered() []logging.Record {
	floor := l.filter.floor()
	out := make([]logging.Record, 0, len(l.recs))
	for _, r := range l.recs {
		if r.Level >= floor {
			out = append(out, r)
		}
	}
	return out
}

func (l *logView) maxOffset(page int) int {
	if page < 1 {
		page = 1
	}
	return max(0, len(l.filtered())-page)
}

// scrollBy moves the window and drops follow mode; 'G' re-engages it.
// When leaving follow, the window first anchors to the visible bottom so a
// single "up" scrolls from what the user actually sees.
func (l *logView) scrollBy(delta, page int) {
	if l.follow {
		l.offset = l.maxOffset(page)
	}
	l.follow = false
	l.offset = min(max(l.offset+delta, 0), l.maxOffset(page))
}

// logsPageSize is the number of log lines that fit between the tab header
// and the help footer.
func (m model) logsPageSize() int {
	return max(0, m.height-2)
}

func (m model) logsView() string {
	page := m.logsPageSize()
	recs := m.logs.filtered()
	off := m.logs.offset
	if m.logs.follow {
		off = max(0, len(recs)-page)
	}
	off = min(off, max(0, len(recs)-1))
	if len(recs) == 0 {
		off = 0
	}
	end := min(off+page, len(recs))

	followTag := "follow"
	if !m.logs.follow {
		followTag = fmt.Sprintf("line %d/%d", off+1, len(recs))
	}
	head := styleHeader.Render(fmt.Sprintf("Logs  [level: %s]  [%s]  %d records",
		m.logs.filter, followTag, len(recs)))

	lines := make([]string, 0, end-off+2)
	lines = append(lines, head)
	for _, r := range recs[off:end] {
		lines = append(lines, formatRecord(r))
	}
	lines = append(lines, styleDim.Render("q quit  l/tab dashboard  e filter  ↑↓/pgup/pgdn scroll  G follow"))
	lines = fitLines(lines, m.height, 1, 1)
	for i := range lines {
		lines[i] = m.clamp(lines[i])
	}
	return strings.Join(lines, "\n")
}

// formatRecord renders one ring record: time, level, source, message, attrs.
func formatRecord(r logging.Record) string {
	lvl := strings.ToUpper(r.Level.String())
	var b strings.Builder
	b.WriteString(r.Time.Format("15:04:05"))
	b.WriteByte(' ')
	b.WriteString(levelStyle(r.Level).Render(fmt.Sprintf("%-5s", lvl)))
	if r.Source != "" {
		b.WriteByte(' ')
		b.WriteString(r.Source)
	}
	b.WriteString(": ")
	b.WriteString(r.Msg)
	if r.Attrs != "" {
		b.WriteByte(' ')
		b.WriteString(styleDim.Render(r.Attrs))
	}
	return b.String()
}
