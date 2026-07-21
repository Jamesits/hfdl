package tui

import (
	"context"
	"errors"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/logging"
)

// pollInterval is the 4Hz snapshot/ring refresh rate.
const pollInterval = 250 * time.Millisecond

// Snapshot mirrors sched.Stats field-for-field (contract pkg/sched); cmd
// adapts the scheduler's Stats onto this struct so tui never imports sched
// and stays testable with fakes.
type Snapshot struct {
	Running, Paused, ENOSPCPaused  bool
	Repo, Revision, CommitSHA      string
	RepoStatus                     string
	BytesDone, BytesTotal          int64
	FilesDone, FilesTotal          int
	PendingCount                   int
	PendingNext                    []string
	GlobalRate                     float64
	ETA                            time.Duration // <0 unknown
	Queues                         [4]QueueStat  // 0=meta 1=download 2=disk 3=install
	Active                         []FileProgress
	Limits                         config.Limits
	BandwidthRate, APIRate         float64
	DutyLevel                      int
	DutyActiveRatio                float64
	DutyMedia                      string
	Cooldowns                      []CooldownInfo
	Stalls, Retries, SalvagedBytes int64
}

// QueueStat mirrors sched.QueueStat.
type QueueStat struct {
	Depth, InFlight int
	Detail          string
}

// FileProgress mirrors sched.FileProgress.
type FileProgress struct {
	FileID      int64
	Path        string
	Done, Total int64
	Conns       int
	Rate        float64
	Upstreams   []string
	Status      string
}

// CooldownInfo mirrors sched.CooldownInfo.
type CooldownInfo struct {
	Target, Kind string
	Remaining    time.Duration
}

// Callbacks are the UI's only side channels back into the scheduler; every
// field is nil-safe (absent callback = key is a no-op beyond local state).
type Callbacks struct {
	OnPauseToggle    func(paused bool)
	OnBandwidthDelta func(factor float64)
}

// bandwidth step factors for the +/- keys (contract: 0.9/1.1 steps).
const (
	bandwidthDownFactor = 0.9
	bandwidthUpFactor   = 1.1
)

// tickMsg is the 4Hz poll pulse.
type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(pollInterval, func(t time.Time) tea.Msg { return tickMsg(t) })
}

type tabID int

const (
	tabDashboard tabID = iota
	tabLogs
)

type model struct {
	snap    func() *Snapshot
	ring    *logging.Ring
	version string
	cb      Callbacks

	width, height int
	tab           tabID
	paused        bool
	current       *Snapshot
	logs          logView
	// warnBaseline is the ring's cumulative WARN+ total when the logs tab was
	// entered; the dashboard badge only counts records newer than that visit.
	// It tracks WarnTotal (monotonic), not Count (retained-only), so eviction
	// never makes the badge under-report.
	warnBaseline uint64
}

func newModel(snap func() *Snapshot, ring *logging.Ring, version string, cb Callbacks) model {
	m := model{
		snap:    snap,
		ring:    ring,
		version: version,
		cb:      cb,
		width:   defaultWidth,
		height:  defaultHeight,
		logs:    newLogView(),
	}
	m.poll() // seed so the first frame already has data
	return m
}

// poll refreshes the snapshot and drains new ring records. Nil-safe on both
// sources: a nil snapshot keeps the previous frame.
func (m *model) poll() {
	if m.snap != nil {
		if s := m.snap(); s != nil {
			m.current = s
		}
	}
	if m.ring != nil {
		m.logs.ingest(m.ring.Since(m.logs.lastSeq))
	}
}

// warnBadge is the number of unseen WARN+ records while on the dashboard. It
// uses the ring's monotonic cumulative WARN+ total (not the retained-only
// Count) so the badge stays accurate after the ring evicts older records.
func (m *model) warnBadge() int {
	if m.ring == nil {
		return 0
	}
	total := m.ring.WarnTotal()
	if total <= m.warnBaseline {
		return 0
	}
	return int(total - m.warnBaseline)
}

func (m model) Init() tea.Cmd { return tickCmd() }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = max(msg.Width, 1)
		m.height = max(msg.Height, 1)
		return m, nil
	case tickMsg:
		m.poll()
		return m, tickCmd()
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

// View wraps render's string in a tea.View: v2's Model.View returns a View
// (a styled string plus optional cursor/altscreen/color metadata) rather than
// a bare string. We only ever set content, so the frame renders inline just
// as it did under v1 (no altscreen).
func (m model) View() tea.View { return tea.NewView(m.render()) }

// render composes the active tab's frame as a styled string. It is View's
// string-producing core, exposed separately so tests assert on content
// without unwrapping a tea.View.
func (m model) render() string {
	if m.tab == tabLogs {
		return m.logsView()
	}
	return m.dashboardView()
}

// Run starts the TUI and blocks until the user quits or ctx is cancelled;
// both are clean exits (nil error). Bubbletea owns the screen for the whole
// run — callers must not write to stdout/stderr concurrently.
func Run(ctx context.Context, snap func() *Snapshot, ring *logging.Ring, version string, cb Callbacks) error {
	return run(ctx, snap, ring, version, cb)
}

// run is Run with injectable program options so tests can redirect
// input/output away from the real terminal fds.
//
// ctx cancellation is wired through AfterFunc+p.Quit rather than
// tea.WithContext on purpose: the latter's kill path (shutdown(kill=true))
// closes the input cancelreader without waiting for the read loop, which
// races under -race; a graceful Quit drains the loop first. Semantics are
// identical — cancellation ends the program and Run returns nil.
func run(ctx context.Context, snap func() *Snapshot, ring *logging.Ring, version string, cb Callbacks, opts ...tea.ProgramOption) error {
	m := newModel(snap, ring, version, cb)
	p := tea.NewProgram(m, opts...)
	stop := context.AfterFunc(ctx, p.Quit)
	defer stop()
	_, err := p.Run()
	if err != nil && (ctx.Err() != nil || errors.Is(err, tea.ErrProgramKilled)) {
		// ctx cancellation tears the program down mid-frame; that is a
		// requested shutdown, not a UI failure.
		return nil
	}
	return err
}
