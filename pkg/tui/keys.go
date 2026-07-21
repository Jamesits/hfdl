package tui

import (
	tea "github.com/charmbracelet/bubbletea"
)

// Key map: q/ctrl+c quit everywhere; l/tab toggle dashboard↔logs;
// dashboard: p pause toggle, +/- bandwidth ∓10%; logs: e level-filter cycle,
// arrows/pgup/pgdn scroll (drops follow), G back to follow mode.

func (m model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c", "q":
		return m, tea.Quit
	case "tab", "l":
		m.switchTab()
		return m, nil
	}
	if m.tab == tabLogs {
		return m.handleLogsKey(msg)
	}
	return m.handleDashboardKey(msg)
}

// switchTab toggles tabs. Entering the logs tab snapshots the cumulative
// WARN+ total so the dashboard badge afterwards only reports records the user
// has not seen — using WarnTotal (monotonic) keeps the baseline valid even
// after the ring evicts records the user did see.
func (m *model) switchTab() {
	if m.tab == tabDashboard {
		if m.ring != nil {
			m.warnBaseline = m.ring.WarnTotal()
		}
		m.tab = tabLogs
		return
	}
	m.tab = tabDashboard
}

func (m model) handleDashboardKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "p":
		m.paused = !m.paused
		if m.cb.OnPauseToggle != nil {
			m.cb.OnPauseToggle(m.paused)
		}
	case "+", "=":
		if m.cb.OnBandwidthDelta != nil {
			m.cb.OnBandwidthDelta(bandwidthUpFactor)
		}
	case "-", "_":
		if m.cb.OnBandwidthDelta != nil {
			m.cb.OnBandwidthDelta(bandwidthDownFactor)
		}
	}
	return m, nil
}

func (m model) handleLogsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	page := m.logsPageSize()
	switch msg.String() {
	case "e":
		m.logs.filter = m.logs.filter.next()
		// Refiltering changes the visible set; re-anchor the window.
		m.logs.offset = min(m.logs.offset, m.logs.maxOffset(page))
	case "up", "k":
		m.logs.scrollBy(-1, page)
	case "down", "j":
		m.logs.scrollBy(1, page)
	case "pgup":
		m.logs.scrollBy(-page, page)
	case "pgdown":
		m.logs.scrollBy(page, page)
	case "g":
		m.logs.follow = false
		m.logs.offset = 0
	case "G":
		m.logs.follow = true
	}
	return m, nil
}
