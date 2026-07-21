// Package tui renders the hfdl terminal dashboard with bubbletea.
//
// The UI is a pure function of two polled sources: a snapshot callback
// (sched's Stats, adapted by cmd onto the local Snapshot mirror so tui
// never imports the scheduler) and the logging ring (logs tab scrollback,
// WARN+ badge). Nothing is ever written to stdout/stderr outside the
// bubbletea program: while the TUI owns the screen, logs land in the ring
// and the store's logs table.
//
// Layout (dashboard tab): header (repo@rev, bytes, speed, ETA), queue
// depths, current-file progress bars, pending summary, footer
// (bandwidth/api/disk-duty/cooldowns/stalls/retries/salvaged), key help.
// Logs tab: ring scrollback with level filter and follow mode.
package tui
