// Package stats implements the process-wide download statistics registry:
// atomic counters, EMA rate estimates (α=0.2) per file, upstream, and global
// (the upstream-policy signal), and 10s windowed per-second rate rings per
// file, upstream, and global for the speeds the TUI and OTel exporters
// display.
//
// The hot path (AddNetwork/AddFile/AddUpstream) is a handful of atomic adds
// and one short per-ring critical section; Snapshot is a deep copy taken at
// 4Hz by the TUI and never mutates registry state. No background goroutines:
// ring windows advance lazily on access.
package stats
