// Package logging provides hfdl's slog fan-out root: every
// record above a single level floor lands in a bounded in-memory ring (the
// TUI logs view), optionally on stderr as text (nil writer = fully silenced,
// mandatory in TUI mode where bubbletea owns the terminal), in an async
// batched DB sink implemented by store behind the LogSink interface, and in
// any additionally attached slog.Handler (the otelslog OTLP bridge).
//
// Bootstrap order matters: cmd starts with stderr only, then attaches the DB
// and OTLP sinks once the store/providers exist — both replays include the
// pre-attach ring contents so early bootstrap records are never lost.
//
// The DB sink is strictly best-effort: a single background goroutine batches
// rows (250ms or 256 records) and a full queue or a failing sink only
// increments DropCount — Handle never blocks a worker on log IO.
package logging
