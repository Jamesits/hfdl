// Package otel wires OpenTelemetry tracing, metrics and logs for hfdl.
//
// Telemetry is optional and off by default: with no OTEL_* exporter
// environment variable set, Setup returns noop providers — no goroutines,
// no connections, and every accessor degenerates to a no-op. When enabled,
// configuration is env-only (the standard OTEL_* set) so the CLI
// surface stays a drop-in hf replacement.
//
// Hard rules enforced here:
//   - Packages never construct providers; cmd builds one *Providers via
//     Setup and injects it downstream. Packages derive named tracers/meters
//     via Tracer("hfdl.<pkg>")/Meter("hfdl.<pkg>").
//   - No metric attribute ever carries a file path/id, block id, or repo
//     name (cardinality guard, see instrumentSpecs and the lint test).
//   - Metric callbacks read an injected MetricsSource (adapted by cmd onto
//     sched.Manager.Snapshot + throttle stats); otel imports neither sched
//     nor store.
package otel // import "github.com/jamesits/hfdl/pkg/otel"
