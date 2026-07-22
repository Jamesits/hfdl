package fcio

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// traceDetail gates the fine-grained fcio.read/fcio.fsync spans. It is
// process-global — set once from cmd via SetTraceDetail (sourced from
// HFDL_TRACE_FCIO_DETAIL) — because it is a diagnostic verbosity switch, not a
// per-call parameter. Off by default: these spans are per-file-pass detail
// whose volume is unwanted in normal runs.
//
// Per-op write spans are deliberately NOT emitted: fcio.File.WriteAt has no
// context on the hot path, so a write span would have no parent to nest under;
// the write cost is already captured by the enclosing transfer/verify/install
// spans. Only the two operations that carry a caller context (ReadAll,
// SyncFile) get detail spans.
var traceDetail atomic.Bool

// SetTraceDetail enables or disables the fcio detail spans. Call once at
// startup, before any Engine use.
func SetTraceDetail(on bool) { traceDetail.Store(on) }

// startDetailSpan opens a gated fcio detail span. When the gate is off it
// returns the ctx unchanged and a nil span (endDetailSpan is then a no-op).
// The tracer is derived from the span already in ctx (the enclosing
// download/verify/install span) rather than an injected one, so fcio needs no
// tracer plumbing: with telemetry disabled that span is the noop and this
// stays allocation-cheap.
func startDetailSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !traceDetail.Load() {
		return ctx, nil
	}
	tr := trace.SpanFromContext(ctx).TracerProvider().Tracer("hfdl.fcio")
	ctx, sp := tr.Start(ctx, name)
	if len(attrs) > 0 {
		sp.SetAttributes(attrs...)
	}
	return ctx, sp
}

func endDetailSpan(sp trace.Span) {
	if sp != nil {
		sp.End()
	}
}
