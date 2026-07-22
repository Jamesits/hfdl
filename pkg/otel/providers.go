package otel

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	otellog "go.opentelemetry.io/otel/log"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Providers bundles the three signal providers cmd injects into every
// package. The zero value is NOT usable; obtain it via Setup or Noop.
//
// When Enabled is false every field holds an API-level noop implementation:
// deriving tracers/meters, bridging slog and wrapping transports have no
// telemetry side effects, and Shutdown succeeds immediately.
type Providers struct {
	Enabled bool

	tp trace.TracerProvider
	mp metric.MeterProvider
	lp otellog.LoggerProvider

	bridge slog.Handler

	// shutdowns run in flush order: tracer → meter → logger. Guarded by
	// once so repeated/concurrent Shutdown calls are idempotent.
	shutdowns []func(context.Context) error
	once      sync.Once
	shutErr   error
}

// Noop returns disabled providers: no goroutines, no connections, every
// accessor a no-op.
func Noop() *Providers {
	return &Providers{
		Enabled: false,
		tp:      tracenoop.NewTracerProvider(),
		mp:      metricnoop.NewMeterProvider(),
		lp:      lognoop.NewLoggerProvider(),
		bridge:  discardHandler{},
	}
}

// Tracer derives a named tracer (convention: "hfdl.<pkg>").
func (p *Providers) Tracer(name string) trace.Tracer {
	return p.tp.Tracer(name)
}

// Meter derives a named meter (convention: "hfdl.<pkg>").
func (p *Providers) Meter(name string) metric.Meter {
	return p.mp.Meter(name)
}

// SlogBridge returns the otelslog bridge handler to attach as the fourth
// fan-out target of logging.Handler. When disabled it returns a handler
// whose Enabled is always false, so slog drops records before formatting
// them — cheap and nil-safe.
func (p *Providers) SlogBridge() slog.Handler {
	return p.bridge
}

// HTTPTransport wraps base with otelhttp instrumentation (W3C traceparent
// propagation toward Hub/CDN/CAS). When disabled it returns base unchanged.
// A nil base means http.DefaultTransport.
//
// The configured TracerProvider and propagator are passed explicitly: hfdl
// keeps its providers local (it never calls otel.SetTracerProvider), so
// without WithTracerProvider otelhttp would emit spans through the global
// noop. The propagator is the composite Setup installs globally.
func (p *Providers) HTTPTransport(base http.RoundTripper) http.RoundTripper {
	if !p.Enabled {
		return base
	}
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base,
		otelhttp.WithTracerProvider(p.tp),
		otelhttp.WithPropagators(otel.GetTextMapPropagator()),
	)
}

// Shutdown flushes and shuts providers down in order — tracer, then
// meter, then logger — each with the passed ctx (cmd derives a 5s budget
// from the command ctx). It is idempotent: later calls return the first
// call's joined error.
func (p *Providers) Shutdown(ctx context.Context) error {
	p.once.Do(func() {
		for _, sd := range p.shutdowns {
			if err := sd(ctx); err != nil {
				p.shutErr = errors.Join(p.shutErr, err)
			}
		}
	})
	return p.shutErr
}

// discardHandler drops all records. Reporting Enabled=false makes slog skip
// record construction entirely, which is the cheapest possible disabled
// bridge.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }
