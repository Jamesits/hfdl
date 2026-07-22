package otel

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"runtime"
	"testing"
	"time"
)

// TestSetupDisabled verifies the default (no exporter env) path: noop
// providers, no goroutines, no connections, identity transport.
func TestSetupDisabled(t *testing.T) {
	before := runtime.NumGoroutine()

	p, err := Setup(t.Context(), mapGetenv(map[string]string{}), "test", nil, nil, nil)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if p == nil {
		t.Fatal("Setup returned nil Providers")
	}
	if p.Enabled {
		t.Fatal("Providers.Enabled = true without exporter env")
	}

	// No goroutines may be started by the disabled path.
	if after := runtime.NumGoroutine(); after != before {
		t.Fatalf("goroutine delta = %d, want 0", after-before)
	}

	// HTTPTransport is the identity function when disabled.
	base := http.DefaultTransport
	if got := p.HTTPTransport(base); got != base {
		t.Fatalf("HTTPTransport(base) = %T, want identity %T", got, base)
	}
	if got := p.HTTPTransport(nil); got != nil {
		t.Fatalf("HTTPTransport(nil) = %v, want nil", got)
	}

	// Tracer/Meter are API-level noops but usable.
	tr := p.Tracer("hfdl.test")
	if tr == nil {
		t.Fatal("Tracer returned nil")
	}
	_, span := tr.Start(t.Context(), "noop-span")
	span.End() // must not panic
	if p.Meter("hfdl.test") == nil {
		t.Fatal("Meter returned nil")
	}

	// SlogBridge drops records cheaply: Enabled=false makes slog skip
	// record construction; Handle itself is a safe no-op.
	br := p.SlogBridge()
	if br == nil {
		t.Fatal("SlogBridge returned nil")
	}
	if br.Enabled(t.Context(), slog.LevelError) {
		t.Fatal("disabled bridge claims Enabled")
	}
	if err := br.Handle(t.Context(), slog.NewRecord(time.Now(), slog.LevelInfo, "x", 0)); err != nil {
		t.Fatalf("disabled bridge Handle: %v", err)
	}

	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestNoop(t *testing.T) {
	p := Noop()
	if p == nil || p.Enabled {
		t.Fatalf("Noop() = %+v, want disabled providers", p)
	}
	if got := p.HTTPTransport(http.DefaultTransport); got != http.DefaultTransport {
		t.Fatal("Noop HTTPTransport not identity")
	}
	if p.SlogBridge().Enabled(t.Context(), slog.LevelInfo) {
		t.Fatal("Noop bridge claims Enabled")
	}
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatalf("Noop Shutdown: %v", err)
	}
}

// TestShutdownOrderAndIdempotency pins the shutdown order
// (tracer→meter→logger) and that repeated Shutdown calls re-run nothing.
func TestShutdownOrderAndIdempotency(t *testing.T) {
	var calls []string
	rec := func(name string, err error) func(context.Context) error {
		return func(context.Context) error {
			calls = append(calls, name)
			return err
		}
	}
	errTracer := errors.New("tracer boom")
	errLogger := errors.New("logger boom")

	p := &Providers{
		shutdowns: []func(context.Context) error{
			rec("tracer", errTracer),
			rec("meter", nil),
			rec("logger", errLogger),
		},
	}

	err := p.Shutdown(t.Context())
	if err == nil || !errors.Is(err, errTracer) || !errors.Is(err, errLogger) {
		t.Fatalf("Shutdown error = %v, want joined tracer+logger errors", err)
	}
	want := []string{"tracer", "meter", "logger"}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls = %v, want order %v", calls, want)
		}
	}

	// Second Shutdown: no re-run, same error.
	err2 := p.Shutdown(t.Context())
	if len(calls) != 3 {
		t.Fatalf("second Shutdown re-ran shutdowns: calls = %v", calls)
	}
	if err2 != err {
		t.Fatalf("second Shutdown error = %v, want first call's %v", err2, err)
	}
}
