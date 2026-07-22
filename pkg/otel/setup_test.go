package otel

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestTraceSampler(t *testing.T) {
	tests := []struct {
		name, arg, want string
		wantErr         bool
	}{
		{"always_on", "", "AlwaysOnSampler", false},
		{"always_off", "", "AlwaysOffSampler", false},
		{"traceidratio", "0.25", "TraceIDRatioBased{0.25}", false},
		{"parentbased_always_on", "", "ParentBased{root:AlwaysOnSampler", false},
		{"parentbased_always_off", "", "ParentBased{root:AlwaysOffSampler", false},
		{"parentbased_traceidratio", "0.5", "ParentBased{root:TraceIDRatioBased{0.5}", false},
		{"unknown", "", "", true},
		{"traceidratio", "2", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name+tc.arg, func(t *testing.T) {
			env := func(key string) string {
				if key == "OTEL_TRACES_SAMPLER" {
					return tc.name
				}
				if key == "OTEL_TRACES_SAMPLER_ARG" {
					return tc.arg
				}
				return ""
			}
			s, err := traceSampler(env)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && !strings.Contains(s.Description(), tc.want) {
				t.Fatalf("description = %q, want substring %q", s.Description(), tc.want)
			}
		})
	}
}

// otlpFixture is a local OTLP/HTTP collector: it records decoded export
// requests per signal path. No network is touched (httptest loopback).
type otlpFixture struct {
	srv *httptest.Server

	mu      sync.Mutex
	traces  []*coltrace.ExportTraceServiceRequest
	metrics []*colmetric.ExportMetricsServiceRequest
	logs    []*collog.ExportLogsServiceRequest
}

func newOTLPFixture(t *testing.T) *otlpFixture {
	t.Helper()
	f := &otlpFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", func(w http.ResponseWriter, r *http.Request) {
		sink(f, w, r, &f.traces, func() *coltrace.ExportTraceServiceRequest { return &coltrace.ExportTraceServiceRequest{} })
	})
	mux.HandleFunc("/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		sink(f, w, r, &f.metrics, func() *colmetric.ExportMetricsServiceRequest { return &colmetric.ExportMetricsServiceRequest{} })
	})
	mux.HandleFunc("/v1/logs", func(w http.ResponseWriter, r *http.Request) {
		sink(f, w, r, &f.logs, func() *collog.ExportLogsServiceRequest { return &collog.ExportLogsServiceRequest{} })
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func sink[T proto.Message](f *otlpFixture, w http.ResponseWriter, r *http.Request, dst *[]T, mk func() T) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	msg := mk()
	if err := proto.Unmarshal(body, msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	*dst = append(*dst, msg)
	f.mu.Unlock()
	// Empty protobuf response body = success per OTLP/HTTP spec.
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
}

func (f *otlpFixture) traceCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, req := range f.traces {
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				n += len(ss.Spans)
			}
		}
	}
	return n
}

func (f *otlpFixture) metricNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, req := range f.metrics {
		for _, rm := range req.ResourceMetrics {
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					out = append(out, m.Name)
				}
			}
		}
	}
	return out
}

func (f *otlpFixture) logBodies() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, req := range f.logs {
		for _, rl := range req.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				for _, lr := range sl.LogRecords {
					out = append(out, lr.Body.GetStringValue())
				}
			}
		}
	}
	return out
}

func (f *otlpFixture) resourceAttrs() []*commonv1.KeyValue {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, req := range f.traces {
		for _, rs := range req.ResourceSpans {
			return rs.Resource.Attributes
		}
	}
	return nil
}

// TestSetupExportsAllSignals runs an enabled Setup against the fixture and
// asserts spans, gauge metrics (fed by MetricsSource) and bridged log
// records all arrive after Shutdown's final flush.
func TestSetupExportsAllSignals(t *testing.T) {
	fix := newOTLPFixture(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", fix.srv.URL)
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "100")
	// Keep batch processors snappy; Shutdown flushes regardless.
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "100")

	src := &fakeSource{m: fullMetrics()}
	p, err := Setup(t.Context(), os.Getenv, "1.2.3-test", src, nil, nil)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if !p.Enabled {
		t.Fatal("Providers.Enabled = false with exporter env set")
	}

	// One span through the derived tracer.
	_, span := p.Tracer("hfdl.test").Start(t.Context(), "test-span")
	span.End()

	// One log record through the bridge.
	slog.New(p.SlogBridge()).InfoContext(t.Context(), "fixture-log-line")

	// Shutdown forces the final collect+export for all three signals.
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if n := fix.traceCount(); n != 1 {
		t.Fatalf("fixture received %d spans, want 1", n)
	}

	// Pinned resource identity: service.name=hfdl, version from pkg/config.
	var svcName, svcVer string
	for _, kv := range fix.resourceAttrs() {
		switch kv.Key {
		case "service.name":
			svcName = kv.Value.GetStringValue()
		case "service.version":
			svcVer = kv.Value.GetStringValue()
		}
	}
	if svcName != "hfdl" {
		t.Fatalf("service.name = %q, want hfdl", svcName)
	}
	if svcVer != "1.2.3-test" {
		t.Fatalf("service.version = %q, want 1.2.3-test", svcVer)
	}

	// Metrics: gauges exported, fed from MetricsSource (Collect ran).
	names := fix.metricNames()
	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	for _, want := range []string{"hfdl.download.speed", "hfdl.queue.depth", "hfdl.connections"} {
		if !found[want] {
			t.Fatalf("metric %q not exported; got %v", want, names)
		}
	}
	if src.calls.Load() == 0 {
		t.Fatal("MetricsSource.Collect never invoked")
	}

	// Logs: bridged record arrived.
	bodies := fix.logBodies()
	foundLog := false
	for _, b := range bodies {
		if b == "fixture-log-line" {
			foundLog = true
		}
	}
	if !foundLog {
		t.Fatalf("bridged log record not exported; got %v", bodies)
	}
}

// TestSetupSignalToggles verifies OTEL_<SIGNAL>_EXPORTER=none disables just
// that signal's export path.
func TestSetupSignalToggles(t *testing.T) {
	fix := newOTLPFixture(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", fix.srv.URL)
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "100")

	src := &fakeSource{m: fullMetrics()}
	p, err := Setup(t.Context(), os.Getenv, "1.0.0", src, nil, nil)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer func() { _ = p.Shutdown(t.Context()) }()

	// Disabled signals degenerate: bridge drops records, transport still
	// wraps (traces enabled).
	if p.SlogBridge().Enabled(t.Context(), slog.LevelInfo) {
		t.Fatal("bridge enabled despite OTEL_LOGS_EXPORTER=none")
	}

	_, span := p.Tracer("hfdl.test").Start(t.Context(), "only-traces")
	span.End()
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if n := fix.traceCount(); n != 1 {
		t.Fatalf("fixture received %d spans, want 1", n)
	}
	if names := fix.metricNames(); len(names) != 0 {
		t.Fatalf("metrics exported despite OTEL_METRICS_EXPORTER=none: %v", names)
	}
	if src.calls.Load() != 0 {
		t.Fatal("Collect invoked despite metrics exporter disabled")
	}
	if bodies := fix.logBodies(); len(bodies) != 0 {
		t.Fatalf("logs exported despite OTEL_LOGS_EXPORTER=none: %v", bodies)
	}
}

// TestHTTPTransportEnabled verifies the enabled path wraps the base
// transport with otelhttp instrumentation.
func TestHTTPTransportEnabled(t *testing.T) {
	fix := newOTLPFixture(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", fix.srv.URL)
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")

	p, err := Setup(t.Context(), os.Getenv, "1.0.0", nil, nil, nil)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer func() { _ = p.Shutdown(t.Context()) }()

	base := http.DefaultTransport
	got := p.HTTPTransport(base)
	if got == base {
		t.Fatal("enabled HTTPTransport returned base unchanged")
	}
	// RoundTrip through the wrapper must work (fixture is a plain server).
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, fix.srv.URL+"/v1/traces", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := got.RoundTrip(req)
	if err != nil {
		t.Fatalf("wrapped RoundTrip: %v", err)
	}
	_ = resp.Body.Close()
}
