package otel

import (
	"testing"
	"time"
)

func mapGetenv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestEnabled(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{"empty env", map[string]string{}, false},
		{"sdk disabled alone", map[string]string{"OTEL_SDK_DISABLED": "true"}, false},
		{"sdk disabled wins", map[string]string{
			"OTEL_SDK_DISABLED": "true", "OTEL_EXPORTER_OTLP_ENDPOINT": "http://x",
		}, false},
		{"sdk disabled case insensitive", map[string]string{
			"OTEL_SDK_DISABLED": "TRUE", "OTEL_TRACES_EXPORTER": "otlp",
		}, false},
		{"sdk disabled false is not truthy", map[string]string{
			"OTEL_SDK_DISABLED": "false", "OTEL_EXPORTER_OTLP_ENDPOINT": "http://x",
		}, true},
		{"generic endpoint", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://x"}, true},
		{"traces endpoint", map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://x"}, true},
		{"metrics endpoint", map[string]string{"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://x"}, true},
		{"logs endpoint", map[string]string{"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://x"}, true},
		{"protocol", map[string]string{"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, true},
		{"headers", map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "k=v"}, true},
		{"traces exporter otlp", map[string]string{"OTEL_TRACES_EXPORTER": "otlp"}, true},
		// Even "none" means the user engaged the telemetry config surface.
		{"metrics exporter none", map[string]string{"OTEL_METRICS_EXPORTER": "none"}, true},
		{"logs exporter", map[string]string{"OTEL_LOGS_EXPORTER": "otlp"}, true},
		{"unrelated env", map[string]string{"HF_TOKEN": "x", "HOME": "/y"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Enabled(mapGetenv(tc.env)); got != tc.want {
				t.Fatalf("Enabled(%v) = %v, want %v", tc.env, got, tc.want)
			}
		})
	}
}

func TestSignalExporter(t *testing.T) {
	cases := []struct {
		env         map[string]string
		wantEnabled bool
		wantUnknown string
	}{
		{map[string]string{}, true, ""},
		{map[string]string{"OTEL_TRACES_EXPORTER": "otlp"}, true, ""},
		{map[string]string{"OTEL_TRACES_EXPORTER": "OTLP"}, true, ""},
		{map[string]string{"OTEL_TRACES_EXPORTER": "none"}, false, ""},
		{map[string]string{"OTEL_TRACES_EXPORTER": "zipkin"}, true, "zipkin"},
	}
	for i, tc := range cases {
		on, unknown := signalExporter(mapGetenv(tc.env), "TRACES")
		if on != tc.wantEnabled || unknown != tc.wantUnknown {
			t.Fatalf("case %d: got (%v, %q), want (%v, %q)", i, on, unknown, tc.wantEnabled, tc.wantUnknown)
		}
	}
}

func TestProtocolFor(t *testing.T) {
	cases := []struct {
		env  map[string]string
		want string
	}{
		{map[string]string{}, "http/protobuf"},
		{map[string]string{"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, "grpc"},
		{map[string]string{"OTEL_EXPORTER_OTLP_PROTOCOL": "http/json"}, "http/json"},
		{map[string]string{"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "grpc"}, "grpc"},
		{ // per-signal wins over generic
			map[string]string{
				"OTEL_EXPORTER_OTLP_PROTOCOL":        "http/protobuf",
				"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "grpc",
			}, "grpc"},
	}
	for i, tc := range cases {
		if got := protocolFor(mapGetenv(tc.env), "TRACES"); got != tc.want {
			t.Fatalf("case %d: got %q, want %q", i, got, tc.want)
		}
	}
}

func TestMetricExportInterval(t *testing.T) {
	cases := []struct {
		env  map[string]string
		want time.Duration
	}{
		{map[string]string{}, 15 * time.Second}, // 15s default
		{map[string]string{"OTEL_METRIC_EXPORT_INTERVAL": "200"}, 200 * time.Millisecond},
		{map[string]string{"OTEL_METRIC_EXPORT_INTERVAL": "30000"}, 30 * time.Second},
		{map[string]string{"OTEL_METRIC_EXPORT_INTERVAL": "garbage"}, 15 * time.Second},
		{map[string]string{"OTEL_METRIC_EXPORT_INTERVAL": "-5"}, 15 * time.Second},
	}
	for i, tc := range cases {
		if got := metricExportInterval(mapGetenv(tc.env)); got != tc.want {
			t.Fatalf("case %d: got %v, want %v", i, got, tc.want)
		}
	}
}
