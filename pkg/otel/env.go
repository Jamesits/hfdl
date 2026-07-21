package otel

import (
	"strconv"
	"strings"
	"time"
)

// Environment variables honored by hfdl: the standard OTEL_* family. The
// SDK additionally honors OTEL_BSP_*, OTEL_BLRP_*,
// OTEL_TRACES_SAMPLER(+_ARG) and the full OTEL_EXPORTER_OTLP_* option set
// natively.
const (
	envSDKDisabled = "OTEL_SDK_DISABLED"

	envOTLPEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTLPProtocol = "OTEL_EXPORTER_OTLP_PROTOCOL"
	envOTLPHeaders  = "OTEL_EXPORTER_OTLP_HEADERS"

	envTracesExporter  = "OTEL_TRACES_EXPORTER"
	envMetricsExporter = "OTEL_METRICS_EXPORTER"
	envLogsExporter    = "OTEL_LOGS_EXPORTER"

	envMetricExportInterval = "OTEL_METRIC_EXPORT_INTERVAL"
)

// defaultMetricExportInterval is the collection tick: the SDK default of
// 60s would alias away the 10s windowed rates from pkg/throttle and
// pkg/stats, so it is overridden to 15s.
const defaultMetricExportInterval = 15 * time.Second

// exporterEnvKeys are the variables whose presence (any one) means the user
// opted into telemetry. Presence of any → Enabled, unless OTEL_SDK_DISABLED
// is truthy.
var exporterEnvKeys = []string{
	envOTLPEndpoint,
	"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
	"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
	envOTLPProtocol,
	envOTLPHeaders,
	envTracesExporter,
	envMetricsExporter,
	envLogsExporter,
}

// Enabled reports whether telemetry should be active: any OTEL exporter
// environment variable is set and OTEL_SDK_DISABLED is not truthy.
func Enabled(getenv func(string) string) bool {
	if truthy(getenv(envSDKDisabled)) {
		return false
	}
	for _, k := range exporterEnvKeys {
		if getenv(k) != "" {
			return true
		}
	}
	return false
}

func truthy(v string) bool {
	return strings.EqualFold(strings.TrimSpace(v), "true")
}

// signalExporter reports whether the given signal ("TRACES", "METRICS",
// "LOGS") should be exported: unset or "otlp" → enabled, "none" → disabled.
// Unknown values follow the OTel convention of warning and falling back to
// the default (otlp); Setup logs the warning.
func signalExporter(getenv func(string) string, signal string) (enabled bool, unknown string) {
	v := strings.ToLower(strings.TrimSpace(getenv("OTEL_" + signal + "_EXPORTER")))
	switch v {
	case "", "otlp":
		return true, ""
	case "none":
		return false, ""
	default:
		return true, v
	}
}

// protocolFor resolves the OTLP transport for a signal: per-signal
// OTEL_EXPORTER_OTLP_<SIGNAL>_PROTOCOL wins over the generic
// OTEL_EXPORTER_OTLP_PROTOCOL. Only "grpc" switches transport; everything
// else (http/protobuf, http/json, unset) uses the HTTP exporter — hfdl only
// ships the two supported protocol stacks (HTTP default, gRPC opt-in).
func protocolFor(getenv func(string) string, signal string) string {
	if v := getenv("OTEL_EXPORTER_OTLP_" + signal + "_PROTOCOL"); v != "" {
		return strings.ToLower(strings.TrimSpace(v))
	}
	if v := getenv(envOTLPProtocol); v != "" {
		return strings.ToLower(strings.TrimSpace(v))
	}
	return "http/protobuf"
}

// metricExportInterval resolves OTEL_METRIC_EXPORT_INTERVAL (milliseconds
// per the OTel spec) against the 15s default. Unparseable or
// non-positive values fall back to the default.
func metricExportInterval(getenv func(string) string) time.Duration {
	v := strings.TrimSpace(getenv(envMetricExportInterval))
	if v == "" {
		return defaultMetricExportInterval
	}
	if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	return defaultMetricExportInterval
}
