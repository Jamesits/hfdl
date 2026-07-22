package otel

import (
	"context"
	"log/slog"
	"net"
	"net/http"
)

// Egress carries caller-built network plumbing for the OTLP exporters, so
// telemetry sockets get the same proxy and IP QoS treatment as the rest of
// hfdl (http.Client is a hookable type that must come from upstream). A nil
// *Egress or nil field keeps the exporter's own env-driven defaults.
type Egress struct {
	// HTTPClient is injected into http/protobuf exporters (WithHTTPClient).
	HTTPClient *http.Client
	// GRPCDialer dials raw TCP for gRPC exporters (grpc.WithContextDialer).
	// grpc-go still applies its own *_PROXY env handling around it.
	GRPCDialer func(context.Context, string) (net.Conn, error)
}

// transportEnvVars are the OTLP env vars WithHTTPClient would silently
// override (per its documented precedence): honoring them wins over socket
// tagging, so any of them being set disables client injection for the
// signal.
var transportEnvVars = []string{"CERTIFICATE", "CLIENT_CERTIFICATE", "CLIENT_KEY", "TIMEOUT"}

// httpClient returns the client to inject for one signal (TRACES, METRICS,
// LOGS), or nil to keep exporter defaults.
func (e *Egress) httpClient(getenv func(string) string, signal string, log *slog.Logger) *http.Client {
	if e == nil || e.HTTPClient == nil {
		return nil
	}
	for _, v := range transportEnvVars {
		for _, name := range []string{
			"OTEL_EXPORTER_OTLP_" + v,
			"OTEL_EXPORTER_OTLP_" + signal + "_" + v,
		} {
			if getenv(name) != "" {
				if log != nil {
					log.Warn("OTLP transport env var set; telemetry sockets keep exporter defaults (no --hfdl-proxy/--hfdl-ipqos)",
						"signal", signal, "env", name)
				}
				return nil
			}
		}
	}
	return e.HTTPClient
}

func (e *Egress) grpcDialer() func(context.Context, string) (net.Conn, error) {
	if e == nil {
		return nil
	}
	return e.GRPCDialer
}
