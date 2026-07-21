package otel

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"

	"github.com/jamesits/hfdl/pkg/config"
)

// serviceName is the pinned resource identity, service.name=hfdl;
// OTEL_RESOURCE_ATTRIBUTES may add anything else but not rename the
// service.
const serviceName = "hfdl"

// Setup builds telemetry providers from the OTEL_* environment (env-only
// configuration; no CLI flags). getenv is the env source (cmd passes
// os.Getenv; the SDK reads process env natively, so the two must agree).
//
// With no exporter env set (the default) Setup returns Noop() and nil —
// no goroutines, no connections. Otherwise it constructs OTLP exporters
// (HTTP default, gRPC when protocol=grpc), a batch span processor
// (OTEL_BSP_*), a periodic metric reader (OTEL_METRIC_EXPORT_INTERVAL,
// default 15s), and a batch log processor behind an otelslog bridge.
//
// src feeds the async gauges once per collection tick; nil skips gauge
// registration. log receives export-failure reports (best-effort: failures
// are counted and logged, never propagated to callers).
func Setup(ctx context.Context, getenv func(string) string, version string, src MetricsSource, log *slog.Logger) (*Providers, error) {
	if !Enabled(getenv) {
		return Noop(), nil
	}
	if version == "" {
		version = config.Version
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),      // OTEL_RESOURCE_ATTRIBUTES, OTEL_SERVICE_NAME
		resource.WithTelemetrySDK(), // SDK name/version/language
		resource.WithHost(),         // host.name, host.arch
		resource.WithAttributes( // pinned identity — wins over env on conflict
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: resource: %w", err)
	}

	p := &Providers{Enabled: true, bridge: discardHandler{}}
	// API-level noops as starting point; each enabled signal replaces its
	// provider and appends its shutdown in flush order (tracer→meter→logger).
	noop := Noop()
	p.tp, p.mp, p.lp = noop.tp, noop.mp, noop.lp

	errHandler := newExportErrorHandler(log)
	otel.SetErrorHandler(errHandler)
	// Outbound propagation (otelhttp reads the global propagator): W3C
	// traceparent + baggage toward Hub/CDN/CAS.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if on, unknown := signalExporter(getenv, "TRACES"); unknown != "" {
		errHandler.logUnknown(envTracesExporter, unknown)
	} else if on {
		tp, err := setupTracer(ctx, getenv, res)
		if err != nil {
			return nil, err
		}
		p.tp = tp
		p.shutdowns = append(p.shutdowns, tp.Shutdown)
	}

	if on, unknown := signalExporter(getenv, "METRICS"); unknown != "" {
		errHandler.logUnknown(envMetricsExporter, unknown)
	} else if on {
		mp, err := setupMeter(ctx, getenv, res, src)
		if err != nil {
			return nil, err
		}
		p.mp = mp
		p.shutdowns = append(p.shutdowns, mp.Shutdown)
	}

	if on, unknown := signalExporter(getenv, "LOGS"); unknown != "" {
		errHandler.logUnknown(envLogsExporter, unknown)
	} else if on {
		lp, err := setupLogger(ctx, getenv, res)
		if err != nil {
			return nil, err
		}
		p.lp = lp
		p.bridge = otelslog.NewHandler(serviceName,
			otelslog.WithLoggerProvider(lp),
			otelslog.WithSource(true),
		)
		p.shutdowns = append(p.shutdowns, lp.Shutdown)
	}

	return p, nil
}

// setupTracer builds the SDK TracerProvider. The SDK honors
// OTEL_TRACES_SAMPLER(+_ARG) with a parentbased_always_on default, and the
// batch processor honors OTEL_BSP_*; exporters read OTEL_EXPORTER_OTLP_*.
func setupTracer(ctx context.Context, getenv func(string) string, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	var (
		exp sdktrace.SpanExporter
		err error
	)
	if protocolFor(getenv, "TRACES") == "grpc" {
		exp, err = otlptracegrpc.New(ctx)
	} else {
		exp, err = otlptracehttp.New(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("otel: trace exporter: %w", err)
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exp),
	), nil
}

// setupMeter builds the SDK MeterProvider with a periodic reader on the
// 15s tick (OTEL_METRIC_EXPORT_INTERVAL overrides it) and
// registers the async gauges against src.
func setupMeter(ctx context.Context, getenv func(string) string, res *resource.Resource, src MetricsSource) (*sdkmetric.MeterProvider, error) {
	var (
		exp sdkmetric.Exporter
		err error
	)
	if protocolFor(getenv, "METRICS") == "grpc" {
		exp, err = otlpmetricgrpc.New(ctx)
	} else {
		exp, err = otlpmetrichttp.New(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("otel: metric exporter: %w", err)
	}
	reader := sdkmetric.NewPeriodicReader(exp,
		sdkmetric.WithInterval(metricExportInterval(getenv)),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
	)
	if src != nil {
		if err := registerGauges(mp.Meter(serviceName), src); err != nil {
			return nil, err
		}
	}
	return mp, nil
}

// setupLogger builds the SDK LoggerProvider with a batch processor;
// export is asynchronous and best-effort.
func setupLogger(ctx context.Context, getenv func(string) string, res *resource.Resource) (*sdklog.LoggerProvider, error) {
	var (
		exp sdklog.Exporter
		err error
	)
	if protocolFor(getenv, "LOGS") == "grpc" {
		exp, err = otlploggrpc.New(ctx)
	} else {
		exp, err = otlploghttp.New(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("otel: log exporter: %w", err)
	}
	return sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
	), nil
}

// exportErrorHandler counts export failures and reports them through the
// injected logger at Debug level. Debug avoids a feedback loop: a failing
// log exporter must not generate fresh log records through the fan-out
// root (which includes the OTLP bridge) at visible levels.
type exportErrorHandler struct {
	log      *slog.Logger
	failures atomic.Int64
}

func newExportErrorHandler(log *slog.Logger) *exportErrorHandler {
	return &exportErrorHandler{log: log}
}

func (h *exportErrorHandler) Handle(err error) {
	h.failures.Add(1)
	if h.log != nil {
		h.log.Debug("otel export failed", "err", err, "total_failures", h.failures.Load())
	}
}

func (h *exportErrorHandler) logUnknown(key, value string) {
	if h.log != nil {
		h.log.Warn("unknown OTEL exporter value, falling back to otlp", "key", key, "value", value)
	}
}
