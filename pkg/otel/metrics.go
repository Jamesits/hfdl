package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// CooldownSample is one labeled point of hfdl.cooldown.remaining.
type CooldownSample struct {
	Target  string  // endpoint or upstream being cooled down
	Kind    string  // api | cas | upstream
	Seconds float64 // remaining cooldown time
}

// Metrics is a plain-value snapshot feeding the async gauges. cmd adapts
// sched.Manager.Snapshot(), stats.Registry and
// throttle Bucket/DutyLimiter stats onto it; pkg/otel never imports those
// packages.
//
// The allowlist bounds attribute names, not value cardinality. Callers bound
// values: upstream endpoints are configuration-derived and few in practice,
// while bucket, queue, and media values come from code-controlled sets. Values
// must never contain file paths, block IDs, or repository names.
type Metrics struct {
	DownloadSpeed float64            // hfdl.download.speed (By/s)
	UpstreamSpeed map[string]float64 // hfdl.upstream.speed, key = upstream
	APIRate       float64            // hfdl.api.rate ({request}/s)

	BandwidthUtilization float64          // hfdl.bandwidth.utilization (1)
	ThrottleWaiters      map[string]int64 // hfdl.throttle.waiters, key = bucket (api|bandwidth)

	QueueDepth    map[string]int64 // hfdl.queue.depth, key = queue (meta|download|disk|install)
	QueueInFlight map[string]int64 // hfdl.queue.in_flight, key = queue

	Connections         int64            // hfdl.connections, global (no upstream attr)
	UpstreamConnections map[string]int64 // hfdl.connections, key = upstream

	DiskDutyLevel       map[string]int64   // hfdl.disk.duty.level (%), key = media
	DiskDutyActiveRatio map[string]float64 // hfdl.disk.duty.active_ratio (1), key = media

	Cooldowns []CooldownSample // hfdl.cooldown.remaining (s)
}

// MetricsSource feeds the async gauges. Collect is invoked at most once per
// collection tick (one export interval), no matter how many gauges are
// registered.
type MetricsSource interface {
	Collect() Metrics
}

// instrumentKind distinguishes gauge/counter shapes in the instrument table.
type instrumentKind int

const (
	kindGaugeF64 instrumentKind = iota
	kindGaugeI64
	kindCounterI64
	kindCounterF64
)

// instrumentSpec is one row of the telemetry design's metric table. The
// table is the single source of truth: gauge registration iterates it, and the
// registration-time cardinality lint test reflects over it to prove no
// instrument may carry high-cardinality attributes (file path/id, block id,
// repo name).
type instrumentSpec struct {
	name  string
	kind  instrumentKind
	unit  string
	attrs []string // attribute keys this instrument may use; nil = none
}

// allowedAttrKeys is the exhaustive set of attribute keys any hfdl metric
// may carry. Metric attributes never carry file path/id, block id, or repo
// name — only coarse dimensions like endpoint, upstream, and target;
// per-file detail lives in traces and logs. The lint test asserts every
// spec's attrs ⊆ this.
var allowedAttrKeys = []string{
	"endpoint", "upstream", "kind", "result", "bucket", "queue", "media", "target",
}

var (
	attrBucket   = []string{"bucket"}
	attrQueue    = []string{"queue"}
	attrMedia    = []string{"media"}
	attrUpstream = []string{"upstream"}
)

// instrumentSpecs mirrors the metric table verbatim. Sync
// counters are owned (created and incremented) by other packages via
// Providers.Counter/FloatCounter; they are listed here so the cardinality
// lint covers the whole surface.
var instrumentSpecs = []instrumentSpec{
	{name: "hfdl.download.speed", kind: kindGaugeF64, unit: "By/s"},
	{name: "hfdl.download.bytes", kind: kindCounterI64, unit: "By", attrs: []string{"kind"}},
	{name: "hfdl.upstream.speed", kind: kindGaugeF64, unit: "By/s", attrs: attrUpstream},
	{name: "hfdl.api.rate", kind: kindGaugeF64, unit: "{request}/s"},
	{name: "hfdl.api.requests", kind: kindCounterI64, unit: "{request}", attrs: []string{"endpoint", "result"}},
	{name: "hfdl.bandwidth.utilization", kind: kindGaugeF64, unit: "1"},
	{name: "hfdl.throttle.waiters", kind: kindGaugeI64, unit: "{caller}", attrs: attrBucket},
	{name: "hfdl.throttle.wait_seconds", kind: kindCounterF64, unit: "s", attrs: attrBucket},
	{name: "hfdl.queue.depth", kind: kindGaugeI64, unit: "{row}", attrs: attrQueue},
	{name: "hfdl.queue.in_flight", kind: kindGaugeI64, unit: "{worker}", attrs: attrQueue},
	{name: "hfdl.connections", kind: kindGaugeI64, unit: "{connection}", attrs: attrUpstream},
	{name: "hfdl.disk.duty.level", kind: kindGaugeI64, unit: "%", attrs: attrMedia},
	{name: "hfdl.disk.duty.active_ratio", kind: kindGaugeF64, unit: "1", attrs: attrMedia},
	{name: "hfdl.cooldown.remaining", kind: kindGaugeF64, unit: "s", attrs: []string{"target", "kind"}},
	{name: "hfdl.stalls", kind: kindCounterI64, unit: "{event}", attrs: attrUpstream},
	{name: "hfdl.block.retries", kind: kindCounterI64, unit: "{event}", attrs: attrUpstream},
	{name: "hfdl.verify.failures", kind: kindCounterI64, unit: "{event}"},
	{name: "hfdl.files.completed", kind: kindCounterI64, unit: "{file}"},
	{name: "hfdl.files.failed", kind: kindCounterI64, unit: "{file}"},
}

// gaugeSet holds the created async instruments so the single collection
// callback can observe them all from one MetricsSource.Collect() call.
type gaugeSet struct {
	downloadSpeed       metric.Float64ObservableGauge
	upstreamSpeed       metric.Float64ObservableGauge
	apiRate             metric.Float64ObservableGauge
	bandwidthUtil       metric.Float64ObservableGauge
	throttleWaiters     metric.Int64ObservableGauge
	queueDepth          metric.Int64ObservableGauge
	queueInFlight       metric.Int64ObservableGauge
	connections         metric.Int64ObservableGauge
	diskDutyLevel       metric.Int64ObservableGauge
	diskDutyActiveRatio metric.Float64ObservableGauge
	cooldownRemaining   metric.Float64ObservableGauge
}

// registerGauges creates every async gauge from instrumentSpecs and
// registers one callback that serves all of them from a single
// MetricsSource.Collect() per collection tick.
func registerGauges(meter metric.Meter, src MetricsSource) error {
	g := gaugeSet{}
	var err error
	mk := func(dst any, name string) {
		if err != nil {
			return
		}
		spec := specByName(name)
		switch p := dst.(type) {
		case *metric.Float64ObservableGauge:
			*p, err = meter.Float64ObservableGauge(name, metric.WithUnit(spec.unit))
		case *metric.Int64ObservableGauge:
			*p, err = meter.Int64ObservableGauge(name, metric.WithUnit(spec.unit))
		}
	}
	mk(&g.downloadSpeed, "hfdl.download.speed")
	mk(&g.upstreamSpeed, "hfdl.upstream.speed")
	mk(&g.apiRate, "hfdl.api.rate")
	mk(&g.bandwidthUtil, "hfdl.bandwidth.utilization")
	mk(&g.throttleWaiters, "hfdl.throttle.waiters")
	mk(&g.queueDepth, "hfdl.queue.depth")
	mk(&g.queueInFlight, "hfdl.queue.in_flight")
	mk(&g.connections, "hfdl.connections")
	mk(&g.diskDutyLevel, "hfdl.disk.duty.level")
	mk(&g.diskDutyActiveRatio, "hfdl.disk.duty.active_ratio")
	mk(&g.cooldownRemaining, "hfdl.cooldown.remaining")
	if err != nil {
		return fmt.Errorf("otel: create gauge: %w", err)
	}

	// One RegisterCallback for every gauge: the SDK invokes it once per
	// collection, so src.Collect() runs exactly once per tick.
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		m := src.Collect()

		o.ObserveFloat64(g.downloadSpeed, m.DownloadSpeed)
		for upstream, v := range m.UpstreamSpeed {
			o.ObserveFloat64(g.upstreamSpeed, v, metric.WithAttributes(attribute.String("upstream", upstream)))
		}
		o.ObserveFloat64(g.apiRate, m.APIRate)
		o.ObserveFloat64(g.bandwidthUtil, m.BandwidthUtilization)
		for bucket, v := range m.ThrottleWaiters {
			o.ObserveInt64(g.throttleWaiters, v, metric.WithAttributes(attribute.String("bucket", bucket)))
		}
		for queue, v := range m.QueueDepth {
			o.ObserveInt64(g.queueDepth, v, metric.WithAttributes(attribute.String("queue", queue)))
		}
		for queue, v := range m.QueueInFlight {
			o.ObserveInt64(g.queueInFlight, v, metric.WithAttributes(attribute.String("queue", queue)))
		}
		// Global connection count carries no upstream attribute: absent
		// means global.
		o.ObserveInt64(g.connections, m.Connections)
		for upstream, v := range m.UpstreamConnections {
			o.ObserveInt64(g.connections, v, metric.WithAttributes(attribute.String("upstream", upstream)))
		}
		for media, v := range m.DiskDutyLevel {
			o.ObserveInt64(g.diskDutyLevel, v, metric.WithAttributes(attribute.String("media", media)))
		}
		for media, v := range m.DiskDutyActiveRatio {
			o.ObserveFloat64(g.diskDutyActiveRatio, v, metric.WithAttributes(attribute.String("media", media)))
		}
		for _, c := range m.Cooldowns {
			o.ObserveFloat64(g.cooldownRemaining, c.Seconds, metric.WithAttributes(
				attribute.String("target", c.Target),
				attribute.String("kind", c.Kind),
			))
		}
		return nil
	},
		g.downloadSpeed, g.upstreamSpeed, g.apiRate, g.bandwidthUtil,
		g.throttleWaiters, g.queueDepth, g.queueInFlight, g.connections,
		g.diskDutyLevel, g.diskDutyActiveRatio, g.cooldownRemaining,
	)
	if err != nil {
		return fmt.Errorf("otel: register gauge callback: %w", err)
	}
	return nil
}

func specByName(name string) instrumentSpec {
	for _, s := range instrumentSpecs {
		if s.name == name {
			return s
		}
	}
	return instrumentSpec{name: name}
}

// Counter creates a generic Int64 counter on a package-owned meter.
// Packages own their instruments (names/units per the metric table); otel
// only provides the constructor so disabled deployments degenerate to
// API-level noops. When p is disabled the returned counter is the noop API
// implementation and err is nil.
func (p *Providers) Counter(meter metric.Meter, name, unit string) (metric.Int64Counter, error) {
	c, err := meter.Int64Counter(name, metric.WithUnit(unit))
	if err != nil {
		return nil, fmt.Errorf("otel: counter %s: %w", name, err)
	}
	return c, nil
}

// FloatCounter is the float64 counterpart of Counter (e.g.
// hfdl.throttle.wait_seconds).
func (p *Providers) FloatCounter(meter metric.Meter, name, unit string) (metric.Float64Counter, error) {
	c, err := meter.Float64Counter(name, metric.WithUnit(unit))
	if err != nil {
		return nil, fmt.Errorf("otel: counter %s: %w", name, err)
	}
	return c, nil
}
