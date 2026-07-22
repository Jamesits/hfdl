package otel

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type fakeSource struct {
	calls atomic.Int64
	m     Metrics
}

func (f *fakeSource) Collect() Metrics {
	f.calls.Add(1)
	return f.m
}

func fullMetrics() Metrics {
	return Metrics{
		DownloadSpeed: 123.5,
		UpstreamSpeed: map[string]float64{"huggingface.co": 100, "cdn.example": 23.5},
		APIRate:       4.5,

		BandwidthUtilization: 0.75,
		ThrottleWaiters:      map[string]int64{"api": 2, "bandwidth": 3},

		QueueDepth:    map[string]int64{"meta": 1, "download": 2, "disk": 3, "install": 4},
		QueueInFlight: map[string]int64{"download": 8},

		Connections:         12,
		UpstreamConnections: map[string]int64{"huggingface.co": 12},

		DiskDutyLevel:       map[string]int64{"ssd": 70},
		DiskDutyActiveRatio: map[string]float64{"ssd": 0.3},

		Cooldowns: []CooldownSample{
			{Target: "huggingface.co", Kind: "api", Seconds: 42},
		},
	}
}

// collect runs one SDK collection against a manual reader and returns the
// exported metrics keyed by instrument name.
func collect(t *testing.T, src MetricsSource) (map[string]metricdata.Metrics, *metric.MeterProvider) {
	t.Helper()
	mr := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(mr))
	if err := registerGauges(mp.Meter("hfdl.test"), src); err != nil {
		t.Fatalf("registerGauges: %v", err)
	}
	var rm metricdata.ResourceMetrics
	if err := mr.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out, mp
}

func gaugeF64(t *testing.T, m metricdata.Metrics) []metricdata.DataPoint[float64] {
	t.Helper()
	g, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("%s: data type %T, want Gauge[float64]", m.Name, m.Data)
	}
	return g.DataPoints
}

func gaugeI64(t *testing.T, m metricdata.Metrics) []metricdata.DataPoint[int64] {
	t.Helper()
	g, ok := m.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("%s: data type %T, want Gauge[int64]", m.Name, m.Data)
	}
	return g.DataPoints
}

func attrsOf[T int64 | float64](dp metricdata.DataPoint[T]) map[string]string {
	out := map[string]string{}
	for _, kv := range dp.Attributes.ToSlice() {
		out[string(kv.Key)] = kv.Value.AsString()
	}
	return out
}

// TestGaugesCollect verifies every async gauge exports the MetricsSource
// snapshot values with the pinned attributes — and that one collection
// tick calls Collect exactly once for all gauges.
func TestGaugesCollect(t *testing.T) {
	src := &fakeSource{m: fullMetrics()}
	got, mp := collect(t, src)
	t.Cleanup(func() { _ = mp.Shutdown(t.Context()) })

	if c := src.calls.Load(); c != 1 {
		t.Fatalf("Collect called %d times in one tick, want 1 (one snapshot serves every gauge)", c)
	}

	// Unlabeled gauges.
	for name, want := range map[string]float64{
		"hfdl.download.speed":         123.5,
		"hfdl.api.rate":               4.5,
		"hfdl.bandwidth.utilization":  0.75,
		"hfdl.disk.duty.active_ratio": 0.3,
	} {
		dps := gaugeF64(t, got[name])
		if len(dps) == 0 {
			t.Fatalf("%s: no data points", name)
		}
		found := false
		for _, dp := range dps {
			if dp.Value == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: no data point with value %v (got %+v)", name, want, dps)
		}
	}

	// Labeled gauges: attribute key + value must match the source maps.
	labeledF64 := []struct {
		name, key, keyVal string
		want              float64
	}{
		{"hfdl.upstream.speed", "upstream", "huggingface.co", 100},
		{"hfdl.upstream.speed", "upstream", "cdn.example", 23.5},
		{"hfdl.cooldown.remaining", "target", "huggingface.co", 42},
	}
	for _, tc := range labeledF64 {
		found := false
		for _, dp := range gaugeF64(t, got[tc.name]) {
			if attrsOf(dp)[tc.key] == tc.keyVal && dp.Value == tc.want {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: no point {%s=%s}=%v", tc.name, tc.key, tc.keyVal, tc.want)
		}
	}

	labeledI64 := []struct {
		name, key, keyVal string
		want              int64
	}{
		{"hfdl.throttle.waiters", "bucket", "api", 2},
		{"hfdl.throttle.waiters", "bucket", "bandwidth", 3},
		{"hfdl.queue.depth", "queue", "meta", 1},
		{"hfdl.queue.depth", "queue", "install", 4},
		{"hfdl.queue.in_flight", "queue", "download", 8},
		{"hfdl.connections", "upstream", "huggingface.co", 12},
		{"hfdl.disk.duty.level", "media", "ssd", 70},
	}
	for _, tc := range labeledI64 {
		found := false
		for _, dp := range gaugeI64(t, got[tc.name]) {
			if attrsOf(dp)[tc.key] == tc.keyVal && dp.Value == tc.want {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: no point {%s=%s}=%d", tc.name, tc.key, tc.keyVal, tc.want)
		}
	}

	// Global connection count carries NO upstream attribute.
	foundGlobal := false
	for _, dp := range gaugeI64(t, got["hfdl.connections"]) {
		if dp.Attributes.Len() == 0 && dp.Value == 12 {
			foundGlobal = true
		}
	}
	if !foundGlobal {
		t.Fatal("hfdl.connections: no attribute-less global point with value 12")
	}

	// Units pinned per the metric table.
	if u := got["hfdl.download.speed"].Unit; u != "By/s" {
		t.Fatalf("hfdl.download.speed unit = %q, want By/s", u)
	}
	if u := got["hfdl.disk.duty.level"].Unit; u != "%" {
		t.Fatalf("hfdl.disk.duty.level unit = %q, want %%", u)
	}
}

// TestCardinalityLint is the registration-time guard for the hard
// rule: no metric attribute may ever carry a file path/id, block id, or
// repo name. It reflects over the instrument table that gauge registration
// and the counter constructors are driven from.
func TestCardinalityLint(t *testing.T) {
	allowed := map[string]bool{}
	for _, k := range allowedAttrKeys {
		allowed[k] = true
	}
	// Explicit denylist, belt and braces: these substrings mark
	// high-cardinality identity attributes.
	forbiddenSubstrings := []string{"file", "path", "block", "repo"}

	seen := map[string]bool{}
	for _, spec := range instrumentSpecs {
		if !strings.HasPrefix(spec.name, "hfdl.") {
			t.Errorf("instrument %q lacks hfdl. prefix", spec.name)
		}
		if spec.unit == "" {
			t.Errorf("instrument %q has no unit", spec.name)
		}
		if seen[spec.name] {
			t.Errorf("instrument %q registered twice", spec.name)
		}
		seen[spec.name] = true

		for _, a := range spec.attrs {
			if !allowed[a] {
				t.Errorf("instrument %q: attribute %q not in allowed set %v", spec.name, a, allowedAttrKeys)
			}
			low := strings.ToLower(a)
			for _, bad := range forbiddenSubstrings {
				if strings.Contains(low, bad) {
					t.Errorf("instrument %q: attribute %q contains forbidden identity substring %q", spec.name, a, bad)
				}
			}
		}
	}

	// Every gauge the callback observes must exist in the table (the
	// callback construction uses specByName against the same table).
	for _, name := range []string{
		"hfdl.download.speed", "hfdl.upstream.speed", "hfdl.api.rate",
		"hfdl.bandwidth.utilization", "hfdl.throttle.waiters",
		"hfdl.queue.depth", "hfdl.queue.in_flight", "hfdl.connections",
		"hfdl.disk.duty.level", "hfdl.disk.duty.active_ratio",
		"hfdl.cooldown.remaining",
	} {
		if !seen[name] {
			t.Errorf("gauge %q observed but missing from instrumentSpecs", name)
		}
	}

	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	fset := token.NewFileSet()
	for _, tree := range []string{"pkg", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, tree), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
				return walkErr
			}
			fileAST, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(fileAST, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				pkg, qualified := sel.X.(*ast.Ident)
				if sel.Sel.Name != "WithAttributes" || !qualified || pkg.Name != "metric" {
					return true
				}
				ast.Inspect(call, func(n ast.Node) bool {
					attrCall, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					attrSel, ok := attrCall.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					id, isAttribute := attrSel.X.(*ast.Ident)
					if !isAttribute || id.Name != "attribute" || len(attrCall.Args) == 0 {
						return true
					}
					lit, literal := attrCall.Args[0].(*ast.BasicLit)
					if literal {
						key := strings.Trim(lit.Value, "\"")
						if !allowed[key] {
							t.Errorf("%s: metric attribute %q is not allowlisted", path, key)
						}
					}
					return true
				})
				return false
			})
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", tree, err)
		}
	}
}

// TestCounters verifies the generic counter constructors hand out usable
// instruments on both real and noop meters.
func TestCounters(t *testing.T) {
	real, mp := collect(t, &fakeSource{m: Metrics{}})
	t.Cleanup(func() { _ = mp.Shutdown(t.Context()) })
	_ = real

	p := &Providers{Enabled: true}
	c, err := p.Counter(mp.Meter("hfdl.test"), "hfdl.download.bytes", "By")
	if err != nil {
		t.Fatalf("Counter: %v", err)
	}
	c.Add(t.Context(), 7)

	fc, err := p.FloatCounter(mp.Meter("hfdl.test"), "hfdl.throttle.wait_seconds", "s")
	if err != nil {
		t.Fatalf("FloatCounter: %v", err)
	}
	fc.Add(t.Context(), 0.25)

	// Noop path: disabled providers still hand out working (noop) counters.
	np := Noop()
	nc, err := np.Counter(np.Meter("hfdl.test"), "hfdl.files.completed", "{file}")
	if err != nil {
		t.Fatalf("noop Counter: %v", err)
	}
	nc.Add(t.Context(), 1) // must not panic
}
