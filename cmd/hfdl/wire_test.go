package main

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/otel"
	"github.com/jamesits/hfdl/pkg/sched"
	"github.com/jamesits/hfdl/pkg/stats"
	"github.com/jamesits/hfdl/pkg/throttle"
	"github.com/jamesits/hfdl/pkg/tui"
)

// fakeManager is the Submit/Run/SetLimits/Snapshot seam double: the download
// command is exercised end to end without a compiled working scheduler.
type fakeManager struct {
	mu       sync.Mutex
	jobs     []sched.Job
	limits   []config.Limits
	snap     *sched.Stats
	dryRun   []sched.DryRunEntry
	runBlock chan struct{}
	runErr   error
}

func newFakeManager() *fakeManager {
	return &fakeManager{snap: &sched.Stats{}, runBlock: make(chan struct{})}
}

func (f *fakeManager) Submit(_ context.Context, j sched.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs = append(f.jobs, j)
	return nil
}

func (f *fakeManager) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.runBlock:
		return f.runErr
	}
}

func (f *fakeManager) SetLimits(l config.Limits) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limits = append(f.limits, l)
}

func (f *fakeManager) Snapshot() *sched.Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap
}

func (f *fakeManager) DryRunReport() []sched.DryRunEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dryRun
}

func (f *fakeManager) lastLimits() config.Limits {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.limits[len(f.limits)-1]
}

// wireTestApp builds a fully wired app against a temp state DB and cache
// with the fake manager installed.
func wireTestApp(t *testing.T, fm *fakeManager, mutate func(*downloadFlags)) (*wireApp, *downloadPlan) {
	t.Helper()
	tmp := t.TempDir()
	f := baseFlags()
	f.cacheDirFlag = filepath.Join(tmp, "cache")
	if mutate != nil {
		mutate(f)
	}
	p, err := buildPlan(f, getenvMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	app, err := wireDownloadWith(ctx, p, getenvMap(nil), func(sched.ManagerConfig) manager { return fm }, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.close(ctx) })
	return app, p
}

func TestWireDownloadConstructsAndSubmits(t *testing.T) {
	fm := newFakeManager()
	app, p := wireTestApp(t, fm, nil)
	if app.manager != fm {
		t.Fatal("factory seam not honored")
	}
	if app.prov == nil || app.handler == nil || app.store == nil {
		t.Fatal("wiring incomplete")
	}
	if app.bandwidth == nil || app.api == nil || app.duty == nil || app.reg == nil {
		t.Fatal("metrics handles not stashed")
	}

	if err := app.submit(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	if len(fm.jobs) != 1 {
		t.Fatalf("jobs = %d", len(fm.jobs))
	}
	j := fm.jobs[0]
	if j.RepoType != hfapi.RepoTypeModel || j.Repo != "org/repo" || j.Revision != "main" {
		t.Fatalf("job ref = %+v", j)
	}
	if j.DestMode != destModeCache || j.DestDir != p.cacheDir {
		t.Fatalf("job dest = %q %q, want cache %q", j.DestMode, j.DestDir, p.cacheDir)
	}
	if j.Limits != p.limits {
		t.Fatalf("job limits = %+v, want %+v", j.Limits, p.limits)
	}
}

func TestWireDownloadLocalDirJob(t *testing.T) {
	fm := newFakeManager()
	app, p := wireTestApp(t, fm, func(f *downloadFlags) {
		f.localDir = filepath.Join(t.TempDir(), "out")
		f.cacheDirFlag = ""
	})
	if err := app.submit(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	j := fm.jobs[0]
	if j.DestMode != destModeLocalDir || j.DestDir != p.cli.LocalDir {
		t.Fatalf("job dest = %q %q", j.DestMode, j.DestDir)
	}
}

func TestPauseToggleStashAndRestore(t *testing.T) {
	fm := newFakeManager()
	fm.snap = &sched.Stats{Limits: config.Limits{MaxBandwidthBps: 1 << 20, DiskActivePct: 80}}
	app, _ := wireTestApp(t, fm, nil)

	app.onPauseToggle(true)
	paused := fm.lastLimits()
	if paused.MaxBandwidthBps != 0 || paused.DiskActivePct != 0 {
		t.Fatalf("paused limits = %+v", paused)
	}
	if app.stashed == nil || app.stashed.MaxBandwidthBps != 1<<20 || app.stashed.DiskActivePct != 80 {
		t.Fatalf("stashed = %+v", app.stashed)
	}

	app.onPauseToggle(false)
	resumed := fm.lastLimits()
	if resumed.MaxBandwidthBps != 1<<20 || resumed.DiskActivePct != 80 {
		t.Fatalf("resumed limits = %+v", resumed)
	}
	if app.stashed != nil {
		t.Fatal("stash not cleared on resume")
	}
}

func TestBandwidthDelta(t *testing.T) {
	fm := newFakeManager()
	app, _ := wireTestApp(t, fm, nil)

	t.Run("unlimited starts at 100MiB/s scaled", func(t *testing.T) {
		fm.snap = &sched.Stats{Limits: config.Limits{MaxBandwidthBps: 0}}
		app.onBandwidthDelta(0.9)
		got := fm.lastLimits().MaxBandwidthBps
		want := int64(float64(bandwidthStepStart) * 0.9)
		if got != want {
			t.Fatalf("got %d, want %d", got, want)
		}
	})
	t.Run("scales live limit", func(t *testing.T) {
		fm.snap = &sched.Stats{Limits: config.Limits{MaxBandwidthBps: 10 << 20}}
		app.onBandwidthDelta(1.1)
		got := fm.lastLimits().MaxBandwidthBps
		want := int64(float64(10<<20) * 1.1)
		if got != want {
			t.Fatalf("got %d, want %d", got, want)
		}
	})
	t.Run("floors at 1KiB/s", func(t *testing.T) {
		fm.snap = &sched.Stats{Limits: config.Limits{MaxBandwidthBps: 100}}
		app.onBandwidthDelta(0.9)
		if got := fm.lastLimits().MaxBandwidthBps; got != bandwidthStepFloor {
			t.Fatalf("got %d, want floor %d", got, bandwidthStepFloor)
		}
	})
}

func TestAdaptStatsFieldPreservation(t *testing.T) {
	in := &sched.Stats{
		Running: true, Paused: true, ENOSPCPaused: true,
		Repo: "org/repo", Revision: "main", CommitSHA: "abc123", RepoStatus: "downloading",
		BytesDone: 100, BytesTotal: 200,
		FilesDone: 1, FilesTotal: 3, PendingCount: 2,
		PendingNext: []string{"a.bin", "b.bin"},
		GlobalRate:  1024.5,
		ETA:         42 * time.Second,
		Queues: [4]sched.QueueStat{
			{Depth: 1, InFlight: 1, Detail: "meta"},
			{Depth: 2, InFlight: 2, Detail: "download"},
			{Depth: 3, InFlight: 3, Detail: "disk"},
			{Depth: 4, InFlight: 4, Detail: "install"},
		},
		Active: []sched.FileProgress{{
			FileID: 7, Path: "model.safetensors", Done: 50, Total: 100,
			Conns: 4, Rate: 512.25, Upstreams: []string{"https://huggingface.co"}, Status: "downloading",
		}},
		Limits:          config.Limits{MaxBandwidthBps: 4096, DiskActivePct: 70},
		BandwidthRate:   2048.75,
		APIRate:         3.5,
		DutyLevel:       70,
		DutyActiveRatio: 0.3,
		DutyMedia:       "ssd",
		Cooldowns:       []sched.CooldownInfo{{Target: "https://huggingface.co", Kind: "api", Remaining: 5 * time.Second}},
		Stalls:          9,
		Retries:         8,
		SalvagedBytes:   77,
	}
	want := &tui.Snapshot{
		Running: true, Paused: true, ENOSPCPaused: true,
		Repo: "org/repo", Revision: "main", CommitSHA: "abc123", RepoStatus: "downloading",
		BytesDone: 100, BytesTotal: 200,
		FilesDone: 1, FilesTotal: 3, PendingCount: 2,
		PendingNext: []string{"a.bin", "b.bin"},
		GlobalRate:  1024.5,
		ETA:         42 * time.Second,
		Queues: [4]tui.QueueStat{
			{Depth: 1, InFlight: 1, Detail: "meta"},
			{Depth: 2, InFlight: 2, Detail: "download"},
			{Depth: 3, InFlight: 3, Detail: "disk"},
			{Depth: 4, InFlight: 4, Detail: "install"},
		},
		Active: []tui.FileProgress{{
			FileID: 7, Path: "model.safetensors", Done: 50, Total: 100,
			Conns: 4, Rate: 512.25, Upstreams: []string{"https://huggingface.co"}, Status: "downloading",
		}},
		Limits:          config.Limits{MaxBandwidthBps: 4096, DiskActivePct: 70},
		BandwidthRate:   2048.75,
		APIRate:         3.5,
		DutyLevel:       70,
		DutyActiveRatio: 0.3,
		DutyMedia:       "ssd",
		Cooldowns:       []tui.CooldownInfo{{Target: "https://huggingface.co", Kind: "api", Remaining: 5 * time.Second}},
		Stalls:          9,
		Retries:         8,
		SalvagedBytes:   77,
	}
	if got := adaptStats(in); !reflect.DeepEqual(got, want) {
		t.Fatalf("adaptStats mismatch:\n got %+v\nwant %+v", got, want)
	}
	if got := adaptStats(nil); got == nil {
		t.Fatal("nil snapshot must adapt to a non-nil zero snapshot")
	}
}

func TestAdaptMetricsMapsEveryField(t *testing.T) {
	snap := &sched.Stats{
		GlobalRate:      1000,
		APIRate:         4.5,
		Queues:          [4]sched.QueueStat{{Depth: 1, InFlight: 1}, {Depth: 2}, {InFlight: 3}, {Depth: 4, InFlight: 4}},
		DutyLevel:       80,
		DutyActiveRatio: 0.2,
		DutyMedia:       "hdd",
		Cooldowns:       []sched.CooldownInfo{{Target: "ep", Kind: "cas", Remaining: 1500 * time.Millisecond}},
	}
	bandwidth := throttle.NewBucket(0, 0)
	api := throttle.NewBucket(5, 10)
	duty := throttle.NewDutyLimiter(80, throttle.MediaHDD)
	reg := stats.New()
	reg.AddUpstream("https://huggingface.co", 1234)
	reg.ReportUpstreamRate("https://huggingface.co", 999)
	reg.ConnStart(1, "https://huggingface.co")

	m := adaptMetrics(snap, bandwidth, api, duty, reg)

	if m.DownloadSpeed != 1000 || m.APIRate != 4.5 {
		t.Fatalf("rates = %v/%v", m.DownloadSpeed, m.APIRate)
	}
	wantDepth := map[string]int64{"meta": 1, "download": 2, "disk": 0, "install": 4}
	wantFlight := map[string]int64{"meta": 1, "download": 0, "disk": 3, "install": 4}
	if !reflect.DeepEqual(m.QueueDepth, wantDepth) || !reflect.DeepEqual(m.QueueInFlight, wantFlight) {
		t.Fatalf("queues = %v/%v", m.QueueDepth, m.QueueInFlight)
	}
	if m.DiskDutyLevel["hdd"] != 80 {
		t.Fatalf("duty level = %v", m.DiskDutyLevel)
	}
	if _, ok := m.DiskDutyActiveRatio["hdd"]; !ok {
		t.Fatalf("duty ratio = %v", m.DiskDutyActiveRatio)
	}
	if len(m.Cooldowns) != 1 || m.Cooldowns[0].Target != "ep" || m.Cooldowns[0].Kind != "cas" || m.Cooldowns[0].Seconds != 1.5 {
		t.Fatalf("cooldowns = %+v", m.Cooldowns)
	}
	if m.ThrottleWaiters == nil {
		t.Fatal("throttle waiters not mapped")
	}
	if _, ok := m.ThrottleWaiters["api"]; !ok {
		t.Fatal("api waiters missing")
	}
	if _, ok := m.ThrottleWaiters["bandwidth"]; !ok {
		t.Fatal("bandwidth waiters missing")
	}
	// ReportUpstreamRate is an EMA (α=0.2) seeded at 0: one report of 999
	// yields 999*0.2.
	if got := m.UpstreamSpeed["https://huggingface.co"]; got != 999*0.2 {
		t.Fatalf("upstream speed = %v", m.UpstreamSpeed)
	}
	if m.UpstreamConnections["https://huggingface.co"] != 1 || m.Connections != 1 {
		t.Fatalf("connections = %v/%v", m.UpstreamConnections, m.Connections)
	}

	// Every field of otel.Metrics must be covered by the adapter: with the
	// populated inputs above, no field may remain its zero value.
	v := reflect.ValueOf(m)
	for i := range v.NumField() {
		name := v.Type().Field(i).Name
		switch name {
		case "BandwidthUtilization":
			// Unlimited bucket: Utilization is defined as 0 by contract.
			continue
		}
		if v.Field(i).IsZero() {
			t.Errorf("otel.Metrics.%s left at zero value", name)
		}
	}
}

func TestMetricsSourceCollectNilSafe(t *testing.T) {
	if got := (&metricsSource{}).Collect(); !reflect.DeepEqual(got, otel.Metrics{}) {
		t.Fatalf("empty source = %+v", got)
	}
	// A wired app with no manager yet still collects (zero gauges).
	_ = (&metricsSource{app: &wireApp{}}).Collect()
}

func TestHelpers(t *testing.T) {
	if got := bandwidthBurst(0); got != 0 {
		t.Fatalf("unlimited burst = %d", got)
	}
	if got := bandwidthBurst(4 << 20); got != bandwidthMinBurst {
		t.Fatalf("small limit burst = %d, want floor %d", got, bandwidthMinBurst)
	}
	if got := bandwidthBurst(64 << 20); got != 32<<20 {
		t.Fatalf("burst = %d, want limit/2", got)
	}
	if clamp(5, 10, 20) != 10 || clamp(25, 10, 20) != 20 || clamp(15, 10, 20) != 15 {
		t.Fatal("clamp broken")
	}
	if ioTier(config.IOAuto) != fcio.TierAuto || ioTier(config.IODirect) != fcio.TierDirect || ioTier(config.IOSequential) != fcio.TierPlain {
		t.Fatal("ioTier mapping broken")
	}
	if mediaClass(fcio.FsSSD) != throttle.MediaSSD || mediaClass(fcio.FsHDD) != throttle.MediaHDD ||
		mediaClass(fcio.FsNetFS) != throttle.MediaNetFS || mediaClass(fcio.FsUnknown) != throttle.MediaUnknown {
		t.Fatal("mediaClass mapping broken")
	}
}

// keep otel imported for the nil-safe test's type reference
var _ otel.MetricsSource = (*metricsSource)(nil)
