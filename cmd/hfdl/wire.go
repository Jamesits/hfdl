package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/jamesits/hfdl/pkg/cache"
	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/logging"
	"github.com/jamesits/hfdl/pkg/netcfg"
	"github.com/jamesits/hfdl/pkg/otel"
	"github.com/jamesits/hfdl/pkg/sched"
	"github.com/jamesits/hfdl/pkg/stats"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/throttle"
	"github.com/jamesits/hfdl/pkg/transfer"
	"github.com/jamesits/hfdl/pkg/tui"
	"github.com/jamesits/hfdl/pkg/verify"
	"github.com/jamesits/hfdl/pkg/xet"
	"github.com/mattn/go-isatty"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	// ringCapacity bounds the in-memory log ring: the last 2k records.
	ringCapacity = 2048
	// logRetainCount is the startup logs-table retention: prune to the
	// newest 50k rows.
	logRetainCount = 50_000
	// otlpHTTPTimeout mirrors the OTLP exporter default request timeout,
	// which WithHTTPClient bypasses (the injected client's Timeout applies
	// instead).
	otlpHTTPTimeout = 10 * time.Second
	// fcioPoolSlab is the fcio buffer pool slab size: 8MiB, FastCopy
	// mainBuf-style.
	fcioPoolSlab = 8 << 20
	// fcioPoolMinCap/MaxCap clamp the --io-buffer auto pool: 64MiB..1GiB. The
	// upper bound is further capped at half of physical RAM (see autoIOBuffer).
	fcioPoolMinCap = 64 << 20
	fcioPoolMaxCap = 1 << 30
	// progressInterval is the non-TTY slog progress cadence.
	progressInterval = 30 * time.Second
	// bandwidthStepStart is where +/- starts from when bandwidth is
	// unlimited: infinity cannot be scaled, so the first keypress pins a
	// finite ceiling of 100MiB/s * factor (the scaling the TUI +/- keys
	// expose).
	bandwidthStepStart = 100 << 20
	// bandwidthStepFloor keeps a scaled limit usable (never rounds to 0).
	bandwidthStepFloor = 1 << 10
	// otelShutdownBudget bounds the tracer→meter→logger shutdown flush: 5s
	// per provider.
	otelShutdownBudget = 5 * time.Second
)

// manager is the seam between cmd and pkg/sched: tests substitute a fake so
// the download command is exercisable without a compiled scheduler.
// *sched.Manager satisfies it structurally (contract local://hfdl-contracts.md).
type manager interface {
	Submit(ctx context.Context, j sched.Job) error
	Run(ctx context.Context) error
	SetLimits(l config.Limits)
	Snapshot() *sched.Stats
	DryRunReport() []sched.DryRunEntry
}

// managerFactory builds the manager; wireComponents uses newManager, tests
// override it.
type managerFactory func(cfg sched.ManagerConfig) manager

func newManager(cfg sched.ManagerConfig) manager { return sched.NewManager(cfg) }

// schedSnapshot mirrors the fields of sched.Stats cmd reads. It exists so
// finalPath can take either a real snapshot or a hand-built test value
// without re-declaring the shape.
type schedSnapshot = sched.Stats

// wireApp holds everything constructed while wiring the process, in teardown
// order.
type wireApp struct {
	log     *slog.Logger
	handler *logging.Handler
	ring    *logging.Ring
	store   *store.Store
	prov    *otel.Providers
	pool    *fcio.Pool
	manager manager
	stashed *config.Limits // pre-pause limits for the TUI pause toggle

	// proxyFn is resolved once in wireTelemetry (system mode may query the
	// platform proxy store) and shared by every transport built afterwards.
	proxyFn netcfg.ProxyFn

	// Kept for the otel metrics adapter: gauges read bucket/duty/stats
	// state that sched's snapshot does not duplicate (utilization, waiters,
	// per-upstream speeds).
	bandwidth *throttle.Bucket
	api       *throttle.Bucket
	duty      *throttle.DutyLimiter
	reg       *stats.Registry
}

// close tears the app down in reverse bootstrap order: manager's ctx is
// already drained by the caller; here we flush logs and OTel (Shutdown
// after Run drains, 5s budget per provider). The flush ctx is detached from the
// command ctx so a SIGINT-cancelled run still gets its full flush budget.
func (a *wireApp) close(ctx context.Context) {
	flush := context.WithoutCancel(ctx)
	if a.handler != nil {
		// Flush pending DB batches before OTel's log exporter shuts down.
		hctx, hcancel := context.WithTimeout(flush, otelShutdownBudget)
		err := a.handler.Close(hctx)
		hcancel()
		// The handler is now closed, so log the failure through the still-live
		// text/ring legs (best-effort; the DB leg it guards is already gone).
		if err != nil {
			a.log.LogAttrs(flush, slog.LevelWarn, "log handler shutdown failed", slog.Any("err", err))
		}
	}
	if a.prov != nil {
		sctx, scancel := context.WithTimeout(flush, otelShutdownBudget)
		if err := a.prov.Shutdown(sctx); err != nil {
			a.log.LogAttrs(flush, slog.LevelWarn, "telemetry shutdown failed", slog.Any("err", err))
		}
		scancel()
	}
	if a.store != nil {
		sctx, scancel := context.WithTimeout(flush, otelShutdownBudget)
		if err := a.store.Close(sctx); err != nil {
			a.log.LogAttrs(flush, slog.LevelWarn, "state store close failed", slog.Any("err", err))
		}
		scancel()
	}
	// Unmap the IO buffer arena last: the manager is drained by now, so no
	// worker holds pool buffers, and nothing after this touches file IO.
	if a.pool != nil {
		if err := a.pool.Close(); err != nil {
			a.log.LogAttrs(flush, slog.LevelWarn, "buffer pool close failed", slog.Any("err", err))
		}
	}
}

// wireTelemetry does the untraceable bootstrap: the root slog fan-out and the
// OTel providers. It is deliberately the minimum needed to obtain a live
// tracer, because this is where the tracer itself is built — no span can wrap
// it. Everything downstream is built by wireComponents, which runDownload runs
// under the hfdl.run span so the whole invocation is traced end to end.
//
// stderrText=false silences the stderr leg entirely for TUI mode — a stray
// write would corrupt the bubbletea screen; ring, DB and OTLP sinks are
// unaffected.
func wireTelemetry(ctx context.Context, p *downloadPlan, getenv func(string) string, stderrText bool) (*wireApp, error) {
	// Root slog fan-out: stderr text (nil in TUI mode) + ring.
	var stderr *os.File
	if stderrText {
		stderr = os.Stderr
	}
	ring := logging.NewRing(ringCapacity)
	handler := logging.NewHandler(p.logLevel, stderr, ring)
	log := slog.New(handler)

	app := &wireApp{log: log, handler: handler, ring: ring}
	app.proxyFn = p.proxy.ProxyFunc(ctx, log)

	// Telemetry (env-only; disabled → noop providers). Built before the store
	// so the hfdl.run span opened next can wrap the store/cache/probe wiring.
	prov := otel.Noop()
	if otel.Enabled(getenv) {
		// Telemetry sockets get the telemetry QoS class and the same proxy
		// resolution as the rest of hfdl. The client timeout mirrors the OTLP
		// default (10s), since WithHTTPClient bypasses the exporter's own
		// timeout handling.
		egress := &otel.Egress{
			HTTPClient: &http.Client{
				Transport: netcfg.Transport(app.proxyFn, p.qos.Telemetry, log),
				Timeout:   otlpHTTPTimeout,
			},
			GRPCDialer: netcfg.GRPCDialer(p.qos.Telemetry, log),
		}
		setup, err := otel.Setup(ctx, getenv, config.VersionString(), &metricsSource{app: app}, log, egress)
		if err != nil {
			_ = handler.Close(ctx)
			return nil, fmt.Errorf("otel setup: %w", err)
		}
		prov = setup
	}
	app.prov = prov
	// The OTLP log bridge is attached ahead of the DB sink (wireComponents):
	// they are independent fan-out sinks, each with its own replay-dedup
	// horizon, so ordering between them is free.
	handler.AttachSlog(prov.SlogBridge())
	return app, nil
}

// wireComponents builds everything downstream of the tracer: the state store,
// the DB log sink, the shared HTTP client and Hub clients, the IO engine, the
// blob cache/verifier/downloader, the xet client and the queue manager. When
// runDownload calls it under the hfdl.run span, the store migrations, cache
// open and filesystem probe all nest under that span. On failure it returns
// the error without teardown — the caller owns app.close.
func (app *wireApp) wireComponents(ctx context.Context, p *downloadPlan, getenv func(string) string, mk managerFactory) error {
	log, prov, handler := app.log, app.prov, app.handler

	// 1. State store (exclusive process lock, migrations, WAL).
	st, err := store.Open(ctx, p.cli.StateDB)
	if err != nil {
		return err
	}
	app.store = st
	// Prune retained logs BEFORE attaching the sink: AttachDB kicks off the
	// async ring replay that inserts fresh rows, and pruning concurrently
	// would race it (deleting rows replay just wrote, or vice versa). Prune
	// old rows from prior runs first, then start the sink.
	if err := st.PruneLogs(ctx, logRetainCount); err != nil {
		log.LogAttrs(ctx, slog.LevelWarn, "log retention prune failed", slog.Any("err", err))
	}
	handler.AttachDB(ctx, st)

	// 2. HTTP clients: tuned transports, otelhttp on top, no global timeout
	// (response timeouts live on the hfapi client). API and download traffic
	// get separate connection pools so their IP QoS classes never share a
	// socket; both reuse the proxy resolved in wireTelemetry.
	apiHC := &http.Client{Transport: prov.HTTPTransport(netcfg.Transport(app.proxyFn, p.qos.API, log))}
	dlHC := &http.Client{Transport: prov.HTTPTransport(netcfg.Transport(app.proxyFn, p.qos.Download, log))}

	// 3. Hub API clients, one per endpoint (first = primary; the rest are
	// payload mirrors — CAS tokens always come from the primary Hub endpoint).
	etagTimeout := config.ETagTimeout(getenv)
	downloadTimeout := config.DownloadTimeout(getenv)
	// hfdl.api.requests: one shared counter, each client labels it with its
	// own endpoint. Noop when telemetry is disabled.
	apiRequests, err := prov.Counter(prov.Meter("hfdl.hfapi"), "hfdl.api.requests", "{request}")
	if err != nil {
		log.Warn("telemetry counter registration failed", "instrument", "hfdl.api.requests", "err", err)
	}
	clients := make(map[string]*hfapi.Client, len(p.endpoints))
	for _, ep := range p.endpoints {
		c := hfapi.NewClient(log, apiHC, ep, p.token, etagTimeout)
		c.SetTracer(prov.Tracer("hfdl.hfapi"))
		c.SetRequestCounter(apiRequests)
		clients[ep] = c
	}
	primary := clients[p.endpoints[0]]

	// 4. IO engine, buffer pool, volume exclusion set.
	// HFDL_TRACE_FCIO_DETAIL gates the fine-grained fcio.read/fcio.fsync spans
	// (process-global; noop unless telemetry is also enabled).
	fcio.SetTraceDetail(config.TraceFCIODetail(getenv))
	engine := fcio.NewEngine(log, st, ioTier(p.limits.IOMode))
	poolCap := p.limits.IOBuffer
	if poolCap == 0 {
		// The physical-RAM probe is best-effort: on failure autoIOBuffer keeps
		// its fixed ceiling. An explicit --hfdl-io-buffer bypasses both.
		totalRAM, ramErr := fcio.TotalRAM()
		if ramErr != nil {
			log.LogAttrs(ctx, slog.LevelDebug,
				"physical RAM probe failed; auto io-buffer keeps its fixed ceiling",
				slog.Any("err", ramErr))
		}
		poolCap = autoIOBuffer(p.limits.MaxWorkers, totalRAM)
	}
	// Pass the process logger so an mmap→heap arena fallback is visible in
	// production logs, not just under tests.
	pool := fcio.NewPool(fcioPoolSlab, poolCap, log)
	app.pool = pool
	// Back the engine's scratch reads with the same pool so ReadAll obeys the
	// --io-buffer cap and back-pressures instead of self-allocating mappings.
	engine.SetPool(pool)
	volumes := fcio.NewVolumeSet()

	// 5. Blob cache + verifier + downloader. The blob store, its in-flight
	// staging and the state DB all live under p.workRoot (the HF cache in cache
	// mode, or <localDir>/.cache/huggingface in --local-dir mode), so local-dir
	// downloads are self-contained and concurrency-safe. The xet chunk cache
	// below stays on the shared HF cache (p.cacheDir): it is content-addressed
	// and meant to be shared, mirroring HF_XET_CACHE.
	blobs, err := cache.OpenStore(ctx, p.workRoot, engine, log)
	if err != nil {
		return err
	}
	bwBurst := config.BandwidthBurst(p.limits.MaxBandwidthBps)
	// Bandwidth starts full so the first chunk transfers without an artificial
	// stall. The API bucket starts empty: a cold start must not fire a burst of
	// Hub requests before the rate limit engages — pace from the first call.
	bandwidth := throttle.NewBucket(p.limits.MaxBandwidthBps, bwBurst, bwBurst)
	// Pace the download client at the transport so every consumer — block
	// workers, the single-stream fallback and xet CAS fetches — shares one
	// wire-level budget; xet cache hits never cross the transport and are
	// never charged. The API client stays unpaced (metadata is IOPS-limited,
	// not bandwidth-limited). Wrapping in place is safe here: no request has
	// used dlHC yet.
	dlHC.Transport = throttle.PacedTransport(dlHC.Transport, bandwidth)
	api := throttle.NewBucket(p.limits.APIIOPS, p.limits.APIBurst, 1)
	// hfdl.throttle.wait_seconds: one shared counter, each bucket labels it
	// with its own name. Noop when telemetry is disabled.
	waitSeconds, err := prov.FloatCounter(prov.Meter("hfdl.throttle"), "hfdl.throttle.wait_seconds", "s")
	if err != nil {
		log.Warn("telemetry counter registration failed", "instrument", "hfdl.throttle.wait_seconds", "err", err)
	}
	bandwidth.SetWaitCounter(waitSeconds, "bandwidth")
	api.SetWaitCounter(waitSeconds, "api")
	duty := throttle.NewDutyLimiter(p.limits.DiskActivePct, throttle.MediaUnknown)
	// Probe the blob store's filesystem once: the duty derate depends on media
	// class, and blobs are written under p.workRoot (which differs from the HF
	// cache in --local-dir mode).
	if fsType, err := fcio.ProbeFs(ctx, p.workRoot); err != nil {
		log.LogAttrs(ctx, slog.LevelWarn, "filesystem probe failed, duty limiter assumes unknown media",
			slog.String("path", p.workRoot), slog.Any("err", err))
	} else {
		duty.SetMedia(mediaClass(fsType))
	}
	checker := verify.NewChecker(engine, pool, duty, log, prov)

	app.bandwidth, app.api, app.duty = bandwidth, api, duty

	reg := stats.New()
	app.reg = reg
	dl := transfer.NewDownloader(transfer.Config{
		Log:                log,
		HTTP:               dlHC,
		Bandwidth:          bandwidth,
		Stats:              reg,
		Engine:             engine,
		Pool:               pool,
		CheckpointInterval: p.limits.CheckpointInterval,
		HeaderTimeout:      downloadTimeout,
		Prov:               prov,
	})

	// 6. Xet client. CAS tokens are per Hub endpoint; endpoints beyond the
	// first are payload mirrors, so the TokenSource closes over the primary.
	xcfg := xet.Config{CacheMaxBytes: xet.CacheMaxBytesFromEnv(getenv)}
	if v := getenv("HF_XET_ENDPOINT"); v != "" {
		xcfg.CasURL = v
	}
	if v := getenv("HF_XET_CACHE"); v != "" {
		xcfg.CacheDir = v
	} else {
		xcfg.CacheDir = filepath.Join(p.cacheDir, "xet")
	}
	// Xet CAS traffic is payload: download class.
	xc := xet.NewClient(log, dlHC, xcfg, primary.XetToken, prov)

	installer := cache.NewInstaller(blobs, engine, volumes, duty, log, prov)

	// 7. Queue manager.
	mgr := mk(sched.ManagerConfig{
		Store:         st,
		Clients:       clients,
		Downloader:    dl,
		Verifier:      checker,
		Installer:     installer,
		Cache:         blobs,
		Engine:        engine,
		Pool:          pool,
		Volumes:       volumes,
		Bandwidth:     bandwidth,
		API:           api,
		Duty:          duty,
		Stats:         reg,
		Xet:           xc,
		Log:           log,
		Prov:          prov,
		Limits:        p.limits,
		CacheDir:      p.cacheDir,
		Offline:       config.Offline(getenv),
		ForceDownload: p.cli.ForceDownload,
		DryRun:        p.cli.DryRun,
	})
	app.manager = mgr
	return nil
}

// wireDownloadWith composes the two wiring phases for callers that do not
// interpose the hfdl.run span (tests). runDownload calls the phases directly
// so the span wraps wireComponents; here they run back to back and a
// component failure tears down the telemetry already built.
func wireDownloadWith(ctx context.Context, p *downloadPlan, getenv func(string) string, mk managerFactory, stderrText bool) (*wireApp, error) {
	app, err := wireTelemetry(ctx, p, getenv, stderrText)
	if err != nil {
		return nil, err
	}
	if err := app.wireComponents(ctx, p, getenv, mk); err != nil {
		app.close(ctx)
		return nil, err
	}
	return app, nil
}

func (a *wireApp) submit(ctx context.Context, p *downloadPlan) error {
	destMode := destModeCache
	destDir := p.cacheDir
	if p.cli.LocalDir != "" {
		destMode = destModeLocalDir
		destDir = p.cli.LocalDir
	}
	return a.manager.Submit(ctx, sched.Job{
		RepoType:   p.ref.RepoType,
		Repo:       p.ref.Repo,
		Revision:   p.ref.Revision,
		Filenames:  p.cli.Filenames,
		Include:    p.cli.Include,
		Exclude:    p.cli.Exclude,
		DestMode:   destMode,
		DestDir:    destDir,
		References: p.cli.References,
		Limits:     p.limits,
	})
}

func (a *wireApp) runManager(ctx context.Context) error { return a.manager.Run(ctx) }

// startRunSpan opens the hfdl.run root span on the injected cmd tracer. It is
// opened before component wiring (wireComponents) so that store migrations,
// cache open and the filesystem probe — and every downstream sched.job /
// transfer.file span — nest under it through ctx, giving one trace for the
// whole invocation. Attributes are the coarse invocation shape only (repo,
// revision, dest_mode, limits) — no per-file detail. When telemetry is
// disabled the tracer is the noop and this is allocation-cheap. Returns the
// span-carrying ctx and an end func the caller defers with the run's terminal
// error (nil on success).
func startRunSpan(ctx context.Context, prov *otel.Providers, p *downloadPlan) (context.Context, func(error)) {
	destMode := destModeCache
	if p.cli.LocalDir != "" {
		destMode = destModeLocalDir
	}
	ctx, span := prov.Tracer("hfdl.cmd").Start(ctx, "hfdl.run", trace.WithAttributes(
		attribute.String("repo", p.ref.Repo),
		attribute.String("revision", p.ref.Revision),
		attribute.String("dest_mode", destMode),
		attribute.Int("limits.max_workers", p.limits.MaxWorkers),
		attribute.Int64("limits.max_bandwidth_bps", p.limits.MaxBandwidthBps),
	))
	// Record the invocation's terminal error on the root span so a failed or
	// interrupted run surfaces as an errored hfdl.run span (covering wiring,
	// submit and the manager run alike), then end it. err is the caller's
	// final return value, read at defer time.
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// runTUI drives the dashboard; the manager runs concurrently on the main
// flow's goroutine (caller arranged it). Callbacks are the only side channel
// back into the scheduler.
func (a *wireApp) runTUI(ctx context.Context) error {
	return tui.Run(ctx, func() *tui.Snapshot { return adaptStats(a.manager.Snapshot()) },
		a.ring, config.VersionString(), tui.Callbacks{
			OnPauseToggle:    a.onPauseToggle,
			OnBandwidthDelta: a.onBandwidthDelta,
		})
}

// onPauseToggle implements the TUI `p` key: stashing the live limits and
// zeroing
// bandwidth + disk duty pauses all worker IO; restoring resumes.
func (a *wireApp) onPauseToggle(paused bool) {
	snap := a.manager.Snapshot()
	if snap == nil {
		return
	}
	if paused {
		l := snap.Limits
		stashed := l // stash before zeroing; &l would alias the paused values
		a.stashed = &stashed
		l.MaxBandwidthBps = 0
		l.DiskActivePct = 0
		a.manager.SetLimits(l)
		return
	}
	if a.stashed != nil {
		a.manager.SetLimits(*a.stashed)
		a.stashed = nil
	}
}

// onBandwidthDelta implements the TUI +/- keys: scale the live bandwidth
// ceiling
// by factor. An unlimited ceiling cannot be scaled — the first keypress pins
// it to 100MiB/s * factor (bandwidthStepStart).
func (a *wireApp) onBandwidthDelta(factor float64) {
	snap := a.manager.Snapshot()
	if snap == nil {
		return
	}
	l := snap.Limits
	if l.MaxBandwidthBps <= 0 {
		l.MaxBandwidthBps = int64(float64(bandwidthStepStart) * factor)
	} else {
		l.MaxBandwidthBps = int64(float64(l.MaxBandwidthBps) * factor)
	}
	if l.MaxBandwidthBps < bandwidthStepFloor {
		l.MaxBandwidthBps = bandwidthStepFloor
	}
	a.manager.SetLimits(l)
}

// progressLoop emits a one-line summary at Info every progressInterval until
// the manager drains (non-TTY, non-quiet mode). Stdout stays clean for the
// final path; these go to stderr through the root handler.
func (a *wireApp) progressLoop(ctx context.Context, runErr <-chan error) error {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	for {
		select {
		// No ctx.Done case: on interrupt the manager drains and reports the
		// outcome itself via runErr, which is the error we want to surface.
		case err := <-runErr:
			return err
		case <-ticker.C:
			s := a.manager.Snapshot()
			if s == nil {
				continue
			}
			a.log.LogAttrs(ctx, slog.LevelInfo, "progress",
				slog.String("repo", s.Repo),
				slog.Int64("bytes_done", s.BytesDone),
				slog.Int64("bytes_total", s.BytesTotal),
				slog.Int("files_done", s.FilesDone),
				slog.Int("files_total", s.FilesTotal),
				slog.Float64("bytes_per_sec", s.GlobalRate))
		}
	}
}

// adaptStats copies sched.Stats onto the tui mirror (field-for-field per
// contract; tui never imports sched).
func adaptStats(s *sched.Stats) *tui.Snapshot {
	if s == nil {
		return &tui.Snapshot{}
	}
	out := &tui.Snapshot{
		Running: s.Running, Paused: s.Paused, ENOSPCPaused: s.ENOSPCPaused,
		Repo: s.Repo, Revision: s.Revision, CommitSHA: s.CommitSHA, RepoStatus: s.RepoStatus,
		BytesDone: s.BytesDone, BytesTotal: s.BytesTotal,
		FilesDone: s.FilesDone, FilesTotal: s.FilesTotal,
		PendingCount: s.PendingCount, PendingNext: s.PendingNext,
		GlobalRate: s.GlobalRate, ETA: s.ETA,
		Active:        make([]tui.FileProgress, len(s.Active)),
		Limits:        s.Limits,
		BandwidthRate: s.BandwidthRate, APIRate: s.APIRate,
		DutyLevel: s.DutyLevel, DutyActiveRatio: s.DutyActiveRatio, DutyMedia: s.DutyMedia,
		Cooldowns: make([]tui.CooldownInfo, len(s.Cooldowns)),
		Stalls:    s.Stalls, Retries: s.Retries, SalvagedBytes: s.SalvagedBytes,
	}
	for i, q := range s.Queues {
		out.Queues[i] = tui.QueueStat{Depth: q.Depth, InFlight: q.InFlight, Detail: q.Detail}
	}
	for i, fp := range s.Active {
		out.Active[i] = tui.FileProgress{
			FileID: fp.FileID, Path: fp.Path, Done: fp.Done, Total: fp.Total,
			Conns: fp.Conns, Rate: fp.Rate, Upstreams: fp.Upstreams, Status: fp.Status,
		}
	}
	for i, c := range s.Cooldowns {
		out.Cooldowns[i] = tui.CooldownInfo{Target: c.Target, Kind: c.Kind, Remaining: c.Remaining}
	}
	return out
}

func ioTier(m config.IOMode) fcio.IOTier {
	switch m {
	case config.IODirect:
		return fcio.TierDirect
	case config.IOSequential:
		return fcio.TierPlain
	}
	return fcio.TierAuto
}

func mediaClass(t fcio.FsType) throttle.MediaClass {
	switch t {
	case fcio.FsSSD:
		return throttle.MediaSSD
	case fcio.FsHDD:
		return throttle.MediaHDD
	case fcio.FsNetFS:
		return throttle.MediaNetFS
	}
	return throttle.MediaUnknown
}

// autoIOBuffer sizes the fcio buffer pool when --hfdl-io-buffer is unset. It
// scales with the connection count (each connection keeps a couple of slabs
// in flight) but is clamped to [64MiB, 1GiB] so it neither starves nor
// dominates, then further capped at half of physical RAM: on small hosts a
// 1GiB pool would evict the page cache or risk OOM. totalRAM <= 0 means the
// probe failed — the fixed clamp stands. The half-RAM cap can legitimately
// fall below fcioPoolMinCap on a tiny host; NewPool floors the result to one
// slab, so the pool stays usable.
func autoIOBuffer(conns int, totalRAM int64) int64 {
	poolCap := clamp(fcioPoolSlab*int64(conns)*2, fcioPoolMinCap, fcioPoolMaxCap)
	if half := totalRAM / 2; totalRAM > 0 && half < poolCap {
		poolCap = half
	}
	return poolCap
}

func clamp(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// stdoutIsTTY reports whether stdout is a terminal (TUI gate; tests stub it).
// The check is on stdout, not stderr, because bubbletea renders to stdout —
// gating on stderr would launch the TUI even when stdout is redirected to a
// file/pipe (corrupting the contract's final-path line).
var stdoutIsTTY = defaultStdoutIsTTY

func defaultStdoutIsTTY() bool {
	return isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())
}
