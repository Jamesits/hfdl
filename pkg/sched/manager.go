package sched

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jamesits/hfdl/pkg/cache"
	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/logging"
	"github.com/jamesits/hfdl/pkg/otel"
	"github.com/jamesits/hfdl/pkg/stats"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/throttle"
	"github.com/jamesits/hfdl/pkg/transfer"
	"github.com/jamesits/hfdl/pkg/verify"
	"github.com/jamesits/hfdl/pkg/xet"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// salvageAttr / networkAttr tag the shared hfdl.download.bytes counter by
// origin (kind=network|salvage): network bytes are tallied from block-done
// events, salvaged bytes from the disk queue.
var (
	salvageAttr = metric.WithAttributes(attribute.String("kind", "salvage"))
	networkAttr = metric.WithAttributes(attribute.String("kind", "network"))
)

const (
	// metaWorkers is the pinned meta-queue depth: two listing workers.
	metaWorkers = 2
	// installCheapWorkers is the pinned cheap-install pool of four:
	// symlink/hardlink installs are metadata ops, no duty gate.
	installCheapWorkers = 4
	// installCopyWorkers is the local-dir reflink/copy pool. Copies serialize
	// per (src,dst) volume pair (installer VolumeSet) and pace under the
	// DutyLimiter, so a small pool suffices — its point is to progress copies
	// across distinct volume pairs without blocking the cheap symlink pool.
	installCopyWorkers = 2

	// maxBlockRetries is the give-up bound for block requeues.
	maxBlockRetries = 8

	// heartbeatInterval renews store leases well inside the 30s lease
	// window (store.leaseDuration).
	heartbeatInterval = 10 * time.Second
	// leaseTTL matches the store's lease duration and bounds how long guarded
	// work may continue while renewals cannot reach the database.
	leaseTTL = 30 * time.Second
	// recoverInterval is the periodic Recover cadence: every lease
	// interval. 30s matches the store lease duration.
	recoverInterval = 30 * time.Second
	// queuePollInterval bounds how long a queue worker sleeps without a
	// wake signal (safety net; every producer also wakes explicitly).
	queuePollInterval = 500 * time.Millisecond
	// countsCacheTTL is the queue-depth cache window; depths are
	// SELECT count(*) GROUP BY status over the store tables.
	countsCacheTTL = 250 * time.Millisecond
	// enospcResumeBytes is the free-space watermark that lifts an ENOSPC
	// pause: enough headroom for block fallocates plus checkpoint churn.
	enospcResumeBytes = int64(256) << 20

	// kvLimitsKey persists the operator's limits across restarts.
	kvLimitsKey = "limits"

	otelScope = "hfdl.sched"
)

// OfflineError reports a network operation attempted in offline mode
// (HF_HUB_OFFLINE): cache hits are served, everything else fails fast.
type OfflineError struct {
	Op string // what was needed, e.g. "list repo org/repo"
}

func (e *OfflineError) Error() string {
	return "sched: offline mode: network access disabled: cannot " + e.Op
}

// Job is one download request (repo + filters + destination), mirroring the
// hf CLI invocation. Filenames selects explicit files; Include/Exclude are
// python-fnmatch globs; References are local salvage roots.
type Job struct {
	RepoType                    hfapi.RepoType
	Repo, Revision              string
	Filenames, Include, Exclude []string
	DestMode, DestDir           string // "cache"|"local-dir"; DestDir = local-dir target or HF cache root
	References                  []string
	Limits                      config.Limits
}

// ManagerConfig wires every dependency cmd builds. All fields come from
// upstream; prov may be nil (noop providers).
type ManagerConfig struct {
	Store   *store.Store
	Clients map[string]*hfapi.Client // keyed by endpoint; all are download upstreams, one is the metadata hub

	Downloader *transfer.Downloader
	Verifier   *verify.Checker
	Installer  *cache.Installer
	Cache      *cache.Store
	Engine     *fcio.Engine
	Pool       *fcio.Pool
	Volumes    *fcio.VolumeSet
	Bandwidth  *throttle.Bucket
	API        *throttle.Bucket
	Duty       *throttle.DutyLimiter
	Stats      *stats.Registry
	Xet        *xet.Client

	Log  *slog.Logger
	Prov *otel.Providers

	Limits config.Limits

	CacheDir      string // HF cache root (fallback for Job.DestDir)
	Offline       bool
	ForceDownload bool
	DryRun        bool
}

// Manager is the queue manager. Construct with NewManager; Submit jobs, then
// Run to drain. A single Run at a time (the runMu guard rejects a concurrent
// one). Submit and the hot-update APIs (SetLimits, Snapshot) are safe to call
// concurrently with a running Run — each shared field carries its own guard
// (limitsMu, activeMu, enospcMu, salvageMu, repoMu, …); there is no single
// "everything else is safe" invariant, only these per-field locks.
type Manager struct {
	cfg ManagerConfig
	log *slog.Logger
	st  *store.Store

	tracer trace.Tracer

	// metrics (sync counters; attributes never carry
	// file/block/repo values).
	filesCompleted metric.Int64Counter
	filesFailed    metric.Int64Counter
	salvageBytes   metric.Int64Counter
	stalls         metric.Int64Counter
	blockRetries   metric.Int64Counter

	// metaEndpoint is the hub used for listing; deterministic pick.
	metaEndpoint string

	// limits (hot-settable; see limits.go).
	limitsMu  sync.RWMutex
	limits    config.Limits
	limitsSet bool // an explicit SetLimits (CLI/Submit/TUI) has been applied

	// pause gates (gates.go): operator pause and ENOSPC pause both block
	// the download and install queues.
	pause  *gate
	enospc *gate

	// wake channels, one per queue (buffered 1; send is non-blocking).
	wakeMeta     chan struct{}
	wakeDownload chan struct{}
	wakeDisk     chan struct{}
	wakeInstall  chan struct{}

	// active download set, keyed by file ID (orchestrator-owned map,
	// SetParallelism reads it under mu).
	activeMu sync.Mutex
	active   map[int64]*activeFile

	// salvage bookkeeping (salvage.go / disk.go). salvageMu guards refsEnabled;
	// salvaging files are now claimed by the durable store lease (LeaseSalvage),
	// not an in-memory map.
	salvageMu   sync.Mutex
	refsEnabled bool // any --reference roots recorded

	// repo cache: repo ID -> row (meta worker fills; download reads).
	repoMu   sync.RWMutex
	repoByID map[int64]*store.Repo

	// destinations of submitted jobs, for the reference exclusion test.
	destsMu sync.Mutex
	dests   []string

	// primary job/repo for the Stats header (last submitted).
	primaryMu sync.RWMutex
	primary   *jobHeader

	// snapshot caches.
	countsMu sync.Mutex
	countsAt time.Time
	counts   map[string]int64
	totalsAt time.Time
	totals   queueTotals

	// run lifecycle.
	runMu     sync.Mutex
	running   bool
	runCancel context.CancelFunc
	detached  context.Context // WithoutCancel(run ctx): cleanup writes
	wg        sync.WaitGroup

	// terminal errors collected for errors.Join at drain.
	errsMu sync.Mutex
	errs   []error

	// job spans: sched.job per submitted job, ended when the job goes
	// terminal (drain sweeper).
	spansMu sync.Mutex
	spans   map[int64]trace.Span

	// ENOSPC pause bookkeeping (gates.go): the failed demand, the probe
	// directory, and the poll backoff persisted across episodes of one run
	// (a flapping volume reaches the cap and stays there). enospcMu guards the
	// whole episode transition (check-then-start) plus enospcDemand/enospcDir,
	// so two concurrent IO failures can never both spawn a watcher and the
	// watcher never races a mid-episode demand raise.
	enospcMu        sync.Mutex
	enospcDemand    int64
	enospcDir       string
	enospcBackoffNs atomic.Int64

	// test seams.
	statfsFreeFn             func(ctx context.Context, dir string) (int64, error)
	probeFn                  func(ctx context.Context, dir string, bytes int64) error
	probeGiveUpLimit         int
	probeBackoffMax          time.Duration
	enospcPoll               time.Duration
	nowFn                    func() time.Time
	recoverInterval          time.Duration // defaults to recoverInterval const
	runDownload              func(ctx context.Context, t *transfer.FileTask, sink *fcio.File, progress transfer.ProgressSink) error
	freeSpaceUnsupportedOnce sync.Once
	identityUnsupportedOnce  sync.Once
}

type jobHeader struct {
	jobID    int64
	repoID   int64
	repo     string
	revision string
	repoType hfapi.RepoType
}

// activeFile is one in-flight download.
type activeFile struct {
	file   store.File
	conns  int
	cancel context.CancelFunc
}

// NewManager builds the manager. Limits come from cfg.Limits; persisted
// limits (kv) are loaded at Run start, the first point a caller ctx is
// available (NewManager takes none).
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	if cfg.Prov == nil {
		cfg.Prov = otel.Noop()
	}
	if cfg.Limits.APIIOPS == 0 && cfg.Limits.DiskActivePct == 0 && cfg.Limits.Conns == 0 {
		cfg.Limits = config.DefaultLimits()
	}
	m := &Manager{
		cfg:             cfg,
		log:             logging.Component(cfg.Log, "sched"),
		st:              cfg.Store,
		tracer:          cfg.Prov.Tracer(otelScope),
		pause:           newGate(),
		enospc:          newGate(),
		wakeMeta:        make(chan struct{}, 1),
		wakeDownload:    make(chan struct{}, 1),
		wakeDisk:        make(chan struct{}, 1),
		wakeInstall:     make(chan struct{}, 1),
		active:          make(map[int64]*activeFile),
		repoByID:        make(map[int64]*store.Repo),
		spans:           make(map[int64]trace.Span),
		statfsFreeFn:    statfsFree,
		probeFn:         writeProbe,
		enospcPoll:      time.Second,
		nowFn:           time.Now,
		recoverInterval: recoverInterval,
	}
	if cfg.Downloader != nil {
		m.runDownload = cfg.Downloader.Run
	}
	m.limits = cfg.Limits
	m.metaEndpoint = pickMetaEndpoint(cfg.Clients)

	meter := cfg.Prov.Meter(otelScope)
	m.filesCompleted, _ = meter.Int64Counter("hfdl.files.completed", metric.WithUnit("{file}"))
	m.filesFailed, _ = meter.Int64Counter("hfdl.files.failed", metric.WithUnit("{file}"))
	m.salvageBytes, _ = meter.Int64Counter("hfdl.download.bytes", metric.WithUnit("By"))
	m.stalls, _ = meter.Int64Counter("hfdl.stalls", metric.WithUnit("{event}"))
	m.blockRetries, _ = meter.Int64Counter("hfdl.block.retries", metric.WithUnit("{event}"))
	return m
}

// pickMetaEndpoint chooses the listing hub: the default HF endpoint when
// configured, else the lexicographically first (deterministic).
func pickMetaEndpoint(clients map[string]*hfapi.Client) string {
	if _, ok := clients[config.DefaultEndpoint]; ok {
		return config.DefaultEndpoint
	}
	keys := make([]string, 0, len(clients))
	for ep := range clients {
		keys = append(keys, ep)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return ""
	}
	return keys[0]
}

// clientFor returns the hfapi client for endpoint, falling back to the
// metadata hub client.
func (m *Manager) clientFor(endpoint string) *hfapi.Client {
	if c, ok := m.cfg.Clients[endpoint]; ok {
		return c
	}
	return m.cfg.Clients[m.metaEndpoint]
}

// currentLimits returns the live limits.
func (m *Manager) currentLimits() config.Limits {
	m.limitsMu.RLock()
	defer m.limitsMu.RUnlock()
	return m.limits
}

// wake nudges a queue worker (coalesced).
func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// recordError collects a terminal error for the drain aggregate.
func (m *Manager) recordError(err error) {
	if err == nil {
		return
	}
	m.errsMu.Lock()
	m.errs = append(m.errs, err)
	m.errsMu.Unlock()
}

// joinedErrors returns the aggregate of terminal errors.
func (m *Manager) joinedErrors() error {
	m.errsMu.Lock()
	defer m.errsMu.Unlock()
	return errors.Join(m.errs...)
}

// repoFor resolves a repo row for a file, cache-first with a read-only
// store fallback.
func (m *Manager) repoFor(ctx context.Context, repoID int64) (*store.Repo, error) {
	m.repoMu.RLock()
	r := m.repoByID[repoID]
	m.repoMu.RUnlock()
	if r != nil {
		return r, nil
	}
	repo := new(store.Repo)
	if err := m.st.DB().NewSelect().Model(repo).Where("id = ?", repoID).Scan(ctx); err != nil {
		return nil, err
	}
	m.repoMu.Lock()
	m.repoByID[repoID] = repo
	m.repoMu.Unlock()
	return repo, nil
}

// heartbeat renews a store lease until done closes; a fencing failure
// (lease lost) is reported through onFenced so the worker abandons its row.
func (m *Manager) heartbeat(ctx context.Context, renew func(until time.Time) error, onFenced func(), attrs ...any) {
	tick := time.NewTicker(heartbeatInterval)
	defer tick.Stop()
	var failingSince time.Time
	leaseUntil := m.nowFn().Add(leaseTTL)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			now := m.nowFn()
			nextUntil := now.Add(2 * heartbeatInterval)
			err := renew(nextUntil)
			if errors.Is(err, store.ErrFenced) {
				if onFenced != nil {
					onFenced()
				}
				return
			}
			if err == nil {
				failingSince = time.Time{}
				leaseUntil = nextUntil
				continue
			}
			if failingSince.IsZero() {
				failingSince = now
			}
			logAttrs := append(append([]any{}, attrs...), "err", err, "failed_for", now.Sub(failingSince))
			m.log.Warn("lease renewal failed", logAttrs...)
			if !now.Before(leaseUntil) {
				m.log.Error("lease renewal unavailable past lease TTL; abandoning guarded work", logAttrs...)
				if onFenced != nil {
					onFenced()
				}
				return
			}
		}
	}
}
