package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/jamesits/hfdl/pkg/cache"
	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/netcfg"
)

// destMode selects where downloaded blobs are materialized (cache.Installer
// InstallRequest.DestMode values).
const (
	destModeCache    = "cache"
	destModeLocalDir = "local-dir"
)

// downloadFlags is the raw flag/env model for `hfdl download`.
// Parsing is split from execution so the whole flag/env matrix is testable
// without touching the store or the network.
type downloadFlags struct {
	repoID    string
	filenames []string

	repoType      string
	revision      string
	include       []string
	exclude       []string
	localDir      string
	cacheDirFlag  string
	tokenFlag     string
	quiet         bool
	forceDownload bool
	dryRun        bool

	endpointFlags  []string
	maxWorkers     int
	blockSizeStr   string
	policyStr      string
	sourcePrioStr  string
	references     []string
	bandwidthStr   string
	apiIOPS        int64
	diskActive     int
	diskWorkers    int
	stallTimeout   time.Duration
	stallMinStr    string
	ioModeStr      string
	ioBufferStr    string
	checkpointIntv time.Duration
	stateDBFlag    string
	logLevelStr    string
	noTUI          bool
	proxyStr       string
	ipqosStr       string
}

func newDownloadCmd() *cobra.Command {
	f := &downloadFlags{}
	// Single source of truth for numeric flag defaults: config.DefaultLimits.
	// Keeps the cobra registration, config.DefaultLimits and the parity tests
	// from drifting apart.
	dfl := config.DefaultLimits()
	cmd := &cobra.Command{
		Use:   "download <repo_id> [<filename> ...]",
		Short: "Download files from the Hugging Face Hub",
		Long: "Download a model, dataset or space from the Hugging Face Hub.\n" +
			"repo_id accepts \"org/repo\" or an hf:// URI (\"hf://datasets/org/repo@rev\").\n" +
			"On success the absolute local path (file or directory) is printed on stdout\n" +
			"as exactly that path and nothing else, matching `hf download`.",
		Args:         cobra.MinimumNArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			f.repoID = args[0]
			if len(args) > 1 {
				f.filenames = args[1:]
			}
			return runDownload(cmd.Context(), f, os.Getenv)
		},
	}

	fl := cmd.Flags()
	// Drop-in upstream flags
	fl.StringVar(&f.repoType, "repo-type", string(hfapi.RepoTypeModel), "repo type: model|dataset|space")
	fl.StringVar(&f.revision, "revision", "main", "git revision (branch, tag or commit)")
	fl.StringArrayVar(&f.include, "include", nil, "glob patterns of files to include")
	fl.StringArrayVar(&f.exclude, "exclude", nil, "glob patterns of files to exclude")
	fl.StringVar(&f.localDir, "local-dir", "", "download into a local directory instead of the HF cache")
	fl.StringVar(&f.cacheDirFlag, "cache-dir", "", "HF cache root (default $HF_HUB_CACHE or $HF_HOME/hub)")
	fl.StringVar(&f.tokenFlag, "token", "", "Hugging Face token (default $HF_TOKEN)")
	fl.BoolVar(&f.quiet, "quiet", false, "suppress progress output; only the final path is printed")
	fl.BoolVar(&f.forceDownload, "force-download", false, "re-download even when the file is already cached")
	fl.BoolVar(&f.dryRun, "dry-run", false, "resolve and list what would be downloaded, then exit")
	fl.IntVar(&f.maxWorkers, "max-workers", dfl.MaxWorkers, "total download connections across all files")

	// hfdl extensions (must start with hfdl)
	fl.StringArrayVar(&f.endpointFlags, "hfdl-endpoint", nil, "Hub endpoint URL (repeatable; mirrors after the first; default $HF_ENDPOINT)")
	fl.StringVar(&f.blockSizeStr, "hfdl-block-size", "", "download block size (default: adaptive 4MiB-64MiB)")
	fl.StringVar(&f.policyStr, "hfdl-upstream-policy", config.BestSpeed.String(), "per-block upstream selection: best-speed|random|round-robin")
	fl.StringVar(&f.sourcePrioStr, "hfdl-source-priority", "", "transfer source preference when a xet hash exists: xet|cdn (default xet; cdn when $HF_HUB_DISABLE_XET is set)")
	fl.StringArrayVar(&f.references, "hfdl-reference", nil, "local file/dir to salvage whole-file matches from (repeatable)")
	fl.StringVar(&f.bandwidthStr, "hfdl-max-bandwidth", "", "global download bandwidth cap (e.g. 500MiB/s; default unlimited)")
	fl.Int64Var(&f.apiIOPS, "hfdl-api-iops", dfl.APIIOPS, "HF API requests per second")
	fl.IntVar(&f.diskActive, "hfdl-disk-active", dfl.DiskActivePct, "disk duty-cycle ceiling 1-100 (percent)")
	fl.IntVar(&f.diskWorkers, "hfdl-disk-workers", dfl.DiskWorkers, "disk-queue workers for hashing/salvage/copy (0 = auto)")
	fl.DurationVar(&f.stallTimeout, "hfdl-stall-timeout", dfl.StallTimeout, "idle-read stall window")
	fl.StringVar(&f.stallMinStr, "hfdl-stall-min-bytes", "32KiB", "minimum bytes per stall window before a connection is killed")
	fl.StringVar(&f.ioModeStr, "hfdl-io-mode", config.IOBuffered.String(), "storage IO mode: buffered|direct|sequential")
	fl.StringVar(&f.ioBufferStr, "hfdl-io-buffer", "", "RAM write-cache pool cap (default auto: clamp(slab*connections*2, 64MiB, 1GiB), capped at half of system RAM)")
	fl.DurationVar(&f.checkpointIntv, "hfdl-checkpoint-interval", dfl.CheckpointInterval, "durable progress cadence (flush/fsync/persist)")
	fl.StringVar(&f.stateDBFlag, "hfdl-state-db", "", "state database path (default <cache>/.hfdl/state.db)")
	fl.StringVar(&f.logLevelStr, "hfdl-log-level", "info", "log level: debug|info|warn|error")
	fl.BoolVar(&f.noTUI, "hfdl-no-tui", false, "disable the interactive TUI")
	fl.StringVar(&f.proxyStr, "hfdl-proxy", netcfg.SpecSystem,
		"outbound proxy: direct|system|http://…|https://…|socks5://…|socks5h://…")
	fl.StringVar(&f.ipqosStr, "hfdl-ipqos", netcfg.DefaultIPQoS,
		"IP QoS for default[,download[,telemetry]] sockets (e.g. af21,cs1,none or 0x48,0x20)")
	return cmd
}

// downloadPlan is the fully resolved, validated invocation. Everything
// runDownload needs flows from here; construction is pure (parse-only, no
// IO beyond the filesystem-independent env reads through getenv).
type downloadPlan struct {
	ref       hfapi.RepoRef
	cli       config.CLI
	limits    config.Limits
	endpoints []string
	cacheDir  string
	token     string
	quiet     bool
	noTUI     bool
	logLevel  slog.Level
	proxy     netcfg.Proxy
	qos       netcfg.IPQoS
	single    bool // exactly one filename positional → final path is the file
}

func buildPlan(f *downloadFlags, getenv func(string) string) (*downloadPlan, error) {
	if f.localDir != "" && f.cacheDirFlag != "" {
		return nil, fmt.Errorf("--local-dir and --cache-dir cannot be used together: " +
			"use --cache-dir (or $HF_HOME) for shared caching, " +
			"or --local-dir for a one-off download to a specific directory")
	}

	var repoType hfapi.RepoType
	switch hfapi.RepoType(f.repoType) {
	case hfapi.RepoTypeModel, hfapi.RepoTypeDataset, hfapi.RepoTypeSpace:
		repoType = hfapi.RepoType(f.repoType)
	default:
		return nil, fmt.Errorf("invalid --repo-type %q: want model|dataset|space", f.repoType)
	}
	ref, err := hfapi.ParseRepoRef(f.repoID, repoType)
	if err != nil {
		return nil, err
	}
	revision := ref.Revision
	if revision == "" {
		revision = f.revision
	}

	limits := config.DefaultLimits()
	limits.MaxWorkers = f.maxWorkers
	limits.APIIOPS = f.apiIOPS
	limits.DiskActivePct = f.diskActive
	limits.DiskWorkers = f.diskWorkers
	limits.StallTimeout = f.stallTimeout
	limits.CheckpointInterval = f.checkpointIntv
	if f.bandwidthStr != "" {
		if limits.MaxBandwidthBps, err = config.ParseRate(f.bandwidthStr); err != nil {
			return nil, fmt.Errorf("--max-bandwidth: %w", err)
		}
	}
	if f.blockSizeStr != "" {
		if limits.BlockSize, err = config.ParseSize(f.blockSizeStr); err != nil {
			return nil, fmt.Errorf("--block-size: %w", err)
		}
	}
	if f.stallMinStr != "" {
		if limits.StallMinBytes, err = config.ParseSize(f.stallMinStr); err != nil {
			return nil, fmt.Errorf("--stall-min-bytes: %w", err)
		}
	}
	if f.ioBufferStr != "" {
		if limits.IOBuffer, err = config.ParseSize(f.ioBufferStr); err != nil {
			return nil, fmt.Errorf("--io-buffer: %w", err)
		}
	}
	if limits.UpstreamPolicy, err = config.ParseUpstreamPolicy(f.policyStr); err != nil {
		return nil, err
	}
	if limits.SourcePriority, err = config.ResolveSourcePriority(f.sourcePrioStr, getenv); err != nil {
		return nil, err
	}
	if limits.IOMode, err = config.ParseIOMode(f.ioModeStr); err != nil {
		return nil, err
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}

	lvl, err := config.ParseLogLevel(f.logLevelStr)
	if err != nil {
		return nil, err
	}

	// Parsing is pure; the system-mode platform lookup happens later in
	// wireTelemetry, where a ctx and logger exist.
	proxy, err := netcfg.ParseProxy(f.proxyStr)
	if err != nil {
		return nil, fmt.Errorf("--hfdl-proxy: %w", err)
	}
	qos, err := netcfg.ParseIPQoS(f.ipqosStr)
	if err != nil {
		return nil, fmt.Errorf("--hfdl-ipqos: %w", err)
	}

	endpoints := config.Endpoints(f.endpointFlags, getenv)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoint resolved")
	}
	cacheDir := config.CacheDir(f.cacheDirFlag, getenv)
	if f.localDir != "" {
		abs, err := filepath.Abs(f.localDir)
		if err != nil {
			return nil, fmt.Errorf("--local-dir: %w", err)
		}
		f.localDir = abs
	} else {
		abs, err := filepath.Abs(cacheDir)
		if err != nil {
			return nil, fmt.Errorf("cache dir %q: %w", cacheDir, err)
		}
		cacheDir = abs
	}

	p := &downloadPlan{
		ref:       hfapi.RepoRef{RepoType: ref.RepoType, Repo: ref.Repo, Revision: revision},
		limits:    limits,
		endpoints: endpoints,
		cacheDir:  cacheDir,
		token:     hfapi.ResolveToken(f.tokenFlag, getenv),
		quiet:     f.quiet,
		noTUI:     f.noTUI,
		logLevel:  slog.Level(lvl),
		proxy:     proxy,
		qos:       qos,
		single:    len(f.filenames) == 1,
	}
	p.cli = config.CLI{
		RepoID:        f.repoID,
		Filenames:     f.filenames,
		RepoType:      string(ref.RepoType),
		Revision:      revision,
		Include:       f.include,
		Exclude:       f.exclude,
		LocalDir:      f.localDir,
		CacheDir:      cacheDir,
		Token:         p.token,
		Quiet:         f.quiet,
		ForceDownload: f.forceDownload,
		DryRun:        f.dryRun,
		MaxWorkers:    f.maxWorkers,
		Endpoints:     endpoints,
		References:    f.references,
		StateDB:       config.StateDBPath(f.stateDBFlag, cacheDir),
		LogLevel:      f.logLevelStr,
		NoTUI:         f.noTUI,
		Proxy:         proxy.String(),
		IPQoS:         qos.String(),
	}
	return p, nil
}

// runDownload executes the download command: build plan → wire the process
// in bootstrap order (stderr handler, open store, attach DB/OTLP sinks)
// → submit job → TUI or plain progress → final path.
func runDownload(ctx context.Context, f *downloadFlags, getenv func(string) string) (err error) {
	p, err := buildPlan(f, getenv)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopSignals := watchSignals(ctx, cancel, defaultSignalChan())
	defer stopSignals()

	// TUI owns the screen only on a real TTY; quiet and non-TTY fall back to
	// periodic slog progress on stderr (or silence when quiet). The decision
	// precedes wiring: TUI mode constructs the root handler without a stderr
	// leg at all.
	useTUI := !p.quiet && !p.noTUI && stdoutIsTTY()

	// Bootstrap telemetry first (root logger + OTel providers) so the tracer
	// exists before the hfdl.run span opens. Component wiring runs afterwards
	// under that span, giving one trace that spans the whole invocation.
	a, err := wireTelemetry(ctx, p, getenv, !useTUI && !p.quiet)
	if err != nil {
		return err
	}
	defer a.close(ctx)

	// hfdl.run root span: component wiring, sched.job and transfer.file spans
	// all nest under it via ctx. Registered after a.close so its End runs first
	// (defer LIFO), ending the span before the provider shutdown flushes it.
	ctx, endRun := startRunSpan(ctx, a.prov, p)
	// endRun records the named return err on the hfdl.run span before ending
	// it; read at defer time so it captures whichever return path fires.
	defer func() { endRun(err) }()

	// Component wiring (store migrations, cache open, filesystem probe, manager)
	// is now traced under hfdl.run.
	if err := a.wireComponents(ctx, p, getenv, newManager); err != nil {
		return err
	}

	if err := a.submit(ctx, p); err != nil {
		return err
	}

	runErrCh := make(chan error, 1)
	go func() { runErrCh <- a.runManager(ctx) }()

	var runErr error
	if useTUI {
		// TUI and manager race each other: the manager draining tears the UI
		// down (ctx cancel), and the user quitting the UI drains the manager.
		uiDone := make(chan error, 1)
		go func() { uiDone <- a.runTUI(ctx) }()
		select {
		case runErr = <-runErrCh:
			cancel()
			<-uiDone
		case uiErr := <-uiDone:
			if uiErr != nil {
				a.log.LogAttrs(ctx, slog.LevelWarn, "tui ended with error", slog.Any("err", uiErr))
			}
			cancel()
			runErr = <-runErrCh
		}
	} else if !p.quiet {
		runErr = a.progressLoop(ctx, runErrCh)
	} else {
		runErr = <-runErrCh
	}
	if runErr != nil {
		// The structured record lands in the log fan-out; the plain final
		// `Error:` line for script parity is printed once by main.
		a.log.LogAttrs(context.WithoutCancel(ctx), slog.LevelError, "download failed", slog.Any("err", runErr))
		return runErr
	}

	// Dry-run prints its own report and never the path line (hf 1.24.0).
	if p.cli.DryRun {
		return printDryRun(os.Stdout, a.manager.DryRunReport())
	}

	// The final path is the contract output: exactly the absolute local path
	// and nothing else on stdout, matching the pinned hf CLI (huggingface_hub
	// 1.24.0) in every mode — quiet, non-quiet, TTY or not (TUI mode prints
	// after teardown). A failed write (EPIPE, full disk on redirected output)
	// is a real error, not a log line.
	if _, err := fmt.Fprintln(os.Stdout, finalPath(p, a.manager.Snapshot())); err != nil {
		return fmt.Errorf("write final path: %w", err)
	}
	return nil
}

// finalPath computes the absolute local path `hf download` reports: the file
// path for a single-file download, otherwise the directory the repo landed
// in. sched's snapshot carries the commit SHA resolved during listing, so
// the snapshot dir is computable without another Hub round trip.
func finalPath(p *downloadPlan, snap *schedSnapshot) string {
	if p.cli.LocalDir != "" {
		if p.single {
			return filepath.Join(p.cli.LocalDir, p.cli.Filenames[0])
		}
		return p.cli.LocalDir
	}
	repoDir := filepath.Join(p.cacheDir, cache.ModelDirName(string(p.ref.RepoType), p.ref.Repo))
	sha := p.ref.Revision
	if snap != nil && snap.CommitSHA != "" {
		sha = snap.CommitSHA
	}
	snapDir := filepath.Join(repoDir, "snapshots", sha)
	if p.single {
		return filepath.Join(snapDir, p.cli.Filenames[0])
	}
	return snapDir
}
