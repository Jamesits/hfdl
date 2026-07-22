package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/netcfg"
	"github.com/jamesits/hfdl/pkg/sched"
)

// getenvMap fakes os.Getenv for the flag/env precedence matrix.
func getenvMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// absPath turns a slash-style golden path into the absolute OS-native path
// buildPlan/finalPath produce (filepath.Abs). On Windows a rooted-but-driveless
// path like /cache resolves against the current drive, so the expectation and
// the production path absolutize against the same drive and agree.
func absPath(t *testing.T, p string) string {
	t.Helper()
	a, err := filepath.Abs(filepath.FromSlash(p))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func baseFlags() *downloadFlags {
	return &downloadFlags{
		repoID:         "org/repo",
		repoType:       "model",
		revision:       "main",
		maxWorkers:     8,
		connections:    8,
		policyStr:      "best-speed",
		apiIOPS:        5,
		diskActive:     100,
		stallTimeout:   15 * time.Second,
		stallMinStr:    "32KiB",
		ioModeStr:      "buffered",
		checkpointIntv: 30 * time.Second,
		logLevelStr:    "info",
	}
}

func TestBuildPlanFlagEnvMatrix(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		p, err := buildPlan(baseFlags(), getenvMap(nil))
		if err != nil {
			t.Fatal(err)
		}
		if p.ref.Repo != "org/repo" || p.ref.RepoType != hfapi.RepoTypeModel || p.ref.Revision != "main" {
			t.Fatalf("ref = %+v", p.ref)
		}
		if len(p.endpoints) != 1 || p.endpoints[0] != config.DefaultEndpoint {
			t.Fatalf("endpoints = %v", p.endpoints)
		}
		want := config.DefaultLimits()
		if p.limits != want {
			t.Fatalf("limits = %+v, want %+v", p.limits, want)
		}
		if p.logLevel != slog.LevelInfo {
			t.Fatalf("logLevel = %v", p.logLevel)
		}
	})

	cases := []struct {
		name   string
		mutate func(*downloadFlags)
		env    map[string]string
		check  func(t *testing.T, p *downloadPlan)
	}{
		{
			name: "repo-type dataset",
			mutate: func(f *downloadFlags) {
				f.repoType = "dataset"
			},
			check: func(t *testing.T, p *downloadPlan) {
				if p.ref.RepoType != hfapi.RepoTypeDataset {
					t.Fatalf("repoType = %v", p.ref.RepoType)
				}
			},
		},
		{
			name: "HF_ENDPOINT env",
			env:  map[string]string{"HF_ENDPOINT": "https://mirror.example/"},
			check: func(t *testing.T, p *downloadPlan) {
				if len(p.endpoints) != 1 || p.endpoints[0] != "https://mirror.example" {
					t.Fatalf("endpoints = %v", p.endpoints)
				}
			},
		},
		{
			name: "--endpoint beats HF_ENDPOINT",
			mutate: func(f *downloadFlags) {
				f.endpointFlags = []string{"https://a.example", "https://b.example"}
			},
			env: map[string]string{"HF_ENDPOINT": "https://mirror.example"},
			check: func(t *testing.T, p *downloadPlan) {
				if len(p.endpoints) != 2 || p.endpoints[0] != "https://a.example" {
					t.Fatalf("endpoints = %v", p.endpoints)
				}
			},
		},
		{
			name: "HF_HUB_CACHE env",
			env:  map[string]string{"HF_HUB_CACHE": "/tmp/hfcache"},
			check: func(t *testing.T, p *downloadPlan) {
				want := absPath(t, "/tmp/hfcache")
				if p.cacheDir != want {
					t.Fatalf("cacheDir = %q", p.cacheDir)
				}
				if p.cli.StateDB != filepath.Join(want, ".hfdl", "state.db") {
					t.Fatalf("stateDB = %q", p.cli.StateDB)
				}
			},
		},
		{
			name: "--cache-dir beats HF_HUB_CACHE",
			mutate: func(f *downloadFlags) {
				f.cacheDirFlag = "/tmp/flagcache"
			},
			env: map[string]string{"HF_HUB_CACHE": "/tmp/envcache"},
			check: func(t *testing.T, p *downloadPlan) {
				if want := absPath(t, "/tmp/flagcache"); p.cacheDir != want {
					t.Fatalf("cacheDir = %q", p.cacheDir)
				}
			},
		},
		{
			name: "HF_HOME hub fallback",
			env:  map[string]string{"HF_HOME": "/tmp/hfhome"},
			check: func(t *testing.T, p *downloadPlan) {
				if want := absPath(t, "/tmp/hfhome/hub"); p.cacheDir != want {
					t.Fatalf("cacheDir = %q", p.cacheDir)
				}
			},
		},
		{
			name: "HF_TOKEN env",
			env:  map[string]string{"HF_TOKEN": "hf_secret"},
			check: func(t *testing.T, p *downloadPlan) {
				if p.token != "hf_secret" {
					t.Fatalf("token = %q", p.token)
				}
			},
		},
		{
			name: "--token beats HF_TOKEN",
			mutate: func(f *downloadFlags) {
				f.tokenFlag = "hf_flag"
			},
			env: map[string]string{"HF_TOKEN": "hf_env"},
			check: func(t *testing.T, p *downloadPlan) {
				if p.token != "hf_flag" {
					t.Fatalf("token = %q", p.token)
				}
			},
		},
		{
			name: "limits from flags",
			mutate: func(f *downloadFlags) {
				f.maxWorkers = 4
				f.connections = 16
				f.bandwidthStr = "500MiB/s"
				f.apiIOPS = 9
				f.diskActive = 55
				f.diskWorkers = 3
				f.stallTimeout = 3 * time.Second
				f.stallMinStr = "1MiB"
				f.blockSizeStr = "8MiB"
				f.ioBufferStr = "256MiB"
				f.checkpointIntv = time.Minute
				f.policyStr = "round-robin"
				f.ioModeStr = "direct"
			},
			check: func(t *testing.T, p *downloadPlan) {
				l := p.limits
				if l.MaxWorkers != 4 || l.Conns != 16 {
					t.Fatalf("workers/conns = %d/%d", l.MaxWorkers, l.Conns)
				}
				if l.MaxBandwidthBps != 500<<20 {
					t.Fatalf("bandwidth = %d", l.MaxBandwidthBps)
				}
				if l.APIIOPS != 9 || l.DiskActivePct != 55 {
					t.Fatalf("api/disk = %d/%d", l.APIIOPS, l.DiskActivePct)
				}
				if l.DiskWorkers != 3 {
					t.Fatalf("disk-workers = %d", l.DiskWorkers)
				}
				if l.StallTimeout != 3*time.Second || l.StallMinBytes != 1<<20 {
					t.Fatalf("stall = %v/%d", l.StallTimeout, l.StallMinBytes)
				}
				if l.BlockSize != 8<<20 || l.IOBuffer != 256<<20 {
					t.Fatalf("block/iobuf = %d/%d", l.BlockSize, l.IOBuffer)
				}
				if l.CheckpointInterval != time.Minute {
					t.Fatalf("checkpoint = %v", l.CheckpointInterval)
				}
				if l.UpstreamPolicy != config.RoundRobin || l.IOMode != config.IODirect {
					t.Fatalf("policy/iomode = %v/%v", l.UpstreamPolicy, l.IOMode)
				}
			},
		},
		{
			name: "include/exclude/references passthrough",
			mutate: func(f *downloadFlags) {
				f.include = []string{"*.safetensors"}
				f.exclude = []string{"*.bin"}
				f.references = []string{"/data/refs"}
				f.filenames = []string{"model.safetensors"}
				f.quiet = true
				f.forceDownload = true
				f.dryRun = true
				f.noTUI = true
			},
			check: func(t *testing.T, p *downloadPlan) {
				if p.cli.Include[0] != "*.safetensors" || p.cli.Exclude[0] != "*.bin" {
					t.Fatalf("globs = %v/%v", p.cli.Include, p.cli.Exclude)
				}
				if p.cli.References[0] != "/data/refs" {
					t.Fatalf("references = %v", p.cli.References)
				}
				if !p.quiet || !p.cli.ForceDownload || !p.cli.DryRun || !p.noTUI {
					t.Fatalf("bool flags = %+v", p.cli)
				}
				if !p.single {
					t.Fatal("single-file positional not detected")
				}
			},
		},
		{
			name: "log-level debug",
			mutate: func(f *downloadFlags) {
				f.logLevelStr = "debug"
			},
			check: func(t *testing.T, p *downloadPlan) {
				if p.logLevel != slog.LevelDebug {
					t.Fatalf("logLevel = %v", p.logLevel)
				}
			},
		},
		{
			name: "proxy and ipqos flags",
			mutate: func(f *downloadFlags) {
				f.proxyStr = "socks5h://127.0.0.1:1080"
				f.ipqosStr = "af21,0x20,none"
			},
			check: func(t *testing.T, p *downloadPlan) {
				if p.proxy.Mode != netcfg.ProxyURL || p.cli.Proxy != "socks5h://127.0.0.1:1080" {
					t.Fatalf("proxy = %+v / %q", p.proxy, p.cli.Proxy)
				}
				if p.qos.API != 0x48 || p.qos.Download != 0x20 || p.qos.Telemetry != netcfg.TOSNone {
					t.Fatalf("qos = %+v", p.qos)
				}
				if p.cli.IPQoS != "af21,0x20,none" {
					t.Fatalf("cli.IPQoS = %q", p.cli.IPQoS)
				}
			},
		},
		{
			name: "defaults: system proxy, af21/cs1 qos",
			check: func(t *testing.T, p *downloadPlan) {
				if p.proxy.Mode != netcfg.ProxySystem || p.cli.Proxy != "system" {
					t.Fatalf("proxy = %+v / %q", p.proxy, p.cli.Proxy)
				}
				if p.qos.API != 0x48 || p.qos.Download != 0x20 || p.qos.Telemetry != 0x20 {
					t.Fatalf("qos = %+v", p.qos)
				}
			},
		},
		{
			name: "explicit --state-db",
			mutate: func(f *downloadFlags) {
				f.stateDBFlag = "/tmp/custom/state.db"
			},
			env: map[string]string{"HF_HUB_CACHE": "/tmp/cache"},
			check: func(t *testing.T, p *downloadPlan) {
				if p.cli.StateDB != "/tmp/custom/state.db" {
					t.Fatalf("stateDB = %q", p.cli.StateDB)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := baseFlags()
			if tc.mutate != nil {
				tc.mutate(f)
			}
			p, err := buildPlan(f, getenvMap(tc.env))
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, p)
		})
	}
}

func TestBuildPlanTokenEnvParity(t *testing.T) {
	tmp := t.TempDir()
	tokenFile := filepath.Join(tmp, "token")
	if err := os.WriteFile(tokenFile, []byte("  hf_from_file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(tmp, "hfhome")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "token"), []byte("hf_from_home"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("HF_TOKEN_PATH beats implicit home token", func(t *testing.T) {
		p, err := buildPlan(baseFlags(), getenvMap(map[string]string{
			"HF_TOKEN_PATH": tokenFile,
			"HF_HOME":       home,
		}))
		if err != nil {
			t.Fatal(err)
		}
		if p.token != "hf_from_file" {
			t.Fatalf("token = %q", p.token)
		}
	})
	t.Run("implicit $HF_HOME/token", func(t *testing.T) {
		p, err := buildPlan(baseFlags(), getenvMap(map[string]string{"HF_HOME": home}))
		if err != nil {
			t.Fatal(err)
		}
		if p.token != "hf_from_home" {
			t.Fatalf("token = %q", p.token)
		}
	})
	t.Run("implicit token disabled", func(t *testing.T) {
		p, err := buildPlan(baseFlags(), getenvMap(map[string]string{
			"HF_HOME":                       home,
			"HF_HUB_DISABLE_IMPLICIT_TOKEN": "1",
		}))
		if err != nil {
			t.Fatal(err)
		}
		if p.token != "" {
			t.Fatalf("token = %q", p.token)
		}
	})
}

func TestBuildPlanRepoRefVariants(t *testing.T) {
	cases := []struct {
		positional string
		repoType   string
		wantType   hfapi.RepoType
		wantRepo   string
		wantRev    string // "" → falls back to --revision (main)
	}{
		{"org/repo", "model", hfapi.RepoTypeModel, "org/repo", ""},
		{"org/repo@v2", "model", hfapi.RepoTypeModel, "org/repo", "v2"},
		{"hf://org/repo", "model", hfapi.RepoTypeModel, "org/repo", ""},
		{"hf://models/org/repo@main", "dataset", hfapi.RepoTypeModel, "org/repo", "main"},
		{"hf://datasets/org/repo", "model", hfapi.RepoTypeDataset, "org/repo", ""},
		{"hf://datasets/org/repo@refs/pr/3", "model", hfapi.RepoTypeDataset, "org/repo", "refs/pr/3"},
		{"hf://spaces/org/repo@dev", "model", hfapi.RepoTypeSpace, "org/repo", "dev"},
	}
	for _, tc := range cases {
		t.Run(tc.positional, func(t *testing.T) {
			f := baseFlags()
			f.repoID = tc.positional
			f.repoType = tc.repoType
			p, err := buildPlan(f, getenvMap(nil))
			if err != nil {
				t.Fatal(err)
			}
			if p.ref.RepoType != tc.wantType || p.ref.Repo != tc.wantRepo {
				t.Fatalf("ref = %+v", p.ref)
			}
			wantRev := tc.wantRev
			if wantRev == "" {
				wantRev = "main"
			}
			if p.ref.Revision != wantRev {
				t.Fatalf("revision = %q, want %q", p.ref.Revision, wantRev)
			}
		})
	}
}

func TestBuildPlanErrors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*downloadFlags)
	}{
		{"local-dir xor cache-dir", func(f *downloadFlags) {
			f.localDir = "/a"
			f.cacheDirFlag = "/b"
		}},
		{"bad repo-type", func(f *downloadFlags) { f.repoType = "wiki" }},
		{"bad repo ref", func(f *downloadFlags) { f.repoID = "a/b/c" }},
		{"empty revision after @", func(f *downloadFlags) { f.repoID = "org/repo@" }},
		{"bad max-bandwidth", func(f *downloadFlags) { f.bandwidthStr = "fast" }},
		{"bad block-size", func(f *downloadFlags) { f.blockSizeStr = "10x" }},
		{"bad stall-min-bytes", func(f *downloadFlags) { f.stallMinStr = "lots" }},
		{"bad io-buffer", func(f *downloadFlags) { f.ioBufferStr = "-1q" }},
		{"bad upstream-policy", func(f *downloadFlags) { f.policyStr = "chaos" }},
		{"bad io-mode", func(f *downloadFlags) { f.ioModeStr = "turbo" }},
		{"bad log-level", func(f *downloadFlags) { f.logLevelStr = "trace" }},
		{"api-iops zero", func(f *downloadFlags) { f.apiIOPS = 0 }},
		{"disk-active out of range", func(f *downloadFlags) { f.diskActive = 101 }},
		{"disk-workers negative", func(f *downloadFlags) { f.diskWorkers = -1 }},
		{"connections zero", func(f *downloadFlags) { f.connections = 0 }},
		{"max-workers zero", func(f *downloadFlags) { f.maxWorkers = 0 }},
		{"stall-timeout zero", func(f *downloadFlags) { f.stallTimeout = 0 }},
		{"checkpoint-interval zero", func(f *downloadFlags) { f.checkpointIntv = 0 }},
		{"bad proxy scheme", func(f *downloadFlags) { f.proxyStr = "ftp://proxy.example" }},
		{"proxy missing host", func(f *downloadFlags) { f.proxyStr = "http://" }},
		{"bad ipqos keyword", func(f *downloadFlags) { f.ipqosStr = "warp-speed" }},
		{"ipqos too many values", func(f *downloadFlags) { f.ipqosStr = "af21,cs1,le,ef" }},
		{"ipqos space separated", func(f *downloadFlags) { f.ipqosStr = "af21 cs1" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := baseFlags()
			tc.mutate(f)
			if _, err := buildPlan(f, getenvMap(nil)); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestDownloadCmdFlagParseErrors(t *testing.T) {
	// Duration-typed flags fail at cobra's flag parse, before RunE.
	cmd := newDownloadCmd()
	cmd.SetArgs([]string{})
	if err := cmd.ParseFlags([]string{"--stall-timeout", "not-a-duration"}); err == nil {
		t.Fatal("expected --stall-timeout parse error")
	}
	cmd = newDownloadCmd()
	if err := cmd.ParseFlags([]string{"--checkpoint-interval", "soon"}); err == nil {
		t.Fatal("expected --checkpoint-interval parse error")
	}
}

func TestFinalPath(t *testing.T) {
	cachePlan := func(single bool) *downloadPlan {
		f := baseFlags()
		if single {
			f.filenames = []string{"model.safetensors"}
		}
		p, err := buildPlan(f, getenvMap(map[string]string{"HF_HUB_CACHE": "/cache"}))
		if err != nil {
			t.Fatal(err)
		}
		return p
	}

	t.Run("cache mode single file", func(t *testing.T) {
		p := cachePlan(true)
		snap := &sched.Stats{CommitSHA: "abc123"}
		got := finalPath(p, snap)
		want := absPath(t, "/cache/models--org--repo/snapshots/abc123/model.safetensors")
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("cache mode snapshot dir", func(t *testing.T) {
		p := cachePlan(false)
		snap := &sched.Stats{CommitSHA: "abc123"}
		got := finalPath(p, snap)
		want := absPath(t, "/cache/models--org--repo/snapshots/abc123")
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("cache mode dataset namespace", func(t *testing.T) {
		f := baseFlags()
		f.repoID = "hf://datasets/org/repo"
		p, err := buildPlan(f, getenvMap(map[string]string{"HF_HUB_CACHE": "/cache"}))
		if err != nil {
			t.Fatal(err)
		}
		got := finalPath(p, &sched.Stats{CommitSHA: "deadbeef"})
		want := absPath(t, "/cache/datasets--org--repo/snapshots/deadbeef")
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("local-dir file", func(t *testing.T) {
		f := baseFlags()
		f.localDir = "/out"
		f.filenames = []string{"config.json"}
		p, err := buildPlan(f, getenvMap(nil))
		if err != nil {
			t.Fatal(err)
		}
		got := finalPath(p, &sched.Stats{})
		if want := absPath(t, "/out/config.json"); got != want {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("local-dir dir", func(t *testing.T) {
		f := baseFlags()
		f.localDir = "/out"
		p, err := buildPlan(f, getenvMap(nil))
		if err != nil {
			t.Fatal(err)
		}
		if got := finalPath(p, &sched.Stats{}); got != absPath(t, "/out") {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("nil snapshot falls back to revision", func(t *testing.T) {
		p := cachePlan(false)
		got := finalPath(p, nil)
		want := absPath(t, "/cache/models--org--repo/snapshots/main")
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}

// TestFinalOutputParity pins the huggingface_hub 1.24.0 stdout format: the
// bare absolute local path and nothing else, in every mode — quiet or not,
// TTY or not, cache or local-dir. No "path=" prefix (that would break
// `LOCAL=$(hf download ...)` capture in existing scripts).
func TestFinalOutputParity(t *testing.T) {
	snap := &sched.Stats{CommitSHA: "abc123"}

	t.Run("non-quiet cache dir is bare", func(t *testing.T) {
		p, err := buildPlan(baseFlags(), getenvMap(map[string]string{"HF_HUB_CACHE": "/cache"}))
		if err != nil {
			t.Fatal(err)
		}
		got := finalPath(p, snap)
		want := absPath(t, "/cache/models--org--repo/snapshots/abc123")
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("quiet cache dir is bare", func(t *testing.T) {
		f := baseFlags()
		f.quiet = true
		p, err := buildPlan(f, getenvMap(map[string]string{"HF_HUB_CACHE": "/cache"}))
		if err != nil {
			t.Fatal(err)
		}
		got := finalPath(p, snap)
		want := absPath(t, "/cache/models--org--repo/snapshots/abc123")
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("non-quiet local-dir single file is bare", func(t *testing.T) {
		f := baseFlags()
		f.localDir = "/out"
		f.filenames = []string{"config.json"}
		p, err := buildPlan(f, getenvMap(nil))
		if err != nil {
			t.Fatal(err)
		}
		got := finalPath(p, snap)
		want := absPath(t, "/out/config.json")
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("quiet local-dir single file is bare", func(t *testing.T) {
		f := baseFlags()
		f.localDir = "/out"
		f.filenames = []string{"config.json"}
		f.quiet = true
		p, err := buildPlan(f, getenvMap(nil))
		if err != nil {
			t.Fatal(err)
		}
		got := finalPath(p, snap)
		want := absPath(t, "/out/config.json")
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}
