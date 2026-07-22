package config

import (
	"fmt"
	"time"
)

// IOMode selects the fcio storage tier: buffered uses the OS page cache
// plus fadvise hints, direct is the O_DIRECT opt-in, sequential the
// in-order fallback for pathological storage. The zero value is IOBuffered
// so a zero Limits is usable.
type IOMode int

const (
	IOBuffered   IOMode = iota // buffered IO + fadvise (default)
	IODirect                   // O_DIRECT / FILE_FLAG_NO_BUFFERING opt-in
	IOSequential               // in-order fallback for pathological storage
)

func (m IOMode) String() string {
	switch m {
	case IOBuffered:
		return "buffered"
	case IODirect:
		return "direct"
	case IOSequential:
		return "sequential"
	}
	return "unknown"
}

func ParseIOMode(s string) (IOMode, error) {
	switch s {
	case "buffered":
		return IOBuffered, nil
	case "direct":
		return IODirect, nil
	case "sequential":
		return IOSequential, nil
	}
	return IOBuffered, fmt.Errorf("invalid io-mode %q: want buffered|direct|sequential", s)
}

// UpstreamPolicy is the per-block upstream selection strategy: which
// endpoint serves the next block request.
type UpstreamPolicy int

const (
	BestSpeed UpstreamPolicy = iota // max EMA, ε-greedy (ε=0.1) — default
	Random
	RoundRobin
)

func (p UpstreamPolicy) String() string {
	switch p {
	case BestSpeed:
		return "best-speed"
	case Random:
		return "random"
	case RoundRobin:
		return "round-robin"
	}
	return "unknown"
}

func ParseUpstreamPolicy(s string) (UpstreamPolicy, error) {
	switch s {
	case "best-speed":
		return BestSpeed, nil
	case "random":
		return Random, nil
	case "round-robin":
		return RoundRobin, nil
	}
	return BestSpeed, fmt.Errorf("invalid upstream-policy %q: want random|round-robin|best-speed", s)
}

// SourcePriority selects which transfer source to prefer for a file that a
// xet hash makes available two ways: xet (content-defined chunk dedup served
// from the CAS) or cdn (the plain HTTP resolve URL the Hub redirects to its
// CDN). A file with no xet hash is CDN-only regardless, so PreferXet still
// falls back to the CDN there; PreferCDN forces the CDN even when a xet hash
// exists. The zero value is PreferXet so a zero Limits keeps the historical
// "xet when available" behaviour.
type SourcePriority int

const (
	PreferXet SourcePriority = iota // xet when available, else cdn (default)
	PreferCDN                       // always cdn, even when a xet hash exists
)

func (s SourcePriority) String() string {
	switch s {
	case PreferXet:
		return "xet"
	case PreferCDN:
		return "cdn"
	}
	return "unknown"
}

func ParseSourcePriority(s string) (SourcePriority, error) {
	switch s {
	case "xet":
		return PreferXet, nil
	case "cdn":
		return PreferCDN, nil
	}
	return PreferXet, fmt.Errorf("invalid source-priority %q: want xet|cdn", s)
}

// Limits is the hot-settable operator constraint set shared by sched,
// throttle and transfer. It is updated live via sched.Manager.SetLimits and
// persisted to the store kv table so restarts keep the last setting.
//
// Bandwidth/DiskActivePct use 0 specially in SetLimits, where 0 suspends
// the corresponding activity; CLI flag validation keeps user-facing values
// in range instead.
type Limits struct {
	MaxBandwidthBps    int64         // global download token bucket; 0 = unlimited
	APIIOPS            int64         // HF API requests/sec
	APIBurst           int64         // API bucket burst
	DiskActivePct      int           // disk duty-cycle ceiling 1–100 (percentage)
	DiskWorkers        int           // disk-queue worker count; 0 = auto by media (SSD 2, else 1). Runtime-capped to GOMAXPROCS-1 to avoid CPU starvation (see sched.diskWorkerCount)
	Conns              int           // per-file block connections
	MaxWorkers         int           // files in downloading at once (upstream parity flag)
	BlockSize          int64         // 0 = adaptive: clamp(pow2(size/conns), 4MiB, 64MiB)
	StallTimeout       time.Duration // idle-read / soft floor window
	StallMinBytes      int64         // soft throughput floor per window
	IOBuffer           int64         // fcio pool cap bytes; 0 = auto clamp(slab×conns×2, 64MiB, 1GiB) capped at ½ RAM
	CheckpointInterval time.Duration // durable progress cadence
	IOMode             IOMode
	UpstreamPolicy     UpstreamPolicy
	SourcePriority     SourcePriority // xet vs cdn transfer source preference
}

// bandwidthMinBurst floors the bandwidth bucket burst when a limit is set: a
// burst below the caller's read-chunk size (pool slabs are multi-MiB) forces
// throttle.Bucket into single-token installments that hit its per-wait floor,
// collapsing throughput to a crawl.
const bandwidthMinBurst = 8 << 20

// BandwidthBurst picks the token-bucket burst for a bandwidth ceiling:
// max(limit/2, 8MiB) when limited; an unlimited (<=0) ceiling ignores burst.
// It is shared by the initial wiring and by SetLimits so every rate change is
// paired with a burst that scales to it — inheriting a stale burst (notably an
// unlimited bucket's 0, which SetRate clamps to 1) would throttle the whole
// download to ~1KiB/s.
func BandwidthBurst(limit int64) int64 {
	if limit <= 0 {
		return 0
	}
	if b := limit / 2; b > bandwidthMinBurst {
		return b
	}
	return bandwidthMinBurst
}

func DefaultLimits() Limits {
	return Limits{
		MaxBandwidthBps:    0,
		APIIOPS:            5,
		APIBurst:           1,
		DiskActivePct:      100,
		DiskWorkers:        0, // auto: sched picks by media class
		Conns:              8,
		MaxWorkers:         8,
		BlockSize:          0,
		StallTimeout:       15 * time.Second,
		StallMinBytes:      32 * 1024,
		IOBuffer:           0,
		CheckpointInterval: 30 * time.Second,
		IOMode:             IOBuffered,
		UpstreamPolicy:     BestSpeed,
		SourcePriority:     PreferXet,
	}
}

// Validate enforces the CLI-facing ranges: api-iops, connections and
// max-workers >= 1, disk-active within 1-100, positive durations.
func (l *Limits) Validate() error {
	if l.APIIOPS < 1 {
		return fmt.Errorf("api-iops must be >= 1, got %d", l.APIIOPS)
	}
	if l.DiskActivePct < 1 || l.DiskActivePct > 100 {
		return fmt.Errorf("disk-active must be 1-100, got %d", l.DiskActivePct)
	}
	if l.DiskWorkers < 0 {
		return fmt.Errorf("disk-workers must be >= 0 (0 = auto), got %d", l.DiskWorkers)
	}
	if l.Conns < 1 {
		return fmt.Errorf("connections must be >= 1, got %d", l.Conns)
	}
	if l.MaxWorkers < 1 {
		return fmt.Errorf("max-workers must be >= 1, got %d", l.MaxWorkers)
	}
	if l.StallTimeout <= 0 {
		return fmt.Errorf("stall-timeout must be positive")
	}
	if l.CheckpointInterval <= 0 {
		return fmt.Errorf("checkpoint-interval must be positive")
	}
	return nil
}
