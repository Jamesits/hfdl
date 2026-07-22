package tune

import "time"

// Defaults for Config's zero values. The settle window must comfortably
// exceed the caller's rate-measurement window (stats uses a 10s ring) so the
// compared rate is a fully-settled 10s sample, never a blend of before and
// after nor the transient of connections still in TCP slow-start. It is held
// well above the ring — not merely past it — so probe accept/revert decisions
// ride on steady state rather than measurement noise.
const (
	DefaultSettle        = 24 * time.Second
	DefaultProbeInterval = 5 * time.Second
	DefaultBackoffStart  = 30 * time.Second
	DefaultBackoffMax    = 5 * time.Minute
	DefaultKillFreeze    = 30 * time.Second

	// defaultGain is the minimum relative rate improvement that keeps a
	// probed budget: below it the extra connections only re-split the same
	// pipe (or measurement noise), so the probe reverts.
	defaultGain = 0.05
	// defaultUtilCeiling is the bandwidth-bucket utilization above which the
	// configured ceiling — not connection count — is the active constraint,
	// so probing is pointless.
	defaultUtilCeiling = 0.9
	// doubleBelow is the budget under which growth doubles (fast ramp for a
	// small ceiling, whose half-max seed lands here); at or above it growth is
	// +25% so a large pool converges without overshooting the sweet spot by 2x.
	doubleBelow = 8
	// saturationNum/Den: probing requires at least 3/4 of the budget to be
	// assigned to live files, otherwise the budget is not the constraint and
	// raising it measures nothing.
	saturationNum, saturationDen = 3, 4
)

// Sample is one tick's view of the world.
type Sample struct {
	Rate        float64 // global windowed download rate, bytes/sec
	Utilization float64 // bandwidth bucket utilization (0 when unlimited)
	Assigned    int     // connections currently assigned to live files
	Kills       int64   // cumulative stall kills
}

// Config tunes the controller; zero values take the defaults above.
type Config struct {
	Settle        time.Duration // post-probe wait before comparing rates
	ProbeInterval time.Duration // pause between an accepted probe and the next
	BackoffStart  time.Duration // first revert backoff
	BackoffMax    time.Duration // revert backoff ceiling
	KillFreeze    time.Duration // probe freeze after a stall kill
	Gain          float64       // minimum relative gain to keep a probe
	UtilCeiling   float64       // bucket utilization that suppresses probing
}

func (c Config) withDefaults() Config {
	if c.Settle <= 0 {
		c.Settle = DefaultSettle
	}
	if c.ProbeInterval <= 0 {
		c.ProbeInterval = DefaultProbeInterval
	}
	if c.BackoffStart <= 0 {
		c.BackoffStart = DefaultBackoffStart
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = DefaultBackoffMax
	}
	if c.KillFreeze <= 0 {
		c.KillFreeze = DefaultKillFreeze
	}
	if c.Gain <= 0 {
		c.Gain = defaultGain
	}
	if c.UtilCeiling <= 0 {
		c.UtilCeiling = defaultUtilCeiling
	}
	return c
}

// Controller is the hill-climbing budget state machine. Not safe for
// concurrent use: sched's tune loop is the sole caller; everyone else reads
// the loop's published budget.
type Controller struct {
	cfg Config

	budget     int
	prevBudget int // pre-probe budget to revert to
	probing    bool
	seeded     bool // half-max start applied on the first Observe

	baseRate    float64   // rate baseline recorded when the probe started
	settleUntil time.Time // probe evaluation time
	nextProbeAt time.Time
	backoff     time.Duration // current revert backoff (0 = none yet)

	freezeUntil time.Time // kill freeze
	lastKills   int64
	killsSeeded bool
}

// New builds a controller. Its budget seeds to half the ceiling on the first
// Observe — the ceiling is unknown until then — and reports 1 until that
// first sample arrives.
func New(cfg Config) *Controller {
	return &Controller{cfg: cfg.withDefaults(), budget: 1}
}

// Budget returns the current budget without observing a sample.
func (c *Controller) Budget() int { return c.budget }

// Committed returns the last accepted budget: Budget minus any in-flight
// probe increment. Admission of new files must key off this, not Budget —
// a probe is speculative, and a file admitted against it cannot be
// un-admitted when the probe reverts (every active file keeps at least one
// connection), which would leave the assignment above the reverted budget
// until a file completes and invalidate later probe rate comparisons.
func (c *Controller) Committed() int {
	if c.probing {
		return c.prevBudget
	}
	return c.budget
}

// Observe feeds one tick's sample and returns the budget to enforce,
// always within [1, maxBudget].
func (c *Controller) Observe(now time.Time, maxBudget int, s Sample) int {
	if maxBudget < 1 {
		maxBudget = 1
	}
	// Seed at half the ceiling on the first sample rather than climbing from
	// 1: a fresh link almost always wants many connections, so start in the
	// middle and let the hill-climb refine up or down from there instead of
	// spending dozens of probe/settle cycles ramping. The ceiling is only
	// known here, at the first Observe, not at New.
	if !c.seeded {
		c.seeded = true
		c.budget = max(1, maxBudget/2)
	}
	if c.budget > maxBudget {
		// Operator lowered the ceiling mid-flight: clamp, and abandon any
		// probe measurement (its candidate budget no longer exists).
		c.budget = maxBudget
		c.probing = false
	}

	// Stall kills perturb the rate: freeze probing and drop an in-flight
	// probe rather than mis-attribute the kill's effect to the budget. The
	// first sample only seeds the counter — kills from before the controller
	// started must not freeze it.
	if !c.killsSeeded {
		c.lastKills, c.killsSeeded = s.Kills, true
	} else if s.Kills > c.lastKills {
		c.lastKills = s.Kills
		c.freezeUntil = now.Add(c.cfg.KillFreeze)
		if c.probing {
			c.budget = c.prevBudget
			c.probing = false
		}
	}

	if c.probing {
		if now.Before(c.settleUntil) {
			return c.budget
		}
		c.probing = false
		if s.Rate >= c.baseRate*(1+c.cfg.Gain) {
			// The extra connections raised the global rate: keep them and
			// keep climbing soon.
			c.backoff = 0
			c.nextProbeAt = now.Add(c.cfg.ProbeInterval)
		} else {
			// No global gain: the pipe (or the remote) is the constraint.
			c.budget = c.prevBudget
			if c.backoff == 0 {
				c.backoff = c.cfg.BackoffStart
			} else {
				c.backoff = min(c.backoff*2, c.cfg.BackoffMax)
			}
			c.nextProbeAt = now.Add(c.backoff)
		}
		return c.budget
	}

	if now.Before(c.nextProbeAt) || now.Before(c.freezeUntil) {
		return c.budget
	}
	if c.budget >= maxBudget {
		return c.budget
	}
	if s.Utilization >= c.cfg.UtilCeiling {
		// The configured bandwidth ceiling is the active constraint; more
		// connections cannot raise the rate. Re-check after a probe interval.
		c.nextProbeAt = now.Add(c.cfg.ProbeInterval)
		return c.budget
	}
	if s.Assigned*saturationDen < c.budget*saturationNum {
		// The current budget is not even assigned (fewer files than budget,
		// or the run is winding down): raising it measures nothing.
		return c.budget
	}
	if s.Rate <= 0 {
		return c.budget
	}

	c.prevBudget = c.budget
	c.baseRate = s.Rate
	c.budget = min(nextStep(c.budget), maxBudget)
	c.probing = true
	c.settleUntil = now.Add(c.cfg.Settle)
	return c.budget
}

// nextStep grows the budget: doubling while small (fast start from 1), +25%
// afterwards.
func nextStep(budget int) int {
	if budget < doubleBelow {
		return budget * 2
	}
	return budget + max(1, budget/4)
}
