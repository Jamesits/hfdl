package transfer

import (
	"errors"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
)

// Upstream selection on block start: Random / RoundRobin over
// the healthy set / BestSpeed as max-EMA with ε-greedy exploration. Stalled
// or errored upstreams are cooled down locally for upstreamCooldownTTL and
// skipped until expiry.

const (
	// epsilonGreedy is the exploration rate for BestSpeed.
	epsilonGreedy = 0.1
	// emaAlpha matches stats.Registry's EMA smoothing so the per-file view
	// and the global registry agree.
	emaAlpha = 0.2
	// emaPenaltyFactor halves the EMA on stall/error.
	emaPenaltyFactor = 0.5
	// upstreamCooldownTTL parks a 429/503 upstream for this file; the next
	// block goes elsewhere.
	upstreamCooldownTTL = 30 * time.Second
	// defaultUpstreamBlacklistTTL is the shorter temporary park applied on a
	// stall or generic transport/validation error (EMA *= 0.5, temporary
	// blacklist TTL, next block goes elsewhere). Kept short so a transiently
	// slow mirror recovers, unlike the 30s 429 cooldown. It is the default for
	// Config.UpstreamBlacklistTTL; the resolved value is carried on httpSource.
	defaultUpstreamBlacklistTTL = 5 * time.Second
)

// healthyLocked filters upstreams usable right now: not identity-excluded,
// not rangeless (unless ignoreRangeless — the single-stream fallback), and
// past any cooldown/blacklist (per-file cooldowns plus the store-seeded
// fields on the Upstream itself).
func (s *httpSource) healthyLocked(now time.Time, ignoreRangeless bool) []*Upstream {
	out := make([]*Upstream, 0, len(s.upstreams))
	for _, u := range s.upstreams {
		ep := u.Endpoint
		if s.excluded[ep] {
			continue
		}
		if !ignoreRangeless && s.rangeless[ep] {
			continue
		}
		if until, ok := s.cooldown[ep]; ok && now.Before(until) {
			continue
		}
		if now.Before(u.CooldownUntil) || now.Before(u.BlacklistUntil) {
			continue
		}
		out = append(out, u)
	}
	return out
}

// pick chooses the upstream for one block attempt per the configured policy.
// When every upstream is rangeless the caller switches to the fallback, so
// pick never has to serve that case; no-healthy (all cooled down) is a
// retriable FailNoHealthy so the block waits out the cooldowns.
func (s *httpSource) pick(now time.Time) (*Upstream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	healthy := s.healthyLocked(now, false)
	if len(healthy) == 0 {
		allRangeless := len(s.upstreams) > 0
		for _, u := range s.upstreams {
			if !s.rangeless[u.Endpoint] {
				allRangeless = false
				break
			}
		}
		if allRangeless {
			return nil, errAllRangeless
		}
		if len(s.upstreams) == 0 {
			return nil, &AttemptError{Kind: FailNoHealthy, Err: errors.New("no upstreams configured")}
		}
		return nil, &AttemptError{Kind: FailNoHealthy, Err: errors.New("all upstreams cooled down or blacklisted")}
	}
	return s.pickLocked(healthy), nil
}

// pickWhole chooses the fallback upstream: rangeless marks are meaningless
// for a plain GET, but identity-excluded mirrors stay out.
func (s *httpSource) pickWhole(now time.Time) *Upstream {
	s.mu.Lock()
	defer s.mu.Unlock()
	if healthy := s.healthyLocked(now, true); len(healthy) > 0 {
		return s.pickLocked(healthy)
	}
	for _, u := range s.upstreams {
		if !s.excluded[u.Endpoint] {
			return u
		}
	}
	return nil
}

// pickLocked applies the policy over a non-empty healthy set.
func (s *httpSource) pickLocked(healthy []*Upstream) *Upstream {
	switch s.policy {
	case config.Random:
		return healthy[s.rng.IntN(len(healthy))]
	case config.RoundRobin:
		s.rr++
		return healthy[(s.rr-1)%uint64(len(healthy))]
	default: // config.BestSpeed: exploit max EMA, ε-greedy explore
		if s.rng.Float64() < epsilonGreedy {
			return healthy[s.rng.IntN(len(healthy))]
		}
		best := healthy[0]
		for _, u := range healthy[1:] {
			if s.ema[u.Endpoint] > s.ema[best.Endpoint] {
				best = u
			}
		}
		return best
	}
}
