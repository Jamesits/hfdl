package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// SetCooldown upserts a 429 gate scoped per endpoint and protocol stage
// (CooldownAPI / CooldownCAS): the retry policy's per-(endpoint, kind)
// cooldowns.
func (s *Store) SetCooldown(ctx context.Context, endpoint, kind string, until time.Time, reason string) error {
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO endpoint_cooldowns (endpoint, kind, until, reason) VALUES (?, ?, ?, ?) "+
			"ON CONFLICT (endpoint, kind) DO UPDATE SET until = excluded.until, reason = excluded.reason",
		endpoint, kind, utc(until), reason); err != nil {
		return fmt.Errorf("store: set cooldown %s/%s: %w", endpoint, kind, err)
	}
	return nil
}

// CooldownUntil returns the gate expiry for (endpoint, kind); the zero time
// with nil error when no gate exists.
func (s *Store) CooldownUntil(ctx context.Context, endpoint, kind string) (time.Time, error) {
	var until time.Time
	err := s.db.QueryRowContext(ctx,
		"SELECT until FROM endpoint_cooldowns WHERE endpoint = ? AND kind = ?",
		endpoint, kind).Scan(&until)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("store: cooldown until %s/%s: %w", endpoint, kind, err)
	}
	return until, nil
}

// Cooldowns lists every gate; sched consults it before dequeuing.
func (s *Store) Cooldowns(ctx context.Context) ([]EndpointCooldown, error) {
	var cds []EndpointCooldown
	if err := s.db.NewSelect().Model(&cds).
		Order("endpoint", "kind").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("store: cooldowns: %w", err)
	}
	return cds, nil
}

// UpsertUpstream registers an endpoint as a download upstream; existing rows
// keep their accumulated stats.
func (s *Store) UpsertUpstream(ctx context.Context, endpoint string) error {
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO upstreams (endpoint) VALUES (?) "+
			"ON CONFLICT (endpoint) DO UPDATE SET updated_at = ?",
		endpoint, utc(time.Now())); err != nil {
		return fmt.Errorf("store: upsert upstream %s: %w", endpoint, err)
	}
	return nil
}

// UpstreamState returns all upstreams with their stats and gates.
func (s *Store) UpstreamState(ctx context.Context) ([]Upstream, error) {
	var ups []Upstream
	if err := s.db.NewSelect().Model(&ups).
		Order("endpoint").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("store: upstream state: %w", err)
	}
	return ups, nil
}

// UpdateUpstream records one transfer outcome for an endpoint: EMA speed,
// success/error tally, and optional cooldown/blacklist gates (nil leaves the
// existing gate untouched). Inserts the row when the endpoint is new.
func (s *Store) UpdateUpstream(ctx context.Context, endpoint string, emaBps float64, ok bool, cooldownUntil, blacklistUntil *time.Time) error {
	succ, errs := 0, 0
	if ok {
		succ = 1
	} else {
		errs = 1
	}
	now := utc(time.Now())
	// One atomic upsert: the old update-then-insert raced (two concurrent
	// callers on a new endpoint both saw 0 updated rows and both inserted,
	// tripping the UNIQUE(endpoint) constraint). ON CONFLICT accumulates the
	// success/error tallies against the existing row.
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO upstreams (endpoint, ema_bps, successes, errors, cooldown_until, blacklist_until, updated_at) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?) "+
			"ON CONFLICT (endpoint) DO UPDATE SET "+
			"ema_bps = excluded.ema_bps, "+
			"successes = upstreams.successes + excluded.successes, "+
			"errors = upstreams.errors + excluded.errors, "+
			"cooldown_until = COALESCE(excluded.cooldown_until, upstreams.cooldown_until), "+
			"blacklist_until = COALESCE(excluded.blacklist_until, upstreams.blacklist_until), "+
			"updated_at = excluded.updated_at",
		endpoint, emaBps, succ, errs, cooldownUntil, blacklistUntil, now); err != nil {
		return fmt.Errorf("store: update upstream %s: %w", endpoint, err)
	}
	return nil
}
