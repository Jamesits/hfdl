package store

import (
	"testing"
	"time"
)

func TestCooldowns(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	until := time.Date(2026, 7, 21, 13, 0, 0, 0, time.UTC)

	if got, err := s.CooldownUntil(ctx, "https://hf.co", CooldownAPI); err != nil || !got.IsZero() {
		t.Fatalf("CooldownUntil missing = (%v, %v), want zero", got, err)
	}
	if err := s.SetCooldown(ctx, "https://hf.co", CooldownAPI, until, "429"); err != nil {
		t.Fatal(err)
	}
	got, err := s.CooldownUntil(ctx, "https://hf.co", CooldownAPI)
	if err != nil || !got.Equal(until) {
		t.Fatalf("CooldownUntil = (%v, %v), want %v", got, err, until)
	}

	// Upsert replaces; kinds are independent.
	later := until.Add(time.Minute)
	if err := s.SetCooldown(ctx, "https://hf.co", CooldownAPI, later, "429 again"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetCooldown(ctx, "https://hf.co", CooldownCAS, until, "cas 429"); err != nil {
		t.Fatal(err)
	}
	cds, err := s.Cooldowns(ctx)
	if err != nil || len(cds) != 2 {
		t.Fatalf("Cooldowns = (%d, %v), want 2", len(cds), err)
	}
	for _, cd := range cds {
		if cd.Kind == CooldownAPI && !cd.Until.Equal(later) {
			t.Errorf("api cooldown until = %v, want updated %v", cd.Until, later)
		}
	}
}

func TestUpstreams(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	cd := time.Date(2026, 7, 21, 14, 0, 0, 0, time.UTC)

	if err := s.UpsertUpstream(ctx, "https://cdn1.example"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertUpstream(ctx, "https://cdn1.example"); err != nil {
		t.Fatal(err)
	}
	ups, err := s.UpstreamState(ctx)
	if err != nil || len(ups) != 1 {
		t.Fatalf("UpstreamState = (%d, %v), want 1", len(ups), err)
	}

	if err := s.UpdateUpstream(ctx, "https://cdn1.example", 1.5e8, true, &cd, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateUpstream(ctx, "https://cdn1.example", 1.2e8, false, nil, nil); err != nil {
		t.Fatal(err)
	}
	ups, err = s.UpstreamState(ctx)
	if err != nil || len(ups) != 1 {
		t.Fatalf("UpstreamState = (%d, %v)", len(ups), err)
	}
	u := ups[0]
	if u.Successes != 1 || u.Errors != 1 {
		t.Errorf("tallies = (%d ok, %d err), want (1, 1)", u.Successes, u.Errors)
	}
	if u.EmaBps != 1.2e8 {
		t.Errorf("ema_bps = %v, want 1.2e8", u.EmaBps)
	}
	if u.CooldownUntil == nil || !u.CooldownUntil.Equal(cd) {
		t.Errorf("cooldown_until = %v, want %v (nil update must not clear)", u.CooldownUntil, cd)
	}

	// Unknown endpoint: inserted on first update.
	if err := s.UpdateUpstream(ctx, "https://cdn2.example", 5e7, true, nil, nil); err != nil {
		t.Fatal(err)
	}
	ups, err = s.UpstreamState(ctx)
	if err != nil {
		t.Fatalf("UpstreamState after implicit insert: %v", err)
	}
	if len(ups) != 2 {
		t.Fatalf("UpstreamState = %d, want 2 after implicit insert", len(ups))
	}
}
