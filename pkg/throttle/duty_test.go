package throttle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// newFakeDuty wires a limiter to a fake clock and recording sleeper.
func newFakeDuty(level int, media MediaClass) (*DutyLimiter, *fakeClock, *fakeSleeper) {
	fc := newFakeClock()
	sl := &fakeSleeper{clock: fc}
	d := NewDutyLimiter(level, media)
	d.clock = fc
	d.sleep = sl.sleep
	return d, fc, sl
}

func TestCheckpointRatioMath(t *testing.T) {
	cases := []struct {
		name      string
		level     int
		media     MediaClass
		busy      time.Duration
		wantSleep time.Duration
	}{
		// active fraction A = level/100, derated by media;
		// remain = busy*(1-A)/A + lastRemain, cap 700ms
		{"p100 is unlimited", 100, MediaSSD, 500 * time.Millisecond, 0},
		{"p50 ssd: sleep == busy", 50, MediaSSD, 500 * time.Millisecond, 500 * time.Millisecond},
		{"p50 netfs derate hits cap", 50, MediaNetFS, 500 * time.Millisecond, 700 * time.Millisecond}, // A=0.357, raw 900ms
		{"p50 hdd derate hits cap", 50, MediaHDD, 500 * time.Millisecond, 700 * time.Millisecond},     // A=0.25, raw 1500ms
		{"p50 unknown derates like hdd", 50, MediaUnknown, 200 * time.Millisecond, 600 * time.Millisecond},
		{"p80 ssd: 0.25x busy", 80, MediaSSD, 100 * time.Millisecond, 25 * time.Millisecond},
		{"p80 hdd: A=0.4 -> 1.5x busy", 80, MediaHDD, 200 * time.Millisecond, 300 * time.Millisecond},
		{"p80 netfs: A=0.571 -> 0.75x busy", 80, MediaNetFS, 200 * time.Millisecond, 150 * time.Millisecond},
		{"p90 ssd", 90, MediaSSD, 900 * time.Millisecond, 100 * time.Millisecond},
		{"p99 ssd", 99, MediaSSD, 100 * time.Millisecond, 2 * time.Millisecond}, // 1.01ms debt rounds up in 1ms slices
		{"busy clamped to 1s then remain capped", 50, MediaSSD, 5 * time.Second, 700 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, fc, sl := newFakeDuty(tc.level, tc.media)
			var c DutyCalc
			if err := d.Checkpoint(t.Context(), &c); err != nil { // prime lastTick
				t.Fatal(err)
			}
			if n := len(sl.lens()); n != 0 {
				t.Fatalf("priming checkpoint slept %d times", n)
			}
			fc.Advance(tc.busy)
			if err := d.Checkpoint(t.Context(), &c); err != nil {
				t.Fatal(err)
			}
			if got := sl.sum(); got != tc.wantSleep {
				t.Fatalf("slept %v, want %v", got, tc.wantSleep)
			}
			for i, s := range sl.lens() {
				if s > 200*time.Millisecond {
					t.Fatalf("sleep slice %d = %v exceeds 200ms", i, s)
				}
			}
		})
	}
}

func TestCheckpointSleepSlices(t *testing.T) {
	// level 50, busy 650ms -> remain 650ms: 3x200ms slices, then a 1ms-slice
	// tail of 50ms (FastCopy: unit = remain > 200 ? 200 : 1).
	d, fc, sl := newFakeDuty(50, MediaSSD)
	var c DutyCalc
	if err := d.Checkpoint(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	fc.Advance(650 * time.Millisecond)
	if err := d.Checkpoint(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	calls := sl.lens()
	if len(calls) != 53 {
		t.Fatalf("sleep calls = %d, want 53 (3x200ms + 50x1ms)", len(calls))
	}
	for i, s := range calls {
		want := time.Millisecond
		if i < 3 {
			want = 200 * time.Millisecond
		}
		if s != want {
			t.Fatalf("slice %d = %v, want %v", i, s, want)
		}
	}
	if got := sl.sum(); got != 650*time.Millisecond {
		t.Fatalf("total sleep = %v, want 650ms", got)
	}
}

func TestCheckpointLastRemainCarry(t *testing.T) {
	// positive debt carries into the next remain computation
	d, fc, sl := newFakeDuty(50, MediaSSD)
	var c DutyCalc
	if err := d.Checkpoint(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	c.lastRemain = 50 * time.Millisecond
	fc.Advance(100 * time.Millisecond)
	if err := d.Checkpoint(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	if got := sl.sum(); got != 150*time.Millisecond {
		t.Fatalf("slept %v, want 150ms (100ms debt + 50ms carry)", got)
	}

	// negative credit reduces the next remain
	d2, fc2, sl2 := newFakeDuty(50, MediaSSD)
	var c2 DutyCalc
	if err := d2.Checkpoint(t.Context(), &c2); err != nil {
		t.Fatal(err)
	}
	c2.lastRemain = -30 * time.Millisecond
	fc2.Advance(100 * time.Millisecond)
	if err := d2.Checkpoint(t.Context(), &c2); err != nil {
		t.Fatal(err)
	}
	if got := sl2.sum(); got != 70*time.Millisecond {
		t.Fatalf("slept %v, want 70ms (100ms debt - 30ms credit)", got)
	}
}

func TestCheckpointOversleepCredit(t *testing.T) {
	// a sleeper that overshoots drives remain negative; the credit must be
	// stored in lastRemain like FastCopy's "pool".
	d, fc, sl := newFakeDuty(50, MediaSSD)
	sl.overshoot = 500 * time.Microsecond
	var c DutyCalc
	if err := d.Checkpoint(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	fc.Advance(100 * time.Millisecond)
	if err := d.Checkpoint(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	if got := c.lastRemain; got != -500*time.Microsecond {
		t.Fatalf("lastRemain = %v, want -500µs oversleep credit", got)
	}
	if n := len(sl.lens()); n != 67 {
		t.Fatalf("sleep calls = %d, want 67 (1.5ms progress per 1ms slice against 100ms)", n)
	}
}

func TestCheckpointCancel(t *testing.T) {
	d, fc, sl := newFakeDuty(50, MediaSSD)
	var c DutyCalc
	if err := d.Checkpoint(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	fc.Advance(500 * time.Millisecond) // remain = 500ms

	ctx, cancel := context.WithCancel(t.Context())
	sl.onCall = func(n int) {
		if n == 2 {
			cancel()
		}
	}
	err := d.Checkpoint(ctx, &c)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Checkpoint returned %v, want context.Canceled", err)
	}
	if n := len(sl.lens()); n != 2 {
		t.Fatalf("sleep calls = %d, want 2 before cancel", n)
	}
	// one 200ms slice landed before the cancel; the rest stays as debt
	if got := c.lastRemain; got != 300*time.Millisecond {
		t.Fatalf("lastRemain = %v, want 300ms carried debt", got)
	}
	s := d.Stats()
	if s.SleepDebt != 300*time.Millisecond {
		t.Fatalf("SleepDebt = %v, want 300ms", s.SleepDebt)
	}
	if s.Workers != 1 {
		t.Fatalf("Workers = %d, want 1", s.Workers)
	}
}

func TestCheckpointPreCancelledContext(t *testing.T) {
	d, fc, sl := newFakeDuty(50, MediaSSD)
	var c DutyCalc
	if err := d.Checkpoint(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	fc.Advance(500 * time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := d.Checkpoint(ctx, &c); !errors.Is(err, context.Canceled) {
		t.Fatalf("Checkpoint returned %v, want context.Canceled", err)
	}
	if n := len(sl.lens()); n != 0 {
		t.Fatalf("slept %d times with a pre-cancelled context", n)
	}
	if got := c.lastRemain; got != 500*time.Millisecond {
		t.Fatalf("lastRemain = %v, want untouched 500ms debt", got)
	}
}

func TestCheckpointPauseWakesOnSetLevel(t *testing.T) {
	d, fc, sl := newFakeDuty(0, MediaSSD)
	var c DutyCalc
	done := make(chan error, 1)
	go func() {
		done <- d.Checkpoint(t.Context(), &c)
	}()
	select {
	case <-done:
		t.Fatal("level-0 checkpoint returned instead of pausing")
	case <-time.After(300 * time.Millisecond): // spans at least one 200ms pause slice
	}
	d.SetLevel(50)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("paused checkpoint did not wake on SetLevel")
	}
	// the tick baseline restarted on resume: 100ms of busy -> 100ms of sleep
	fc.Advance(100 * time.Millisecond)
	if err := d.Checkpoint(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	if got := sl.sum(); got != 100*time.Millisecond {
		t.Fatalf("slept %v after resume, want 100ms (pause time is not busy time)", got)
	}
}

func TestCheckpointPauseCancel(t *testing.T) {
	d, _, _ := newFakeDuty(0, MediaSSD)
	var c DutyCalc
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- d.Checkpoint(ctx, &c)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("paused checkpoint returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("paused checkpoint did not honor cancel")
	}
}

func TestCheckpointUnlimitedDoesNoAccounting(t *testing.T) {
	d, fc, sl := newFakeDuty(100, MediaSSD)
	var c DutyCalc
	for range 5 {
		if err := d.Checkpoint(t.Context(), &c); err != nil {
			t.Fatal(err)
		}
		fc.Advance(500 * time.Millisecond)
	}
	if n := len(sl.lens()); n != 0 {
		t.Fatalf("unlimited limiter slept %d times", n)
	}
	s := d.Stats()
	if s.ActiveRatio != 0 || s.Workers != 0 || s.SleepDebt != 0 {
		t.Fatalf("unlimited limiter did accounting: %+v", s)
	}
}

func TestSetLevelHot(t *testing.T) {
	d, fc, sl := newFakeDuty(50, MediaSSD)
	var c DutyCalc
	ctx := t.Context()
	if err := d.Checkpoint(ctx, &c); err != nil {
		t.Fatal(err)
	}
	fc.Advance(100 * time.Millisecond)
	if err := d.Checkpoint(ctx, &c); err != nil {
		t.Fatal(err)
	}
	if got := sl.sum(); got != 100*time.Millisecond {
		t.Fatalf("slept %v, want 100ms", got)
	}

	d.SetLevel(100)
	fc.Advance(100 * time.Millisecond)
	if err := d.Checkpoint(ctx, &c); err != nil {
		t.Fatal(err)
	}
	if n := len(sl.lens()); n != 100 { // 100ms debt = 100 1ms slices, unchanged
		t.Fatalf("unlimited checkpoint slept; total calls = %d", n)
	}

	// back to 50: lastTick stayed fresh during the unlimited stretch, so the
	// next busy tick is 100ms — not the accumulated 200ms.
	d.SetLevel(50)
	fc.Advance(100 * time.Millisecond)
	if err := d.Checkpoint(ctx, &c); err != nil {
		t.Fatal(err)
	}
	if got := sl.sum(); got != 200*time.Millisecond {
		t.Fatalf("slept %v total, want 200ms (100ms per limited checkpoint)", got)
	}
}

func TestSetMediaHot(t *testing.T) {
	d, fc, sl := newFakeDuty(50, MediaSSD)
	var c DutyCalc
	ctx := t.Context()
	if err := d.Checkpoint(ctx, &c); err != nil {
		t.Fatal(err)
	}
	fc.Advance(200 * time.Millisecond)
	if err := d.Checkpoint(ctx, &c); err != nil {
		t.Fatal(err)
	}
	d.SetMedia(MediaNetFS)
	fc.Advance(200 * time.Millisecond)
	if err := d.Checkpoint(ctx, &c); err != nil {
		t.Fatal(err)
	}
	// SSD: 200ms; netfs: ratio 0.5/1.4 -> 200ms * 1.8 = 360ms
	if got := sl.sum(); got != 560*time.Millisecond {
		t.Fatalf("slept %v total, want 560ms (200ms ssd + 360ms netfs)", got)
	}
	if got := d.Stats().Media; got != MediaNetFS {
		t.Fatalf("Media = %v, want netfs", got)
	}
}

func TestWriteChunkSize(t *testing.T) {
	const normal = int64(8 << 20)
	cases := []struct {
		level int
		want  int64
	}{
		{-5, 256 << 10}, // clamped to 0 (pause): heaviest limiting
		{0, 256 << 10},  // pause: WAITMIN_BUF
		{1, 256 << 10},  // WAITMIN_BUF
		{50, 256 << 10}, // WAITMIN_BUF
		{89, 256 << 10}, // WAITMIN_BUF
		{90, 1 << 20},   // WAITMID_BUF
		{99, 1 << 20},   // WAITMID_BUF
		{100, normal},   // unlimited
		{150, normal},   // clamped to 100
	}
	d := NewDutyLimiter(100, MediaSSD)
	for _, tc := range cases {
		d.SetLevel(tc.level)
		if got := d.WriteChunkSize(normal); got != tc.want {
			t.Fatalf("level %d: WriteChunkSize = %d, want %d", tc.level, got, tc.want)
		}
	}
}

func TestDutyStats(t *testing.T) {
	d, fc, sl := newFakeDuty(50, MediaSSD)
	var a, b DutyCalc
	ctx := t.Context()
	if err := d.Checkpoint(ctx, &a); err != nil { // prime A
		t.Fatal(err)
	}
	fc.Advance(500 * time.Millisecond)
	if err := d.Checkpoint(ctx, &a); err != nil { // busy 500ms, sleep 500ms
		t.Fatal(err)
	}
	s := d.Stats()
	if s.Level != 50 || s.Media != MediaSSD {
		t.Fatalf("config in stats: %+v", s)
	}
	if s.ActiveRatio != 0.5 {
		t.Fatalf("ActiveRatio = %v, want 0.5 (500ms busy / 1000ms wall)", s.ActiveRatio)
	}
	if s.Workers != 1 {
		t.Fatalf("Workers = %d, want 1", s.Workers)
	}
	if s.SleepDebt != 0 {
		t.Fatalf("SleepDebt = %v, want 0", s.SleepDebt)
	}
	if got := sl.sum(); got != 500*time.Millisecond {
		t.Fatalf("slept %v, want 500ms", got)
	}

	if err := d.Checkpoint(ctx, &b); err != nil { // prime B -> second worker
		t.Fatal(err)
	}
	if got := d.Stats().Workers; got != 2 {
		t.Fatalf("Workers = %d, want 2", got)
	}

	// window + liveness TTL expire together
	fc.Advance(11 * time.Second)
	s = d.Stats()
	if s.ActiveRatio != 0 || s.Workers != 0 || s.SleepDebt != 0 {
		t.Fatalf("stats after window expiry: %+v", s)
	}

	// a swept worker re-registers on its next checkpoint
	if err := d.Checkpoint(ctx, &a); err != nil {
		t.Fatal(err)
	}
	if got := d.Stats().Workers; got != 1 {
		t.Fatalf("Workers after re-register = %d, want 1", got)
	}
}

func TestCheckpointConcurrent(t *testing.T) {
	// real clock + default sleeper at light limiting: each checkpoint sleeps
	// ~1ms, so all 32 workers stay inside the 10s liveness window and the
	// final worker count is deterministic. Run under -race.
	d := NewDutyLimiter(99, MediaSSD)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var c DutyCalc
			for range 50 {
				if err := d.Checkpoint(t.Context(), &c); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := d.Stats().Workers; got != 32 {
		t.Fatalf("Workers = %d, want 32", got)
	}
}

func TestMediaClassString(t *testing.T) {
	cases := map[MediaClass]string{
		MediaSSD: "ssd", MediaHDD: "hdd", MediaNetFS: "netfs", MediaUnknown: "unknown",
	}
	for m, want := range cases {
		if got := m.String(); got != want {
			t.Fatalf("MediaClass(%d).String() = %q, want %q", int(m), got, want)
		}
	}
}
