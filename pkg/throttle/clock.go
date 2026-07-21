package throttle

import (
	"context"
	"time"
)

// clock abstracts the time source so tests can drive accounting
// deterministically. Real pacing (Bucket wait timers, the default sleeper)
// still uses the runtime timer wheel.
type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// sleeper is the DutyLimiter sleep seam: tests substitute a fake that
// records slice durations and advances the fake clock instead of blocking.
type sleeper func(ctx context.Context, d time.Duration) error

// sleepCtx is the production sleeper: ctx-cancelable, like FastCopy's
// isAbort-checked ::Sleep loop.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
