package sched

import (
	"context"
	"errors"
	"strings"
	"time"
)

// WAL write contention: a deferred store transaction that reads before it
// writes can lose its snapshot to a concurrent writer and surface
// SQLITE_BUSY_SNAPSHOT (517), which busy_timeout cannot absorb — the
// transaction must be retried. storeCall wraps every store mutation sched
// issues from concurrent workers with exactly that retry.
const (
	busyRetries    = 24
	busyBackoffMin = 20 * time.Millisecond
	busyBackoffMax = 400 * time.Millisecond
)

// isBusy reports whether err is a SQLite busy/locked condition (plain
// SQLITE_BUSY or BUSY_SNAPSHOT), wrapped at any depth.
func isBusy(err error) bool {
	if err == nil {
		return false
	}
	var coder interface{ Code() int }
	if errors.As(err, &coder) {
		if coder.Code()&0xff == 5 { // SQLITE_BUSY extended codes
			return true
		}
	}
	return strings.Contains(err.Error(), "database is locked")
}

// storeCall runs fn, retrying SQLite-busy failures with capped exponential
// backoff. The store's guards make every mutation idempotent or fenced, so
// a retry after a committed-but-unacknowledged write is safe (it re-runs
// the same guarded statement, which either re-applies or fences).
func (m *Manager) storeCall(ctx context.Context, fn func() error) error {
	var err error
	backoff := busyBackoffMin
	for range busyRetries {
		err = fn()
		if !isBusy(err) {
			return err
		}
		if ctx.Err() != nil {
			return err
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return err
		}
		backoff *= 2
		if backoff > busyBackoffMax {
			backoff = busyBackoffMax
		}
	}
	return err
}
