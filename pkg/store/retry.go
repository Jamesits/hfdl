package store

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"
	"modernc.org/sqlite"
)

// SQLITE_BUSY_SNAPSHOT (517) is the one failure busy_timeout cannot fix: a
// deferred tx that read before its first write holds a stale snapshot once
// another writer commits, and every write then fails until rollback. Two
// defenses, per investigation under concurrent workers:
//
//  1. _txlock=immediate in the DSN (see store.go): every tx takes the write
//     lock at BEGIN, so a snapshot can never go stale mid-tx; contention
//     waits on busy_timeout at BEGIN instead of failing later.
//  2. withBusyRetry below: bounded retry with jittered backoff around whole
//     tx bodies (fn is re-executed after rollback — bodies must rebuild all
//     statements inside fn, never reuse them across attempts).

const (
	// busyMaxAttempts bounds whole-tx retries on SQLITE_BUSY*.
	busyMaxAttempts = 5
	// busyBaseBackoff doubles per attempt (10, 20, 40, 80ms) plus jitter.
	busyBaseBackoff = 10 * time.Millisecond
	// sqliteBusyPrimary is the primary result code shared by every
	// SQLITE_BUSY variant; extended codes (BUSY_SNAPSHOT 517, ...) carry it
	// in the low byte. Hardcoded to avoid importing modernc.org/sqlite/lib.
	sqliteBusyPrimary = 5
)

// isBusyErr reports whether err is any SQLITE_BUSY variant. The modernc
// driver enables extended result codes, so Code() may be 517 & friends.
func isBusyErr(err error) bool {
	var se *sqlite.Error
	if errors.As(err, &se) {
		return se.Code()&0xff == sqliteBusyPrimary
	}
	return false
}

func jitter(d time.Duration) time.Duration {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return d
	}
	return d + time.Duration(binary.LittleEndian.Uint64(b[:])%uint64(d))
}

// withBusyRetry runs fn, retrying the whole body on SQLITE_BUSY* with
// bounded jittered backoff. Cheap insurance for any path that escapes the
// immediate txlock (driver quirks, nested savepoints, future code).
func withBusyRetry(ctx context.Context, fn func() error) error {
	backoff := busyBaseBackoff
	var err error
	for attempt := 0; attempt < busyMaxAttempts; attempt++ {
		if err = fn(); !isBusyErr(err) {
			return err
		}
		if attempt == busyMaxAttempts-1 {
			break
		}
		t := time.NewTimer(jitter(backoff))
		select {
		case <-ctx.Done():
			t.Stop()
			return errors.Join(err, ctx.Err())
		case <-t.C:
		}
		backoff *= 2
	}
	return err
}

// inTx runs fn inside a transaction with whole-body busy retry. fn must be
// re-executable: it runs again from scratch after a busy rollback.
func (s *Store) inTx(ctx context.Context, op string, fn func(tx bun.Tx) error) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("store: %s: begin: %w", op, err)
		}
		if err := fn(tx); err != nil {
			tx.Rollback() //nolint:errcheck // best-effort
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("store: %s: commit: %w", op, err)
		}
		return nil
	})
}
