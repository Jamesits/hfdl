package transfer

import (
	"context"
	"fmt"
	"sync"
)

// intervalTracker guards the live in-memory IntervalSet (flushed ranges
// only — RAM-buffered bytes are never added, so they are invisible to
// checkpoint snapshots) and counts mutations so idle checkpoints can skip
// the fsync+persist round.
type intervalTracker struct {
	mu      sync.Mutex
	set     IntervalSet
	changes uint64
}

func (t *intervalTracker) add(start, end int64) {
	t.mu.Lock()
	t.set.Add(start, end)
	t.changes++
	t.mu.Unlock()
}

// reset clears the set to empty for a fresh size. Used on entering the
// single-stream fallback: the ranged-mode partial intervals are not resumable
// there (no mid-file resume), so a later checkpoint must not persist them.
func (t *intervalTracker) reset(size int64) {
	t.mu.Lock()
	t.set = IntervalSet{size: size}
	t.changes++
	t.mu.Unlock()
}

func (t *intervalTracker) snapshot() (*IntervalSet, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.set.clone(), t.changes
}

// coversAll reports whether [0,total) is fully flushed (416 handling: a 416
// is benign exactly when the file is already complete per size + set).
func (t *intervalTracker) coversAll(total int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.set.contains(0, total)
}

// prefixCovered returns the length of the longest flushed prefix of
// [start,end) — the bytes a Tier C re-attempt may skip from the stream.
func (t *intervalTracker) prefixCovered(start, end int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	pos := start
	for _, v := range t.set.iv {
		if v.End <= pos {
			continue
		}
		if v.Start > pos {
			break
		}
		pos = v.End
		if pos >= end {
			break
		}
	}
	return pos - start
}

// checkpointer sequences the durable checkpoint:
//  1. snapshot the in-memory interval set (flushed ranges only, by
//     construction);
//  2. fsync the cache file — this is what makes the snapshot durable;
//  3. persist the marshaled snapshot via the ProgressSink in one tx.
//
// The order is the durability invariant: anything in the persisted blob was
// fsynced first, so the durable record can only under-report. fsyncFn and
// persistFn are seams so tests can assert call order without a real file.
type checkpointer struct {
	mu        sync.Mutex
	tracker   *intervalTracker
	fsyncFn   func() error
	persistFn func(ctx context.Context, blob []byte) error
	onPersist func(fsyncedBytes int64)

	lastChanges uint64
	haveLast    bool
}

// checkpoint persists progress when the set changed since the last
// checkpoint (or always, when forced — file completion/shutdown).
func (c *checkpointer) checkpoint(ctx context.Context, force bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap, changes := c.tracker.snapshot()
	if !force && c.haveLast && changes == c.lastChanges {
		return nil
	}
	if err := c.fsyncFn(); err != nil {
		return fmt.Errorf("transfer: checkpoint fsync: %w", err)
	}
	blob, err := snap.MarshalBinary()
	if err != nil {
		return err
	}
	if err := c.persistFn(ctx, blob); err != nil {
		return fmt.Errorf("transfer: checkpoint persist: %w", err)
	}
	c.lastChanges = changes
	c.haveLast = true
	if c.onPersist != nil {
		c.onPersist(snap.total())
	}
	return nil
}
