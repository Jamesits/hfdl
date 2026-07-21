package sched

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/transfer"
)

// errGiveUp terminates a file's transfer run after a block exhausted its
// retry budget: give up at 8. The file is already in 'error' by the
// time Lease surfaces it.
var errGiveUp = errors.New("sched: block retry budget exhausted, file errored")

// maxLeaseWait caps one backoff-hiding sleep inside Lease (re-poll
// cadence); the wait is also cut short by ctx and by a peer's Requeue.
const maxLeaseWait = 5 * time.Second

// leaseRepoll is the re-poll delay when a pending row lost a claim race
// (available_at NULL yet LeaseBlocks saw nothing).
const leaseRepoll = 20 * time.Millisecond

// blockLeaser adapts the store to transfer.BlockLeaser for one file. It
// tracks the per-block lease tokens transfer never sees, translates
// completion into the downloading→downloaded transition, and applies the
// durable retry policy (available_at backoff, give-up at maxBlockRetries).
type blockLeaser struct {
	m       *Manager
	fileID  int64
	fileTok store.LeaseToken

	mu     sync.Mutex
	tokens map[int64]store.LeaseToken

	wake chan struct{} // Requeue signals waiters to re-evaluate availability

	gaveUp atomic.Bool
}

func newBlockLeaser(m *Manager, fileID int64, fileTok store.LeaseToken) *blockLeaser {
	return &blockLeaser{
		m:       m,
		fileID:  fileID,
		fileTok: fileTok,
		tokens:  make(map[int64]store.LeaseToken),
		wake:    make(chan struct{}, 1),
	}
}

// Lease claims one pending block of the file. ok=false means the file has
// NO pending blocks at all; blocks that merely sit in their available_at
// backoff are waited out (capped, ctx-cancelable) instead of being
// reported as "no work" — reporting them would strand the file in
// 'downloading' until lease expiry because transfer exits when every
// worker sees ok=false with nothing in flight.
func (l *blockLeaser) Lease(ctx context.Context, fileID int64) (transfer.Block, bool, error) {
	for {
		if l.gaveUp.Load() {
			return transfer.Block{}, false, errGiveUp
		}
		lbs, err := l.m.st.LeaseBlocks(ctx, 1, store.BlockFilter{FileIDs: []int64{fileID}}, l.m.nowFn())
		if err != nil {
			return transfer.Block{}, false, fmt.Errorf("sched: lease blocks file %d: %w", fileID, err)
		}
		if len(lbs) > 0 {
			b := lbs[0]
			l.mu.Lock()
			l.tokens[b.ID] = b.Token
			l.mu.Unlock()
			return transfer.Block{ID: b.ID, Idx: b.Idx, Offset: b.Offset, Length: b.Length}, true, nil
		}

		pending, earliest, err := l.pendingState(ctx)
		if err != nil {
			return transfer.Block{}, false, err
		}
		if pending == 0 {
			return transfer.Block{}, false, nil
		}

		wait := maxLeaseWait
		switch earliest {
		case nil:
			// Pending row mid-claim race: re-poll quickly.
			wait = leaseRepoll
		default:
			if d := earliest.Sub(l.m.nowFn()); d < wait {
				wait = max(d, 0)
			}
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return transfer.Block{}, false, ctx.Err()
		case <-l.wake:
			t.Stop()
		case <-t.C:
		}
	}
}

// pendingState reports the file's pending-block count and the earliest
// backoff expiry (nil when some pending row is available now). Scanned
// through bun: modernc returns TIMESTAMP columns as strings in raw
// database/sql queries, and bun's conversion handles them.
func (l *blockLeaser) pendingState(ctx context.Context) (int64, *time.Time, error) {
	var probe struct {
		Pending  int64      `bun:"pending"`
		Earliest *time.Time `bun:"earliest"`
	}
	err := l.m.st.DB().NewRaw(
		"SELECT COUNT(*) AS pending, MIN(available_at) AS earliest FROM blocks WHERE file_id = ? AND status = ?",
		l.fileID, string(store.BlockPending)).Scan(ctx, &probe)
	if err != nil {
		return 0, nil, fmt.Errorf("sched: pending blocks probe file %d: %w", l.fileID, err)
	}
	return probe.Pending, probe.Earliest, nil
}

// Complete marks a block done; when it was the file's last live block the
// file transitions downloading → downloaded in the same call path (the
// store asserts zero live block leases inside that tx).
func (l *blockLeaser) Complete(ctx context.Context, b transfer.Block) error {
	tok := l.pop(b.ID)
	var fileDone bool
	err := l.m.storeCall(ctx, func() error {
		var cerr error
		fileDone, cerr = l.m.st.CompleteBlock(ctx, b.ID, tok)
		return cerr
	})
	if err != nil {
		return err // fenced or DB failure: the run must abort
	}
	if !fileDone {
		return nil
	}
	err = l.m.storeCall(ctx, func() error {
		return l.m.st.TransitionFile(ctx, l.fileID, l.fileTok, store.FileDownloading, store.FileDownloaded, nil)
	})
	if err != nil && !errors.Is(err, store.ErrFenced) {
		return fmt.Errorf("sched: transition file %d to downloaded: %w", l.fileID, err)
	}
	if err == nil {
		l.m.log.Debug("file download complete", "file_id", l.fileID)
		wake(l.m.wakeDisk)
	}
	return nil
}

// Requeue hands a failed block back with durable backoff; at the 8-retry
// bound the file goes terminal-error and the run is failed via errGiveUp.
func (l *blockLeaser) Requeue(ctx context.Context, b transfer.Block, backoff time.Duration, cause error) error {
	l.m.noteIOError(ctx, cause) // ENOSPC: global download+install pause
	tok := l.pop(b.ID)
	retries, err := l.m.st.RequeueBlock(ctx, b.ID, tok, l.m.nowFn().Add(backoff), cause)
	if err != nil {
		return err
	}
	if retries < maxBlockRetries {
		// A new pending row may be available before the earliest a waiter
		// computed: nudge it to re-evaluate.
		select {
		case l.wake <- struct{}{}:
		default:
		}
		return nil
	}
	l.gaveUp.Store(true)
	dctx := l.m.detachedCtxOr(ctx)
	terr := fmt.Errorf("sched: file %d: %w", l.fileID, errGiveUp)
	if terr2 := l.m.storeCall(dctx, func() error {
		return l.m.st.TransitionFile(dctx, l.fileID, l.fileTok, store.FileDownloading, store.FileError, cause)
	}); terr2 != nil && !errors.Is(terr2, store.ErrFenced) {
		l.m.log.Warn("transition to error after give-up failed", "file_id", l.fileID, "err", terr2)
	}
	if ferr := l.m.storeCall(dctx, func() error {
		return l.m.st.FailJobFilesForFile(dctx, l.fileID, terr)
	}); ferr != nil {
		l.m.log.Warn("fail job files after give-up failed", "file_id", l.fileID, "err", ferr)
	}
	l.m.filesFailed.Add(dctx, 1)
	l.m.recordError(terr)
	return errGiveUp
}

// renew heartbeats every active block lease (the file lease is renewed by
// the download worker itself).
func (l *blockLeaser) renew(ctx context.Context, until time.Time) {
	l.mu.Lock()
	pending := make(map[int64]store.LeaseToken, len(l.tokens))
	for id, tok := range l.tokens {
		pending[id] = tok
	}
	l.mu.Unlock()
	for id, tok := range pending {
		// Individual fencing is ignored: a reclaimed block's late writes
		// are fenced server-side anyway.
		_ = l.m.st.RenewLease(ctx, store.LeaseBlock, id, tok, until)
	}
}

func (l *blockLeaser) pop(blockID int64) store.LeaseToken {
	l.mu.Lock()
	defer l.mu.Unlock()
	tok := l.tokens[blockID]
	delete(l.tokens, blockID)
	return tok
}
