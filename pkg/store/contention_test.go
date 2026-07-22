package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// checkNotBusy fails the test for every unexpected error, distinguishing busy
// leaks in its diagnostic.
func checkNotBusy(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if isBusyErr(err) || strings.Contains(err.Error(), "database is locked") {
		t.Errorf("SQLITE_BUSY escaped to caller: %v", err)
		return
	}
	t.Errorf("unexpected store error: %v", err)
}

// TestBusyRetryAbsorbsSnapshot deterministically produces
// SQLITE_BUSY_SNAPSHOT (517) — a deferred tx reads (snapshot), another
// writer commits, the tx's write then fails — and proves withBusyRetry
// re-executes the body to success. This is the exact failure the
// _txlock=immediate DSN prevents and the retry insures against.
func TestBusyRetryAbsorbsSnapshot(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	seedListedRepo(t, s, "org/repo", nil)

	// A second connection WITHOUT _txlock=immediate: its txs stay deferred,
	// so a read-then-write body can go stale under a concurrent commit.
	raw, err := sql.Open(driverName, fileURI(s.path, "_pragma=busy_timeout(5000)"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	var attempts int
	var sawBusy bool
	err = withBusyRetry(ctx, func() error {
		attempts++
		tx, err := raw.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		var n int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM repos").Scan(&n); err != nil {
			_ = tx.Rollback()
			return err
		}
		if attempts == 1 {
			// Another writer commits between our read and our write:
			// the snapshot is now stale.
			if err := s.SetKV(ctx, "force-wal-writer", "1"); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, "UPDATE repos SET retries = retries"); err != nil {
			_ = tx.Rollback()
			if isBusyErr(err) {
				sawBusy = true
			}
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		t.Fatalf("withBusyRetry: %v", err)
	}
	if !sawBusy {
		t.Fatal("first attempt did not hit SQLITE_BUSY_SNAPSHOT — reproduction broken")
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (one busy, one clean)", attempts)
	}
}

// TestBusySnapshotContention reproduces the WAL write-contention finding:
// many workers running fenced multi-statement txs (CompleteBlock,
// RequeueBlock, TransitionFile) over one file's blocks. With
// _txlock=immediate + whole-tx busy retry, no 517/5 may escape and no row
// may strand: the file must reach downloaded in seconds.
func TestBusySnapshotContention(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	const nblocks = 64
	const workers = 8

	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: nblocks * 100, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	fileTok := leaseFile(t, s, fileID, nblocks)

	var done atomic.Int64
	var transitioned atomic.Bool
	var wg sync.WaitGroup

	// Block workers: lease one block, then complete or requeue it (50/50
	// churn keeps the write lock hot and snapshots contested).
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for done.Load() < nblocks {
				leased, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
				if err != nil {
					checkNotBusy(t, err)
					return
				}
				if len(leased) == 0 {
					if done.Load() >= nblocks {
						return
					}
					time.Sleep(time.Millisecond)
					continue
				}
				lb := leased[0]
				if (int(lb.ID)+w)%2 == 0 {
					fileDone, err := s.CompleteBlock(ctx, lb.ID, lb.Token)
					if err != nil {
						checkNotBusy(t, err)
						return
					}
					done.Add(1)
					_ = fileDone
				} else {
					// Requeue (immediately available) → someone retries it.
					if _, err := s.RequeueBlock(ctx, lb.ID, lb.Token, time.Now(), fmt.Errorf("churn")); err != nil {
						checkNotBusy(t, err)
						return
					}
				}
			}
		}(w)
	}

	// Transitioners hammer the fenced downloading→downloaded tx the whole
	// time; exactly one wins, the rest see live leases or fencing — never busy.
	for range 2 {
		wg.Go(func() {
			for done.Load() < nblocks && !transitioned.Load() {
				err := s.TransitionFile(ctx, fileID, fileTok, FileDownloading, FileDownloaded, nil)
				switch {
				case err == nil:
					transitioned.Store(true)
					return
				case errors.Is(err, ErrLiveBlockLeases), errors.Is(err, ErrBlocksPending), errors.Is(err, ErrFenced):
					time.Sleep(time.Millisecond)
				default:
					checkNotBusy(t, err)
					return
				}
			}
		})
	}

	wg.Wait()
	if !transitioned.Load() {
		// Workers drained all blocks without a transition win: finish now.
		if err := s.TransitionFile(ctx, fileID, fileTok, FileDownloading, FileDownloaded, nil); err != nil {
			t.Fatalf("final TransitionFile: %v", err)
		}
	}

	// No stranding: every block done, file downloaded, zero live leases.
	var pending, active int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FILTER (WHERE status = ?), COUNT(*) FILTER (WHERE status = ?) FROM blocks WHERE file_id = ?",
		string(BlockPending), string(BlockActive), fileID).Scan(&pending, &active); err != nil {
		t.Fatal(err)
	}
	if pending != 0 || active != 0 {
		t.Errorf("stranded blocks: pending=%d active=%d, want 0/0", pending, active)
	}
	if st := fileStatus(t, s, fileID); st != FileDownloaded {
		t.Errorf("file status = %s, want downloaded", st)
	}

	// Phase 2: fenced-out writers hammering a verifying file must not break
	// the owner's FailVerify tx (or surface busy themselves).
	_, vtok, err := s.LeaseVerify(ctx, time.Now())
	if err != nil {
		t.Fatalf("LeaseVerify: %v", err)
	}
	var stop atomic.Bool
	for w := range 4 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for !stop.Load() {
				err := s.CompleteVerify(ctx, fileID, "01JWRONGTOKEN000000000000", "/x")
				if err != nil && !errors.Is(err, ErrFenced) {
					checkNotBusy(t, err)
					return
				}
				if err := s.SaveProgress(ctx, fileID, "01JWRONGTOKEN000000000000", []byte{byte(w)}); err != nil && !errors.Is(err, ErrFenced) {
					checkNotBusy(t, err)
					return
				}
			}
		}(w)
	}
	if err := s.FailVerify(ctx, fileID, vtok, errors.New("hash mismatch")); err != nil {
		checkNotBusy(t, err)
	}
	stop.Store(true)
	wg.Wait()
	if st := fileStatus(t, s, fileID); st != FileQueued {
		t.Errorf("after FailVerify under contention: status = %s, want queued", st)
	}
}
