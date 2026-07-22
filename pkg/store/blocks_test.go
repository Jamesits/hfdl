package store

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLeaseBlocksRequiresDownloadingParent(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 100, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	leaseFile(t, s, fileID, 1)
	if _, err := s.db.ExecContext(ctx, "UPDATE files SET status = ?, lease_token = NULL, lease_until = NULL WHERE id = ?", string(FileQueued), fileID); err != nil {
		t.Fatal(err)
	}
	leased, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if err != nil || len(leased) != 0 {
		t.Fatalf("LeaseBlocks with queued parent = (%d, %v), want empty", len(leased), err)
	}
}

func TestLeaseBlocksPerBlockTokens(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 300, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	leaseFile(t, s, fileID, 3)

	now := time.Now()
	leased, err := s.LeaseBlocks(ctx, 2, BlockFilter{FileIDs: []int64{fileID}}, now)
	if err != nil {
		t.Fatalf("LeaseBlocks: %v", err)
	}
	if len(leased) != 2 {
		t.Fatalf("leased %d, want 2 (n caps the batch)", len(leased))
	}
	seen := map[LeaseToken]bool{}
	for _, lb := range leased {
		if lb.Token == "" || seen[lb.Token] {
			t.Fatalf("block %d token %q not unique", lb.ID, lb.Token)
		}
		seen[lb.Token] = true
		if lb.Status != BlockActive {
			t.Errorf("block %d status = %s, want active", lb.ID, lb.Status)
		}
		if lb.LeaseUntil == nil || !lb.LeaseUntil.After(now) {
			t.Errorf("block %d lease_until = %v", lb.ID, lb.LeaseUntil)
		}
		if lb.Offset != int64(lb.Idx)*100 || lb.Length != 100 {
			t.Errorf("block %d geometry = (%d, %d)", lb.ID, lb.Offset, lb.Length)
		}
	}

	// Remaining block leasable; exhausted afterwards.
	rest, err := s.LeaseBlocks(ctx, 10, BlockFilter{FileIDs: []int64{fileID}}, now)
	if err != nil || len(rest) != 1 {
		t.Fatalf("LeaseBlocks rest = (%d, %v), want 1", len(rest), err)
	}
	none, err := s.LeaseBlocks(ctx, 10, BlockFilter{FileIDs: []int64{fileID}}, now)
	if err != nil || len(none) != 0 {
		t.Fatalf("LeaseBlocks exhausted = (%d, %v), want empty", len(none), err)
	}
}

func TestLeaseBlocksFilterAndBackoff(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{
		{Path: "a", Size: 200, GitOID: "g"},
		{Path: "b", Size: 200, GitOID: "g"},
	})
	aID := mustFileID(t, s, repoID, "a")
	bID := mustFileID(t, s, repoID, "b")
	leaseFile(t, s, aID, 2)
	leaseFile(t, s, bID, 2)

	// File filter: only a's blocks.
	leased, err := s.LeaseBlocks(ctx, 10, BlockFilter{FileIDs: []int64{aID}}, time.Now())
	if err != nil || len(leased) != 2 {
		t.Fatalf("LeaseBlocks filter = (%d, %v), want 2", len(leased), err)
	}
	for _, lb := range leased {
		if lb.FileID != aID {
			t.Fatalf("leased block of file %d under filter for %d", lb.FileID, aID)
		}
	}

	// MaxPerFile caps per file across the active set.
	leased, err = s.LeaseBlocks(ctx, 10, BlockFilter{MaxPerFile: map[int64]int{bID: 1}}, time.Now())
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseBlocks MaxPerFile = (%d, %v), want 1", len(leased), err)
	}

	// Backed-off block (available_at in the future) is skipped until due.
	if _, err := s.db.ExecContext(ctx,
		"UPDATE blocks SET status = ?, available_at = ? WHERE file_id = ? AND idx = 1",
		string(BlockPending), time.Now().Add(time.Hour).UTC(), bID); err != nil {
		t.Fatal(err)
	}
	leased, err = s.LeaseBlocks(ctx, 10, BlockFilter{FileIDs: []int64{bID}}, time.Now())
	if err != nil || len(leased) != 0 {
		t.Fatalf("LeaseBlocks backoff = (%d, %v), want 0", len(leased), err)
	}
	leased, err = s.LeaseBlocks(ctx, 10, BlockFilter{FileIDs: []int64{bID}}, time.Now().Add(2*time.Hour))
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseBlocks past backoff = (%d, %v), want 1", len(leased), err)
	}
}

func TestCompleteBlockFileDone(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 200, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	leaseFile(t, s, fileID, 2)

	leased, err := s.LeaseBlocks(ctx, 2, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if err != nil || len(leased) != 2 {
		t.Fatalf("LeaseBlocks: %v (%d)", err, len(leased))
	}
	done, err := s.CompleteBlock(ctx, leased[0].ID, leased[0].Token)
	if err != nil || done {
		t.Fatalf("first CompleteBlock = (%v, %v), want (false, nil)", done, err)
	}
	done, err = s.CompleteBlock(ctx, leased[1].ID, leased[1].Token)
	if err != nil || !done {
		t.Fatalf("last CompleteBlock = (%v, %v), want (true, nil)", done, err)
	}

	// Replay with the same token: fenced.
	if _, err := s.CompleteBlock(ctx, leased[1].ID, leased[1].Token); !errors.Is(err, ErrFenced) {
		t.Fatalf("replay CompleteBlock err = %v, want ErrFenced", err)
	}
}

func TestRequeueBlock(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 200, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	leaseFile(t, s, fileID, 1)

	leased, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseBlocks: %v (%d)", err, len(leased))
	}
	block := leased[0]

	backoff := time.Now().Add(30 * time.Second)
	retries, err := s.RequeueBlock(ctx, block.ID, block.Token, backoff, errors.New("boom 503"))
	if err != nil {
		t.Fatalf("RequeueBlock: %v", err)
	}
	if retries != 1 {
		t.Errorf("retries = %d, want 1", retries)
	}
	var st, lastErr string
	var availAt time.Time
	var leaseToken *string
	if err := s.db.QueryRowContext(ctx,
		"SELECT status, available_at, last_error, lease_token FROM blocks WHERE id = ?", block.ID).
		Scan(&st, &availAt, &lastErr, &leaseToken); err != nil {
		t.Fatal(err)
	}
	if st != string(BlockPending) {
		t.Errorf("status = %s, want pending", st)
	}
	if availAt.UTC().Sub(backoff.UTC()).Abs() > time.Second {
		t.Errorf("available_at = %v, want ~%v", availAt, backoff)
	}
	if lastErr != "boom 503" {
		t.Errorf("last_error = %q", lastErr)
	}
	if leaseToken != nil {
		t.Errorf("lease_token = %v, want cleared", leaseToken)
	}

	// Backed-off row is skipped until due, then re-leased and requeued again.
	if got, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now()); err != nil || len(got) != 0 {
		t.Fatalf("backed-off LeaseBlocks = (%d, %v), want empty", len(got), err)
	}
	leased, err = s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, backoff.Add(time.Second))
	if err != nil || len(leased) != 1 {
		t.Fatalf("re-LeaseBlocks: %v (%d)", err, len(leased))
	}
	retries, err = s.RequeueBlock(ctx, block.ID, leased[0].Token, backoff, nil)
	if err != nil || retries != 2 {
		t.Fatalf("second RequeueBlock = (%d, %v), want (2, nil)", retries, err)
	}
	var lastErrPtr *string
	if err := s.db.QueryRowContext(ctx, "SELECT last_error FROM blocks WHERE id = ?", block.ID).Scan(&lastErrPtr); err != nil {
		t.Fatal(err)
	}
	if lastErrPtr != nil {
		t.Errorf("last_error = %q, want NULL after nil-cause requeue", *lastErrPtr)
	}

	// Wrong token: fenced, row untouched.
	if _, err := s.RequeueBlock(ctx, block.ID, "01JWRONGTOKEN000000000000", backoff, nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("RequeueBlock wrong token err = %v, want ErrFenced", err)
	}
	var retriesDB int
	if err := s.db.QueryRowContext(ctx, "SELECT retries FROM blocks WHERE id = ?", block.ID).Scan(&retriesDB); err != nil {
		t.Fatal(err)
	}
	if retriesDB != 2 {
		t.Errorf("retries = %d after fenced write, want 2 (untouched)", retriesDB)
	}
}

// TestLeaseBlocksConcurrent hammers the same file from 16 goroutines: every
// block must be leased exactly once.
func TestLeaseBlocksConcurrent(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	const workers = 16
	const blocks = 64
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "shared", Size: blocks * 100, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "shared")
	leaseFile(t, s, fileID, blocks)

	results := make([][]LeasedBlock, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				leased, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
				if err != nil {
					errs[i] = err
					return
				}
				results[i] = append(results[i], leased...)
				if len(leased) == 0 {
					return
				}
			}
		}(i)
	}
	wg.Wait()

	ids := map[int64]bool{}
	toks := map[LeaseToken]bool{}
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		for _, lb := range results[i] {
			if ids[lb.ID] {
				t.Fatalf("block %d leased twice", lb.ID)
			}
			ids[lb.ID] = true
			if toks[lb.Token] {
				t.Fatalf("token %q issued twice", lb.Token)
			}
			toks[lb.Token] = true
		}
	}
	if len(ids) != blocks {
		t.Fatalf("leased %d blocks total, want %d", len(ids), blocks)
	}

	// Everything claimed: nothing pending remains.
	var pending int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM blocks WHERE status = ?", string(BlockPending)).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Errorf("%d blocks still pending", pending)
	}
}
