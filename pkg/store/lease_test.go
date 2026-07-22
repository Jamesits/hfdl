package store

import (
	"errors"
	"testing"
	"time"
)

func TestRenewLease(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 1, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	tok := leaseFile(t, s, fileID, 1)

	until := time.Now().Add(5 * time.Minute)
	if err := s.RenewLease(ctx, LeaseFile, fileID, tok, until); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	var got time.Time
	if err := s.db.QueryRowContext(ctx, "SELECT lease_until FROM files WHERE id = ?", fileID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got.UTC().Equal(until.UTC().Truncate(time.Second)) && got.UTC().Sub(until.UTC()).Abs() > time.Second {
		t.Errorf("lease_until = %v, want ~%v", got, until)
	}

	// Wrong token: fenced, row untouched.
	if err := s.RenewLease(ctx, LeaseFile, fileID, "01JWRONGTOKEN000000000000", until); !errors.Is(err, ErrFenced) {
		t.Fatalf("RenewLease wrong token err = %v, want ErrFenced", err)
	}
	if _, err := s.db.ExecContext(ctx, "UPDATE files SET status = ? WHERE id = ?", string(FileQueued), fileID); err != nil {
		t.Fatal(err)
	}
	if err := s.RenewLease(ctx, LeaseFile, fileID, tok, until); !errors.Is(err, ErrFenced) {
		t.Fatalf("RenewLease after status change err = %v, want ErrFenced", err)
	}
}

func mustFileID(t *testing.T, s *Store, repoID int64, path string) int64 {
	t.Helper()
	var id int64
	if err := s.db.QueryRowContext(t.Context(),
		"SELECT id FROM files WHERE repo_id = ? AND path = ?", repoID, path).Scan(&id); err != nil {
		t.Fatalf("mustFileID %s: %v", path, err)
	}
	return id
}

// TestStaleLeaseFencing is the core fencing scenario: worker A's lease
// expires, Recover requeues the row, worker B claims it, and every late
// write from A is fenced to 0 rows.
func TestStaleLeaseFencing(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 200, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")

	now := time.Now()
	tokA, err := s.LeaseFileForDownload(ctx, fileID, now)
	if err != nil {
		t.Fatalf("LeaseFileForDownload: %v", err)
	}
	if err := s.ReplacePendingBlocks(ctx, fileID, tokA, []Block{{Idx: 0, Offset: 0, Length: 200}}); err != nil {
		t.Fatalf("ReplacePendingBlocks: %v", err)
	}
	leasedA, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, now)
	if err != nil || len(leasedA) != 1 {
		t.Fatalf("LeaseBlocks A: %v (%d)", err, len(leasedA))
	}
	blockA := leasedA[0]

	// Both leases expire; Recover requeues file and block.
	stats, err := s.Recover(ctx, now.Add(2*leaseDuration))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if stats.Files != 1 || stats.Blocks != 1 {
		t.Fatalf("Recover stats = %+v, want Files=1 Blocks=1", stats)
	}
	if st := fileStatus(t, s, fileID); st != FileQueued {
		t.Fatalf("file status after Recover = %s, want queued", st)
	}

	// B claims the block.
	tokB, err := s.LeaseFileForDownload(ctx, fileID, time.Now())
	if err != nil {
		t.Fatalf("LeaseFileForDownload B: %v", err)
	}
	leasedB, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if err != nil || len(leasedB) != 1 {
		t.Fatalf("LeaseBlocks B: %v (%d)", err, len(leasedB))
	}
	if leasedB[0].ID != blockA.ID {
		t.Fatalf("B leased block %d, want reclaimed %d", leasedB[0].ID, blockA.ID)
	}
	if leasedB[0].Token == blockA.Token {
		t.Fatal("B's token equals A's stale token")
	}

	// A's late writes are all fenced and affect 0 rows.
	if _, err := s.CompleteBlock(ctx, blockA.ID, blockA.Token); !errors.Is(err, ErrFenced) {
		t.Errorf("A CompleteBlock err = %v, want ErrFenced", err)
	}
	if err := s.SaveProgress(ctx, fileID, tokA, []byte{1}); !errors.Is(err, ErrFenced) {
		t.Errorf("A SaveProgress err = %v, want ErrFenced", err)
	}
	if err := s.TransitionFile(ctx, fileID, tokA, FileDownloading, FileDownloaded, nil); !errors.Is(err, ErrFenced) {
		t.Errorf("A TransitionFile err = %v, want ErrFenced", err)
	}
	if err := s.RenewLease(ctx, LeaseBlock, blockA.ID, blockA.Token, time.Now()); !errors.Is(err, ErrFenced) {
		t.Errorf("A RenewLease err = %v, want ErrFenced", err)
	}

	// B's claim is intact: still active, owned by B's token.
	var status, lt string
	if err := s.db.QueryRowContext(ctx,
		"SELECT status, lease_token FROM blocks WHERE id = ?", blockA.ID).Scan(&status, &lt); err != nil {
		t.Fatal(err)
	}
	if status != string(BlockActive) || lt != string(leasedB[0].Token) {
		t.Errorf("block = (%s, %s), want (active, B token)", status, lt)
	}

	// B finishes normally.
	done, err := s.CompleteBlock(ctx, leasedB[0].ID, leasedB[0].Token)
	if err != nil || !done {
		t.Errorf("B CompleteBlock = (%v, %v), want (true, nil)", done, err)
	}
	if err := s.TransitionFile(ctx, fileID, tokB, FileDownloading, FileDownloaded, nil); err != nil {
		t.Errorf("B TransitionFile: %v", err)
	}
}
