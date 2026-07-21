package store

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestTransitionFileLiveLeaseRejected(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 200, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	tok := leaseFile(t, s, fileID, 2)

	// One block still leased: transition must fail.
	leased, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseBlocks: %v (%d)", err, len(leased))
	}
	err = s.TransitionFile(ctx, fileID, tok, FileDownloading, FileDownloaded, nil)
	if !errors.Is(err, ErrLiveBlockLeases) {
		t.Fatalf("TransitionFile with live lease err = %v, want ErrLiveBlockLeases", err)
	}
	if st := fileStatus(t, s, fileID); st != FileDownloading {
		t.Fatalf("file status = %s after rejected transition, want downloading", st)
	}

	// Complete only the leased block: the file has a second, still-pending
	// block, so it is NOT byte-complete and the transition must be rejected —
	// downloaded means every block done, not just the leased ones.
	if _, err := s.CompleteBlock(ctx, leased[0].ID, leased[0].Token); err != nil {
		t.Fatal(err)
	}
	if err := s.TransitionFile(ctx, fileID, tok, FileDownloading, FileDownloaded, nil); !errors.Is(err, ErrBlocksPending) {
		t.Fatalf("TransitionFile with a pending block err = %v, want ErrBlocksPending", err)
	}
	if st := fileStatus(t, s, fileID); st != FileDownloading {
		t.Fatalf("file status = %s after rejected transition, want downloading", st)
	}

	// Finish the remaining block: now every block is done and the transition
	// succeeds and ends the lease.
	rest, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if err != nil || len(rest) != 1 {
		t.Fatalf("LeaseBlocks (remaining): %v (%d)", err, len(rest))
	}
	if _, err := s.CompleteBlock(ctx, rest[0].ID, rest[0].Token); err != nil {
		t.Fatal(err)
	}
	if err := s.TransitionFile(ctx, fileID, tok, FileDownloading, FileDownloaded, nil); err != nil {
		t.Fatalf("TransitionFile: %v", err)
	}
	if st := fileStatus(t, s, fileID); st != FileDownloaded {
		t.Fatalf("file status = %s, want downloaded", st)
	}
	// Lease ended: the old token no longer owns the row.
	if err := s.TransitionFile(ctx, fileID, tok, FileDownloaded, FileCached, nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("post-transition use of old token err = %v, want ErrFenced", err)
	}
}

func TestVerifyLifecycle(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 100, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	tok := leaseFile(t, s, fileID, 1)
	leased, _ := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if _, err := s.CompleteBlock(ctx, leased[0].ID, leased[0].Token); err != nil {
		t.Fatal(err)
	}
	if err := s.TransitionFile(ctx, fileID, tok, FileDownloading, FileDownloaded, nil); err != nil {
		t.Fatal(err)
	}

	f, vtok, err := s.LeaseVerify(ctx, time.Now())
	if err != nil {
		t.Fatalf("LeaseVerify: %v", err)
	}
	if f.ID != fileID || f.Status != FileVerifying || vtok == "" {
		t.Fatalf("LeaseVerify = (%+v, %q)", f, vtok)
	}
	// Nothing else to verify.
	if _, _, err := s.LeaseVerify(ctx, time.Now()); !errors.Is(err, ErrNoWork) {
		t.Fatalf("LeaseVerify 2 err = %v, want ErrNoWork", err)
	}
	// Wrong token: fenced.
	if err := s.CompleteVerify(ctx, fileID, "01JWRONGTOKEN000000000000", "/cache/blobs/x"); !errors.Is(err, ErrFenced) {
		t.Fatalf("CompleteVerify wrong token err = %v, want ErrFenced", err)
	}
	if err := s.CompleteVerify(ctx, fileID, vtok, "/cache/blobs/x"); err != nil {
		t.Fatalf("CompleteVerify: %v", err)
	}
	if st := fileStatus(t, s, fileID); st != FileCached {
		t.Fatalf("file status = %s, want cached", st)
	}
	var cachePath string
	if err := s.db.QueryRowContext(ctx, "SELECT cache_path FROM files WHERE id = ?", fileID).Scan(&cachePath); err != nil {
		t.Fatal(err)
	}
	if cachePath != "/cache/blobs/x" {
		t.Errorf("cache_path = %q", cachePath)
	}
}

func TestFailVerifyResetThenError(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 100, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")

	failOnce := func() {
		t.Helper()
		tok := leaseFile(t, s, fileID, 1)
		if err := s.SaveProgress(ctx, fileID, tok, []byte{0xDE, 0xAD}); err != nil {
			t.Fatal(err)
		}
		leased, _ := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
		if _, err := s.CompleteBlock(ctx, leased[0].ID, leased[0].Token); err != nil {
			t.Fatal(err)
		}
		if err := s.TransitionFile(ctx, fileID, tok, FileDownloading, FileDownloaded, nil); err != nil {
			t.Fatal(err)
		}
		_, vtok, err := s.LeaseVerify(ctx, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := s.FailVerify(ctx, fileID, vtok, errors.New("hash mismatch")); err != nil {
			t.Fatal(err)
		}
	}

	failOnce() // first failure: requeue
	if st := fileStatus(t, s, fileID); st != FileQueued {
		t.Fatalf("after fail 1: status = %s, want queued", st)
	}
	var fails, blocksPending int
	var progress []byte
	if err := s.db.QueryRowContext(ctx,
		"SELECT verify_fails, progress FROM files WHERE id = ?", fileID).Scan(&fails, &progress); err != nil {
		t.Fatal(err)
	}
	if fails != 1 || progress != nil {
		t.Errorf("after fail 1: verify_fails=%d progress=%v, want 1/nil (untrusted bytes dropped)", fails, progress)
	}
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM blocks WHERE file_id = ? AND status = ?", fileID, string(BlockPending)).Scan(&blocksPending); err != nil {
		t.Fatal(err)
	}
	if blocksPending != 1 {
		t.Errorf("after fail 1: pending blocks = %d, want 1 (reset)", blocksPending)
	}

	failOnce() // second failure: terminal
	if st := fileStatus(t, s, fileID); st != FileError {
		t.Fatalf("after fail 2: status = %s, want error", st)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT verify_fails FROM files WHERE id = ?", fileID).Scan(&fails); err != nil {
		t.Fatal(err)
	}
	if fails != maxVerifyFails {
		t.Errorf("verify_fails = %d, want %d", fails, maxVerifyFails)
	}
}

func TestSaveLoadProgress(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 100, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")

	// Nothing saved yet: nil blob.
	blob, err := s.LoadProgress(ctx, fileID)
	if err != nil || blob != nil {
		t.Fatalf("LoadProgress empty = (%v, %v)", blob, err)
	}

	tok := leaseFile(t, s, fileID, 1)
	want := []byte{1, 2, 3, 4}
	if err := s.SaveProgress(ctx, fileID, tok, want); err != nil {
		t.Fatalf("SaveProgress: %v", err)
	}
	blob, err = s.LoadProgress(ctx, fileID)
	if err != nil || !bytes.Equal(blob, want) {
		t.Fatalf("LoadProgress = (%v, %v)", blob, err)
	}
	var ver int
	if err := s.db.QueryRowContext(ctx, "SELECT progress_ver FROM files WHERE id = ?", fileID).Scan(&ver); err != nil {
		t.Fatal(err)
	}
	if ver != 2 {
		t.Errorf("progress_ver = %d, want 2 (bumped on save)", ver)
	}

	// Wrong token: fenced, blob untouched.
	if err := s.SaveProgress(ctx, fileID, "01JWRONGTOKEN000000000000", []byte{9}); !errors.Is(err, ErrFenced) {
		t.Fatalf("SaveProgress wrong token err = %v, want ErrFenced", err)
	}
	blob, _ = s.LoadProgress(ctx, fileID)
	if !bytes.Equal(blob, want) {
		t.Errorf("blob changed by fenced write: %v", blob)
	}
}

func TestNextDownloadableFilesCommitGate(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	// Listing completed but commit_sha cleared afterwards (unresolved rev).
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{
		{Path: "a", Size: 1, GitOID: "g"},
		{Path: "b", Size: 1, GitOID: "g"},
	})
	if _, err := s.db.ExecContext(ctx, "UPDATE repos SET commit_sha = NULL WHERE id = ?", repoID); err != nil {
		t.Fatal(err)
	}
	files, err := s.NextDownloadableFiles(ctx, 10)
	if err != nil || len(files) != 0 {
		t.Fatalf("NextDownloadableFiles without sha = (%d, %v), want 0", len(files), err)
	}
	if _, err := s.db.ExecContext(ctx, "UPDATE repos SET commit_sha = 'abc' WHERE id = ?", repoID); err != nil {
		t.Fatal(err)
	}
	files, err = s.NextDownloadableFiles(ctx, 1)
	if err != nil || len(files) != 1 {
		t.Fatalf("NextDownloadableFiles limit = (%d, %v), want 1", len(files), err)
	}
	if files[0].Path != "a" {
		t.Errorf("first downloadable = %s, want a (ORDER BY id)", files[0].Path)
	}
	files, err = s.NextDownloadableFiles(ctx, 10)
	if err != nil || len(files) != 2 {
		t.Fatalf("NextDownloadableFiles = (%d, %v), want 2", len(files), err)
	}
}

func TestLeaseFileForDownloadNotQueued(t *testing.T) {
	s := openTestStore(t)
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 1, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	leaseFile(t, s, fileID, 1)
	// Already downloading: second lease is fenced.
	if _, err := s.LeaseFileForDownload(t.Context(), fileID, time.Now()); !errors.Is(err, ErrFenced) {
		t.Fatalf("re-lease err = %v, want ErrFenced", err)
	}
}

func TestReplacePendingBlocks(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 400, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	tok := leaseFile(t, s, fileID, 2)

	// One block done: re-chunking keeps it, replaces only pending.
	leased, _ := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if _, err := s.CompleteBlock(ctx, leased[0].ID, leased[0].Token); err != nil {
		t.Fatal(err)
	}
	rechunk := []Block{{Idx: 1, Offset: 100, Length: 50}, {Idx: 2, Offset: 150, Length: 50}, {Idx: 3, Offset: 200, Length: 200}}
	if err := s.ReplacePendingBlocks(ctx, fileID, tok, rechunk); err != nil {
		t.Fatalf("ReplacePendingBlocks: %v", err)
	}
	var pending, done int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM blocks WHERE file_id = ? AND status = ?", fileID, string(BlockPending)).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM blocks WHERE file_id = ? AND status = ?", fileID, string(BlockDone)).Scan(&done); err != nil {
		t.Fatal(err)
	}
	if pending != 3 || done != 1 {
		t.Errorf("blocks after re-chunk: pending=%d done=%d, want 3/1", pending, done)
	}

	// Wrong state: cached file refuses re-chunking.
	if _, err := s.db.ExecContext(ctx, "UPDATE files SET status = ? WHERE id = ?", string(FileCached), fileID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplacePendingBlocks(ctx, fileID, tok, rechunk); !errors.Is(err, ErrFenced) {
		t.Fatalf("ReplacePendingBlocks on cached err = %v, want ErrFenced", err)
	}
}

func TestMarkSalvagingTokenlessFlow(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{
		{Path: "lfs.bin", Size: 100, GitOID: "g", SHA256: "s", IsLFS: true},
		{Path: "small.txt", Size: 10, GitOID: "g"},
	})
	lfsID := mustFileID(t, s, repoID, "lfs.bin")

	targets, err := s.SalvageableTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].ID != lfsID {
		t.Fatalf("SalvageableTargets = %+v, want only lfs.bin", targets)
	}

	if err := s.MarkSalvaging(ctx, lfsID); err != nil {
		t.Fatalf("MarkSalvaging: %v", err)
	}
	if st := fileStatus(t, s, lfsID); st != FileSalvaging {
		t.Fatalf("status = %s, want salvaging", st)
	}
	// Atomic claim: a second marker is fenced.
	if err := s.MarkSalvaging(ctx, lfsID); !errors.Is(err, ErrFenced) {
		t.Fatalf("second MarkSalvaging err = %v, want ErrFenced", err)
	}

	// Salvage-apply: tokenless progress save, then re-enter the machine at
	// downloaded so the verifier still hashes the copied bytes.
	if err := s.SaveProgress(ctx, lfsID, "", []byte{0xFF}); err != nil {
		t.Fatalf("tokenless SaveProgress on salvaging: %v", err)
	}
	if err := s.TransitionFile(ctx, lfsID, "", FileSalvaging, FileDownloaded, nil); err != nil {
		t.Fatalf("salvaging→downloaded: %v", err)
	}
	if _, _, err := s.LeaseVerify(ctx, time.Now()); err != nil {
		t.Fatalf("salvaged file must be verifiable: %v", err)
	}
}
