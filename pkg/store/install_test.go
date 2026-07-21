package store

import (
	"errors"
	"testing"
	"time"
)

// cacheFile drives one file through the whole download+verify pipeline.
func cacheFile(t *testing.T, s *Store, fileID int64) {
	t.Helper()
	ctx := t.Context()
	tok := leaseFile(t, s, fileID, 1)
	leased, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseBlocks: %v (%d)", err, len(leased))
	}
	if _, err := s.CompleteBlock(ctx, leased[0].ID, leased[0].Token); err != nil {
		t.Fatal(err)
	}
	if err := s.TransitionFile(ctx, fileID, tok, FileDownloading, FileDownloaded, nil); err != nil {
		t.Fatal(err)
	}
	var vtok LeaseToken
	for {
		f, v, err := s.LeaseVerify(ctx, time.Now())
		if err != nil {
			t.Fatalf("LeaseVerify: %v", err)
		}
		if f.ID == fileID {
			vtok = v
			break
		}
	}
	if err := s.CompleteVerify(ctx, fileID, vtok, "/cache/blobs/"+string(rune('a'+fileID))); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseInstallAndJobCompletion(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, jobID := seedListedRepo(t, s, "org/repo", []FileEntry{
		{Path: "a", Size: 100, GitOID: "ga"},
		{Path: "b", Size: 100, GitOID: "gb"},
	})
	aID := mustFileID(t, s, repoID, "a")
	bID := mustFileID(t, s, repoID, "b")

	// Nothing cached yet: no install work.
	if _, _, _, _, _, err := s.LeaseInstall(ctx, time.Now()); !errors.Is(err, ErrNoWork) {
		t.Fatalf("LeaseInstall before cache err = %v, want ErrNoWork", err)
	}

	cacheFile(t, s, aID)
	cacheFile(t, s, bID)

	jf, f, j, r, tok, err := s.LeaseInstall(ctx, time.Now())
	if err != nil {
		t.Fatalf("LeaseInstall: %v", err)
	}
	if jf.JobID != jobID || jf.FileID != aID || jf.Status != JobFileInstalling {
		t.Fatalf("LeaseInstall jf = %+v", jf)
	}
	if f.ID != aID || f.Status != FileCached || j.ID != jobID || r.ID != repoID || tok == "" {
		t.Fatalf("LeaseInstall joined rows = (%+v, %+v, %+v)", f, j, r)
	}

	// Job not done while b is pending.
	if err := s.CompleteInstall(ctx, jobID, aID, tok, nil); err != nil {
		t.Fatalf("CompleteInstall a: %v", err)
	}
	var jobSt string
	if err := s.db.QueryRowContext(ctx, "SELECT status FROM jobs WHERE id = ?", jobID).Scan(&jobSt); err != nil {
		t.Fatal(err)
	}
	if jobSt == string(JobDone) {
		t.Fatal("job done with b still pending")
	}
	done, total, err := s.JobProgress(ctx, jobID)
	if err != nil || done != 1 || total != 2 {
		t.Fatalf("JobProgress = (%d, %d, %v), want (1, 2, nil)", done, total, err)
	}

	// Wrong token on the second install is fenced.
	jf2, _, _, _, tok2, err := s.LeaseInstall(ctx, time.Now())
	if err != nil || jf2.FileID != bID {
		t.Fatalf("LeaseInstall b = (%+v, %v)", jf2, err)
	}
	if err := s.CompleteInstall(ctx, jobID, bID, "01JWRONGTOKEN000000000000", nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("CompleteInstall wrong token err = %v, want ErrFenced", err)
	}
	if err := s.CompleteInstall(ctx, jobID, bID, tok2, nil); err != nil {
		t.Fatalf("CompleteInstall b: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT status FROM jobs WHERE id = ?", jobID).Scan(&jobSt); err != nil {
		t.Fatal(err)
	}
	if jobSt != string(JobDone) {
		t.Errorf("job status = %s, want done after all job_files done", jobSt)
	}
	done, total, _ = s.JobProgress(ctx, jobID)
	if done != 2 || total != 2 {
		t.Errorf("JobProgress = (%d, %d), want (2, 2)", done, total)
	}

	// FileJobFiles reflects the shared file's associations.
	jfs, err := s.FileJobFiles(ctx, aID)
	if err != nil || len(jfs) != 1 {
		t.Fatalf("FileJobFiles = (%d, %v)", len(jfs), err)
	}
	if jfs[0].Status != JobFileDone {
		t.Errorf("job_file status = %s, want done", jfs[0].Status)
	}
}

func TestCompleteInstallError(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, jobID := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 1, GitOID: "g"}})
	fileID := mustFileID(t, s, repoID, "a")
	cacheFile(t, s, fileID)

	_, _, _, _, tok, err := s.LeaseInstall(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteInstall(ctx, jobID, fileID, tok, errors.New("disk full")); err != nil {
		t.Fatal(err)
	}
	var st, lastErr string
	if err := s.db.QueryRowContext(ctx,
		"SELECT status, last_error FROM job_files WHERE job_id = ? AND file_id = ?", jobID, fileID).Scan(&st, &lastErr); err != nil {
		t.Fatal(err)
	}
	if st != string(JobFileError) || lastErr != "disk full" {
		t.Errorf("job_file = (%s, %q), want (error, disk full)", st, lastErr)
	}
}
