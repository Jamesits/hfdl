package store

import (
	"errors"
	"testing"
	"time"
)

func jobStatus(t *testing.T, s *Store, jobID int64) JobStatus {
	t.Helper()
	var st string
	if err := s.db.QueryRowContext(t.Context(), "SELECT status FROM jobs WHERE id = ?", jobID).Scan(&st); err != nil {
		t.Fatalf("jobStatus: %v", err)
	}
	return JobStatus(st)
}

func jfStatus(t *testing.T, s *Store, jobID, fileID int64) JobFileStatus {
	t.Helper()
	var st string
	if err := s.db.QueryRowContext(t.Context(),
		"SELECT status FROM job_files WHERE job_id = ? AND file_id = ?", jobID, fileID).Scan(&st); err != nil {
		t.Fatalf("jfStatus: %v", err)
	}
	return JobFileStatus(st)
}

func TestEnsureJobFilesIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, job1 := seedListedRepo(t, s, "org/repo", []FileEntry{
		{Path: "a", Size: 1, GitOID: "g"},
		{Path: "b", Size: 1, GitOID: "g"},
	})
	aID := mustFileID(t, s, repoID, "a")
	bID := mustFileID(t, s, repoID, "b")

	// Second job on the same repo: no CompleteListing ran for it.
	j2 := &Job{Repo: &Repo{Name: "org/repo", Endpoint: "https://hf.co"}, DestMode: DestModeLocalDir, DestDir: t.TempDir()}
	if err := s.EnqueueJob(ctx, j2); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_files WHERE job_id = ?", j2.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("job2 has %d job_files before ensure, want 0", n)
	}

	if err := s.EnsureJobFiles(ctx, j2.ID, []int64{aID, bID}); err != nil {
		t.Fatalf("EnsureJobFiles: %v", err)
	}
	if st := jfStatus(t, s, j2.ID, aID); st != JobFilePending {
		t.Errorf("job2/a = %s, want pending", st)
	}
	// Idempotent: re-run + existing rows (job1) untouched.
	if err := s.EnsureJobFiles(ctx, j2.ID, []int64{aID, bID}); err != nil {
		t.Fatalf("EnsureJobFiles 2: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_files WHERE job_id = ?", j2.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("job2 job_files = %d after re-ensure, want 2 (no dupes)", n)
	}
	if st := jfStatus(t, s, job1, aID); st != JobFilePending {
		t.Errorf("job1/a = %s, want pending (untouched)", st)
	}
}

func TestSetJobFileDests(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, jobID := seedListedRepo(t, s, "org/repo", []FileEntry{
		{Path: "a", Size: 1, GitOID: "g"},
		{Path: "b", Size: 1, GitOID: "g"},
	})
	aID := mustFileID(t, s, repoID, "a")
	bID := mustFileID(t, s, repoID, "b")

	if err := s.SetJobFileDests(ctx, jobID, map[int64]string{aID: "/dest/a", bID: "/dest/b"}); err != nil {
		t.Fatalf("SetJobFileDests: %v", err)
	}
	var dest string
	if err := s.db.QueryRowContext(ctx,
		"SELECT dest_path FROM job_files WHERE job_id = ? AND file_id = ?", jobID, aID).Scan(&dest); err != nil {
		t.Fatal(err)
	}
	if dest != "/dest/a" {
		t.Errorf("dest_path = %q, want /dest/a", dest)
	}

	// Unknown file for the job: strict error.
	if err := s.SetJobFileDests(ctx, jobID, map[int64]string{999999: "/dest/x"}); err == nil {
		t.Fatal("SetJobFileDests with missing row: want error")
	}
}

func TestFinishJob(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, job1 := seedListedRepo(t, s, "org/repo", []FileEntry{
		{Path: "a", Size: 1, GitOID: "g"},
		{Path: "b", Size: 1, GitOID: "g"},
	})
	aID := mustFileID(t, s, repoID, "a")
	bID := mustFileID(t, s, repoID, "b")

	// nil cause: job + pending job_files → done; resolved rows untouched.
	if _, err := s.db.ExecContext(ctx,
		"UPDATE job_files SET status = ? WHERE job_id = ? AND file_id = ?", string(JobFileDone), job1, aID); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishJob(ctx, job1, nil); err != nil {
		t.Fatalf("FinishJob: %v", err)
	}
	if st := jobStatus(t, s, job1); st != JobDone {
		t.Errorf("job1 = %s, want done", st)
	}
	if st := jfStatus(t, s, job1, bID); st != JobFileDone {
		t.Errorf("job1/b = %s, want done", st)
	}

	// Non-nil cause: job → error with last_error; pending → error.
	j2 := &Job{Repo: &Repo{Name: "org/repo", Endpoint: "https://hf.co"}, DestMode: DestModeCache, DestDir: t.TempDir()}
	if err := s.EnqueueJob(ctx, j2); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureJobFiles(ctx, j2.ID, []int64{aID}); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishJob(ctx, j2.ID, errors.New("aborted")); err != nil {
		t.Fatalf("FinishJob err: %v", err)
	}
	if st := jobStatus(t, s, j2.ID); st != JobError {
		t.Errorf("job2 = %s, want error", st)
	}
	if st := jfStatus(t, s, j2.ID, aID); st != JobFileError {
		t.Errorf("job2/a = %s, want error", st)
	}
	var lastErr string
	if err := s.db.QueryRowContext(ctx, "SELECT last_error FROM jobs WHERE id = ?", j2.ID).Scan(&lastErr); err != nil {
		t.Fatal(err)
	}
	if lastErr != "aborted" {
		t.Errorf("job2 last_error = %q", lastErr)
	}

	// Already terminal: not finishable again.
	if err := s.FinishJob(ctx, j2.ID, nil); err == nil {
		t.Error("FinishJob on terminal job: want error")
	}
}

func TestFailRepoCascade(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	job := &Job{Repo: &Repo{Name: "org/repo", Endpoint: "https://hf.co"}, DestMode: DestModeCache, DestDir: t.TempDir()}
	if err := s.EnqueueJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	// A second, already-done job on the same repo must survive the cascade.
	j2 := &Job{Repo: &Repo{Name: "org/repo", Endpoint: "https://hf.co"}, DestMode: DestModeCache, DestDir: t.TempDir()}
	if err := s.EnqueueJob(ctx, j2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "UPDATE jobs SET status = ? WHERE id = ?", string(JobDone), j2.ID); err != nil {
		t.Fatal(err)
	}

	repo, tok, err := s.LeaseMeta(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FailRepo(ctx, repo.ID, "01JWRONGTOKEN000000000000", errors.New("gated")); !errors.Is(err, ErrFenced) {
		t.Fatalf("FailRepo wrong token err = %v, want ErrFenced", err)
	}
	var repoSt string
	if err := s.db.QueryRowContext(ctx, "SELECT status FROM repos WHERE id = ?", repo.ID).Scan(&repoSt); err != nil {
		t.Fatal(err)
	}
	if repoSt != string(RepoListing) {
		t.Fatalf("repo = %s after fenced fail, want listing", repoSt)
	}

	if err := s.FailRepo(ctx, repo.ID, tok, errors.New("gated repo")); err != nil {
		t.Fatalf("FailRepo: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT status FROM repos WHERE id = ?", repo.ID).Scan(&repoSt); err != nil {
		t.Fatal(err)
	}
	if repoSt != string(RepoError) {
		t.Errorf("repo = %s, want error", repoSt)
	}
	if st := jobStatus(t, s, job.ID); st != JobError {
		t.Errorf("queued job = %s, want error (cascade)", st)
	}
	var lastErr string
	if err := s.db.QueryRowContext(ctx, "SELECT last_error FROM jobs WHERE id = ?", job.ID).Scan(&lastErr); err != nil {
		t.Fatal(err)
	}
	if lastErr != "gated repo" {
		t.Errorf("cascaded last_error = %q", lastErr)
	}
	if st := jobStatus(t, s, j2.ID); st != JobDone {
		t.Errorf("done job = %s, want done (cascade must skip terminal jobs)", st)
	}
}

func TestFailJobFilesForFileCascade(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, job1 := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 1, GitOID: "g"}})
	aID := mustFileID(t, s, repoID, "a")

	j2 := &Job{Repo: &Repo{Name: "org/repo", Endpoint: "https://hf.co"}, DestMode: DestModeCache, DestDir: t.TempDir()}
	if err := s.EnqueueJob(ctx, j2); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureJobFiles(ctx, j2.ID, []int64{aID}); err != nil {
		t.Fatal(err)
	}
	// job2's row for the file is already done: job2 is NOT affected.
	if _, err := s.db.ExecContext(ctx,
		"UPDATE job_files SET status = ? WHERE job_id = ? AND file_id = ?", string(JobFileDone), j2.ID, aID); err != nil {
		t.Fatal(err)
	}

	if err := s.FailJobFilesForFile(ctx, aID, errors.New("verify failed twice")); err != nil {
		t.Fatalf("FailJobFilesForFile: %v", err)
	}
	if st := jfStatus(t, s, job1, aID); st != JobFileError {
		t.Errorf("job1/a = %s, want error", st)
	}
	if st := jobStatus(t, s, job1); st != JobError {
		t.Errorf("job1 = %s, want error (owned a pending row)", st)
	}
	if st := jfStatus(t, s, j2.ID, aID); st != JobFileDone {
		t.Errorf("job2/a = %s, want done (untouched)", st)
	}
	if st := jobStatus(t, s, j2.ID); st != JobQueued {
		t.Errorf("job2 = %s, want queued (no pending row for the file)", st)
	}
}

func TestReleaseMeta(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	if err := s.EnqueueJob(ctx, &Job{
		Repo: &Repo{Name: "org/repo", Endpoint: "https://hf.co"}, DestMode: DestModeCache, DestDir: t.TempDir(),
	}); err != nil {
		t.Fatal(err)
	}
	repo, tok, err := s.LeaseMeta(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if err := s.ReleaseMeta(ctx, repo.ID, "01JWRONGTOKEN000000000000", nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("ReleaseMeta wrong token err = %v, want ErrFenced", err)
	}
	if err := s.ReleaseMeta(ctx, repo.ID, tok, errors.New("429 backoff")); err != nil {
		t.Fatalf("ReleaseMeta: %v", err)
	}
	var st, lastErr string
	var retries int
	var leaseToken *string
	if err := s.db.QueryRowContext(ctx,
		"SELECT status, retries, last_error, lease_token FROM repos WHERE id = ?", repo.ID).
		Scan(&st, &retries, &lastErr, &leaseToken); err != nil {
		t.Fatal(err)
	}
	if st != string(RepoPending) || retries != 1 || lastErr != "429 backoff" || leaseToken != nil {
		t.Errorf("repo = (%s, retries %d, %q, lease %v), want (pending, 1, 429 backoff, nil)",
			st, retries, lastErr, leaseToken)
	}

	// Immediately leasable again; the released token is dead.
	repo2, _, err := s.LeaseMeta(ctx, time.Now())
	if err != nil || repo2.ID != repo.ID {
		t.Fatalf("re-LeaseMeta after release: %v", err)
	}
	if err := s.CompleteListing(ctx, repo.ID, tok, nil); !errors.Is(err, ErrFenced) {
		t.Errorf("old token after release err = %v, want ErrFenced", err)
	}
}
