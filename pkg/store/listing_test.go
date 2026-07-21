package store

import (
	"errors"
	"testing"
	"time"
)

func TestEnqueueJobIdempotentRepo(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()

	mk := func() *Job {
		return &Job{
			Repo:     &Repo{Name: "org/repo", Endpoint: "https://hf.co"},
			DestMode: DestModeLocalDir,
			DestDir:  t.TempDir(),
		}
	}
	j1 := mk()
	if err := s.EnqueueJob(ctx, j1); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}
	j2 := mk()
	if err := s.EnqueueJob(ctx, j2); err != nil {
		t.Fatalf("EnqueueJob 2: %v", err)
	}
	if j1.ID == 0 || j2.ID == 0 || j1.ID == j2.ID {
		t.Fatalf("job IDs = %d, %d", j1.ID, j2.ID)
	}
	if j1.RepoID != j2.RepoID {
		t.Fatalf("repo IDs differ: %d vs %d — same repo must be reused", j1.RepoID, j2.RepoID)
	}
	if j1.Status != JobQueued {
		t.Errorf("job status = %s, want queued", j1.Status)
	}
	var repos int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM repos").Scan(&repos); err != nil {
		t.Fatal(err)
	}
	if repos != 1 {
		t.Errorf("repos = %d, want 1", repos)
	}
}

func TestLeaseMetaEmpty(t *testing.T) {
	s := openTestStore(t)
	repo, tok, err := s.LeaseMeta(t.Context(), time.Now())
	if repo != nil || tok != "" || !errors.Is(err, ErrNoWork) {
		t.Fatalf("LeaseMeta empty = (%v, %q, %v), want ErrNoWork", repo, tok, err)
	}
}

func TestLeaseMetaSkipsClaimed(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	seedJob := func(name string) {
		if err := s.EnqueueJob(ctx, &Job{
			Repo:     &Repo{Name: name, Endpoint: "https://hf.co"},
			DestMode: DestModeCache, DestDir: t.TempDir(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	seedJob("org/one")
	seedJob("org/two")

	r1, tok1, err := s.LeaseMeta(ctx, time.Now())
	if err != nil {
		t.Fatalf("LeaseMeta 1: %v", err)
	}
	if r1.Name != "org/one" || r1.Status != RepoListing || tok1 == "" {
		t.Fatalf("LeaseMeta 1 = (%+v, %q)", r1, tok1)
	}
	// Second claim gets the next repo, not the claimed one.
	r2, _, err := s.LeaseMeta(ctx, time.Now())
	if err != nil {
		t.Fatalf("LeaseMeta 2: %v", err)
	}
	if r2.Name != "org/two" {
		t.Fatalf("LeaseMeta 2 = %s, want org/two", r2.Name)
	}
	if _, _, err := s.LeaseMeta(ctx, time.Now()); !errors.Is(err, ErrNoWork) {
		t.Fatalf("LeaseMeta 3 err = %v, want ErrNoWork", err)
	}

	// Wrong token on CompleteListing: fenced, repo still listing.
	if err := s.CompleteListing(ctx, r1.ID, "01JWRONGTOKEN000000000000", nil); !errors.Is(err, ErrFenced) {
		t.Fatalf("CompleteListing wrong token err = %v, want ErrFenced", err)
	}
	var st string
	if err := s.db.QueryRowContext(ctx, "SELECT status FROM repos WHERE id = ?", r1.ID).Scan(&st); err != nil {
		t.Fatal(err)
	}
	if st != string(RepoListing) {
		t.Errorf("repo status = %s after fenced write, want listing", st)
	}
}

func TestCompleteListingIdempotentUpsert(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	entries := []FileEntry{
		{Path: "a.bin", Size: 100, GitOID: "ga", SHA256: "sa", IsLFS: true},
		{Path: "b.txt", Size: 50, GitOID: "gb"},
	}
	repoID, jobID := seedListedRepo(t, s, "org/repo", entries)

	var nFiles, nJF int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM files WHERE repo_id = ?", repoID).Scan(&nFiles); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_files WHERE job_id = ?", jobID).Scan(&nJF); err != nil {
		t.Fatal(err)
	}
	if nFiles != 2 || nJF != 2 {
		t.Fatalf("after listing: files=%d job_files=%d, want 2/2", nFiles, nJF)
	}

	// New files land directly in the download queue with listing metadata.
	aID := mustFileID(t, s, repoID, "a.bin")
	if st := fileStatus(t, s, aID); st != FileQueued {
		t.Fatalf("a.bin status = %s, want queued", st)
	}
	var size int64
	var sha string
	var isLFS bool
	if err := s.db.QueryRowContext(ctx,
		"SELECT size, sha256, is_lfs FROM files WHERE id = ?", aID).Scan(&size, &sha, &isLFS); err != nil {
		t.Fatal(err)
	}
	if size != 100 || sha != "sa" || !isLFS {
		t.Errorf("a.bin = (size %d, sha %s, lfs %v)", size, sha, isLFS)
	}

	// A download starts, then a crash-recovery re-list happens (repo driven
	// back through the meta queue).
	tok := leaseFile(t, s, aID, 1)
	if _, err := s.db.ExecContext(ctx, "UPDATE repos SET status = ? WHERE id = ?", string(RepoPending), repoID); err != nil {
		t.Fatal(err)
	}
	repo, tok2, err := s.LeaseMeta(ctx, time.Now())
	if err != nil {
		t.Fatalf("re-LeaseMeta: %v", err)
	}
	// Re-list with one new entry; a.bin must keep its downloading state.
	if err := s.CompleteListing(ctx, repo.ID, tok2, append(entries, FileEntry{Path: "c.md", Size: 5, GitOID: "gc"})); err != nil {
		t.Fatalf("re-CompleteListing: %v", err)
	}
	if st := fileStatus(t, s, aID); st != FileDownloading {
		t.Errorf("a.bin status after re-list = %s, want downloading (upsert must not clobber state)", st)
	}
	var tokDB string
	if err := s.db.QueryRowContext(ctx, "SELECT lease_token FROM files WHERE id = ?", aID).Scan(&tokDB); err != nil {
		t.Fatal(err)
	}
	if tokDB != string(tok) {
		t.Errorf("a.bin lease token changed by re-list")
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM files WHERE repo_id = ?", repoID).Scan(&nFiles); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_files WHERE job_id = ?", jobID).Scan(&nJF); err != nil {
		t.Fatal(err)
	}
	if nFiles != 3 || nJF != 3 {
		t.Errorf("after re-list: files=%d job_files=%d, want 3/3 (no duplicates)", nFiles, nJF)
	}
	var repoSt string
	if err := s.db.QueryRowContext(ctx, "SELECT status FROM repos WHERE id = ?", repoID).Scan(&repoSt); err != nil {
		t.Fatal(err)
	}
	if repoSt != string(RepoListed) {
		t.Errorf("repo status = %s, want listed", repoSt)
	}
}
