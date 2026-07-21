package store

import (
	"strconv"
	"testing"
	"time"
)

func TestRecoverRequeuesExpired(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()

	// Seed one leased row per queue, all claimed at `past`.
	past := time.Now().Add(-time.Hour)
	repoID, jobID := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a", Size: 100, GitOID: "g", SHA256: "s", IsLFS: true}})
	fileID := mustFileID(t, s, repoID, "a")

	// repo: back to pending, then claimed for listing at `past`.
	if _, err := s.db.ExecContext(ctx, "UPDATE repos SET status = ? WHERE id = ?", string(RepoPending), repoID); err != nil {
		t.Fatal(err)
	}
	repo, _, err := s.LeaseMeta(ctx, past)
	if err != nil || repo.ID != repoID {
		t.Fatalf("LeaseMeta: %v", err)
	}

	// file downloading + active block at `past`.
	ftok, err := s.LeaseFileForDownload(ctx, fileID, past)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReplacePendingBlocks(ctx, fileID, []Block{{Idx: 0, Offset: 0, Length: 100}}); err != nil {
		t.Fatal(err)
	}
	leased, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{fileID}}, past)
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseBlocks: %v (%d)", err, len(leased))
	}

	// verifying file at `past`.
	vID := insertFile(t, s, repoID, "v", 10, FileDownloaded)
	if _, _, err := s.LeaseVerify(ctx, past); err != nil {
		t.Fatal(err)
	}

	// salvaging file (tokenless claim has no lease_until; Recover leaves it).
	svID := insertFile(t, s, repoID, "sv", 10, FileQueued)
	if err := s.MarkSalvaging(ctx, svID); err != nil {
		t.Fatal(err)
	}

	// installing job_file at `past`: need a cached file with a job_file.
	cID := insertFile(t, s, repoID, "c", 10, FileCached)
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO job_files (job_id, file_id, status) VALUES (?, ?, ?)", jobID, cID, string(JobFilePending)); err != nil {
		t.Fatal(err)
	}
	jf, _, _, _, _, err := s.LeaseInstall(ctx, past)
	if err != nil || jf.FileID != cID {
		t.Fatalf("LeaseInstall: %v", err)
	}

	// hashing reference at `past` (size matches queued target 'a'=100? it's
	// downloading; 'sv' is salvaging size 10 — matches).
	if err := s.AddReferenceFiles(ctx, []ReferenceFile{{Path: "/ref/r", Size: 10, MtimeNs: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.LeaseReferenceHash(ctx, past); err != nil {
		t.Fatal(err)
	}

	stats, err := s.Recover(ctx, time.Now())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	want := RecoveryStats{Repos: 1, Files: 1, Verifying: 1, Salvaging: 0, Blocks: 1, JobFiles: 1, References: 1}
	if stats != want {
		t.Errorf("Recover stats = %+v, want %+v", stats, want)
	}

	// Row states after requeue.
	checks := []struct {
		query string
		want  string
	}{
		{"SELECT status FROM repos WHERE id = " + itoa(repoID), string(RepoPending)},
		{"SELECT status FROM files WHERE id = " + itoa(fileID), string(FileQueued)},
		{"SELECT status FROM files WHERE id = " + itoa(vID), string(FileDownloaded)},
		{"SELECT status FROM files WHERE id = " + itoa(svID), string(FileSalvaging)}, // tokenless: untouched
		{"SELECT status FROM blocks WHERE id = " + itoa(leased[0].ID), string(BlockPending)},
		{"SELECT status FROM job_files WHERE job_id = " + itoa(jobID) + " AND file_id = " + itoa(cID), string(JobFilePending)},
		{"SELECT status FROM reference_files WHERE path = '/ref/r'", string(RefPending)},
	}
	for _, c := range checks {
		var got string
		if err := s.db.QueryRowContext(ctx, c.query).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("%s = %s, want %s", c.query, got, c.want)
		}
	}

	// Leases cleared: the stale file token is fenced now.
	if err := s.TransitionFile(ctx, fileID, ftok, FileDownloading, FileDownloaded, nil); err == nil {
		t.Error("stale token still works after Recover")
	}

	// Requeued rows are leasable again immediately.
	if _, err := s.LeaseFileForDownload(ctx, fileID, time.Now()); err != nil {
		t.Errorf("re-lease after Recover: %v", err)
	}
}

func TestCounts(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, jobID := seedListedRepo(t, s, "org/repo", []FileEntry{
		{Path: "a", Size: 1, GitOID: "g"},
		{Path: "b", Size: 1, GitOID: "g"},
	})
	aID := mustFileID(t, s, repoID, "a")
	leaseFile(t, s, aID, 2)
	leased, err := s.LeaseBlocks(ctx, 1, BlockFilter{FileIDs: []int64{aID}}, time.Now())
	if err != nil || len(leased) != 1 {
		t.Fatal(err)
	}
	_ = jobID

	counts, err := s.Counts(ctx)
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	wants := map[string]int64{
		"repos.listed":      1,
		"jobs.queued":       1,
		"files.queued":      1,
		"files.downloading": 1,
		"job_files.pending": 2,
		"blocks.active":     1,
		"blocks.pending":    1,
	}
	for k, want := range wants {
		if got := counts[k]; got != want {
			t.Errorf("counts[%q] = %d, want %d (all: %v)", k, got, want, counts)
		}
	}
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}
