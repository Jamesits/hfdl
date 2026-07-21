package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

// EnqueueJob inserts the job and its repo row in one tx. The repo is keyed
// by (type, name, revision, endpoint): re-enqueueing an existing repo keeps
// its listing state, so restarts resume where they stopped.
func (s *Store) EnqueueJob(ctx context.Context, j *Job) error {
	if j.Repo == nil {
		return fmt.Errorf("store: enqueue job: job carries no repo")
	}
	r := j.Repo
	if r.Type == "" {
		r.Type = "model"
	}
	if r.Revision == "" {
		r.Revision = "main"
	}
	if j.Status == "" {
		j.Status = JobQueued
	}
	return s.inTx(ctx, "enqueue job", func(tx bun.Tx) error {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO repos (type, name, revision, endpoint, status) VALUES (?, ?, ?, ?, ?) "+
				"ON CONFLICT (type, name, revision, endpoint) DO NOTHING",
			r.Type, r.Name, r.Revision, r.Endpoint, string(RepoPending)); err != nil {
			return fmt.Errorf("store: enqueue job repo %s: %w", r.Name, err)
		}
		if err := tx.QueryRowContext(ctx,
			"SELECT id FROM repos WHERE type = ? AND name = ? AND revision = ? AND endpoint = ?",
			r.Type, r.Name, r.Revision, r.Endpoint).Scan(&r.ID); err != nil {
			return fmt.Errorf("store: enqueue job repo %s: %w", r.Name, err)
		}

		j.RepoID = r.ID
		if err := tx.NewInsert().Model(j).
			ExcludeColumn("created_at", "updated_at"). // DDL defaults (UTC)
			Returning("id, created_at, updated_at").
			Scan(ctx); err != nil {
			return fmt.Errorf("store: enqueue job: %w", err)
		}
		return nil
	})
}

// LeaseMeta claims the oldest pending repo for listing
// (pending → listing), returning the repo and its fencing token.
// ErrNoWork when the meta queue is empty.
func (s *Store) LeaseMeta(ctx context.Context, now time.Time) (*Repo, LeaseToken, error) {
	tok := newToken()
	repo := new(Repo)
	err := claimOne(ctx, s.db, repo,
		"UPDATE repos SET status = ?, lease_owner = ?, lease_token = ?, lease_until = ?, updated_at = ? "+
			"WHERE id = (SELECT id FROM repos WHERE status = ? AND (available_at IS NULL OR available_at <= ?) ORDER BY id LIMIT 1) AND status = ? "+
			"RETURNING *",
		string(RepoListing), s.owner, string(tok), utc(now.Add(leaseDuration)), utc(now),
		string(RepoPending), utc(now), string(RepoPending))
	if err != nil {
		return nil, "", fmt.Errorf("store: lease meta: %w", err)
	}
	return repo, tok, nil
}

// SetCommitSHA pins rev → commit_sha once per repo: the meta worker
// resolves the revision, pins the sha, then lists at the immutable sha.
// Guarded by the listing lease. The method exists because no other pinned
// store method persists commit_sha, and NextDownloadableFiles gates on it.
func (s *Store) SetCommitSHA(ctx context.Context, repoID int64, tok LeaseToken, sha string) error {
	return execGuarded(ctx, s.db, "set commit sha", repoID,
		"UPDATE repos SET commit_sha = ?, updated_at = ? WHERE id = ? AND status = ? AND "+leaseGuard,
		sha, utc(time.Now()), repoID, string(RepoListing), string(tok), string(tok))
}

// CompleteListing persists a listing in one tx: files are upserted by
// (repo_id, path) without touching existing download state (idempotent
// re-listing after a crash), job_files rows are created for every job of the
// repo, and the repo is marked listed — all fenced by the listing token.
//
// Fully specified entries are inserted straight into the download queue
// (status queued); 'discovered' remains the DDL default for rows inserted
// without complete metadata (the meta queue's hash-backfill pass fills in
// the git_oid IS NULL rows later).
func (s *Store) CompleteListing(ctx context.Context, repoID int64, tok LeaseToken, files []FileEntry) error {
	return s.inTx(ctx, "complete listing", func(tx bun.Tx) error {
		now := utc(time.Now())
		if err := execGuarded(ctx, tx, "complete listing", repoID,
			"UPDATE repos SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
				"WHERE id = ? AND status = ? AND "+leaseGuard,
			string(RepoListed), now, repoID, string(RepoListing), string(tok), string(tok)); err != nil {
			return err
		}

		for i := range files {
			f := &files[i]
			var fileID int64
			if err := tx.QueryRowContext(ctx,
				"INSERT INTO files (repo_id, path, size, git_oid, sha256, xet_hash, is_lfs, status, updated_at) "+
					"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "+
					"ON CONFLICT (repo_id, path) DO UPDATE SET size = excluded.size, git_oid = excluded.git_oid, "+
					"sha256 = excluded.sha256, xet_hash = excluded.xet_hash, is_lfs = excluded.is_lfs, "+
					"updated_at = excluded.updated_at "+
					"RETURNING id",
				repoID, f.Path, f.Size, f.GitOID, f.SHA256, f.XetHash, f.IsLFS, string(FileQueued), now,
			).Scan(&fileID); err != nil {
				return fmt.Errorf("store: complete listing file %s: %w", f.Path, err)
			}
			if _, err := tx.ExecContext(ctx,
				"INSERT INTO job_files (job_id, file_id, status, updated_at) "+
					"SELECT id, ?, ?, ? FROM jobs WHERE repo_id = ? "+
					"ON CONFLICT (job_id, file_id) DO NOTHING",
				fileID, string(JobFilePending), now, repoID); err != nil {
				return fmt.Errorf("store: complete listing job_files %s: %w", f.Path, err)
			}
		}
		return nil
	})
}

// JobProgress reports done/total job_files for the TUI and drain detection.
func (s *Store) JobProgress(ctx context.Context, jobID int64) (done, total int, err error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FILTER (WHERE status = ?), COUNT(*) FROM job_files WHERE job_id = ?",
		string(JobFileDone), jobID)
	if err := row.Scan(&done, &total); err != nil {
		return 0, 0, fmt.Errorf("store: job progress %d: %w", jobID, err)
	}
	return done, total, nil
}

// FileJobFiles returns every job association of a file (install fan-out:
// two jobs may share one files row with different destinations).
func (s *Store) FileJobFiles(ctx context.Context, fileID int64) ([]JobFile, error) {
	var jfs []JobFile
	if err := s.db.NewSelect().Model(&jfs).
		Where("file_id = ?", fileID).
		Order("job_id").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("store: file job_files %d: %w", fileID, err)
	}
	return jfs, nil
}

// LeaseInstall claims the oldest pending job_files row whose file is cached
// (pending → installing) and returns it with its file, job, and repo.
// ErrNoWork when nothing is installable.
func (s *Store) LeaseInstall(ctx context.Context, now time.Time) (*JobFile, *File, *Job, *Repo, LeaseToken, error) {
	return s.leaseInstall(ctx, now, "")
}

// LeaseInstallMode is LeaseInstall restricted to jobs of a single destination
// mode ("cache" | "local-dir"), so the scheduler can run a cheap symlink pool
// (cache mode) and a separate duty-gated copy pool (local-dir) without cheap
// installs starving behind long copies. Empty destMode leases any mode.
func (s *Store) LeaseInstallMode(ctx context.Context, now time.Time, destMode string) (*JobFile, *File, *Job, *Repo, LeaseToken, error) {
	return s.leaseInstall(ctx, now, destMode)
}

func (s *Store) leaseInstall(ctx context.Context, now time.Time, destMode string) (*JobFile, *File, *Job, *Repo, LeaseToken, error) {
	var (
		jf  *JobFile
		f   *File
		j   *Job
		r   *Repo
		tok LeaseToken
	)
	err := s.inTx(ctx, "lease install", func(tx bun.Tx) error {
		tok = newToken()
		jf = new(JobFile)
		sel := "SELECT jf.job_id, jf.file_id FROM job_files jf " +
			"JOIN files f ON f.id = jf.file_id AND f.status = ? " +
			"JOIN jobs jb ON jb.id = jf.job_id " +
			"WHERE jf.status = ?"
		args := []any{
			string(JobFileInstalling), s.owner, string(tok), utc(now.Add(leaseDuration)), utc(now),
			string(FileCached), string(JobFilePending),
		}
		if destMode != "" {
			sel += " AND jb.dest_mode = ?"
			args = append(args, destMode)
		}
		sel += " ORDER BY jf.job_id, jf.file_id LIMIT 1"
		err := claimOne(ctx, tx, jf,
			"UPDATE job_files SET status = ?, lease_owner = ?, lease_token = ?, lease_until = ?, updated_at = ? "+
				"WHERE (job_id, file_id) = ("+sel+") AND status = ? "+
				"RETURNING *",
			append(args, string(JobFilePending))...)
		if err != nil {
			return fmt.Errorf("store: lease install: %w", err)
		}

		f = new(File)
		if err := tx.NewSelect().Model(f).Where("id = ?", jf.FileID).Scan(ctx); err != nil {
			return fmt.Errorf("store: lease install file: %w", err)
		}
		j = new(Job)
		if err := tx.NewSelect().Model(j).Where("id = ?", jf.JobID).Scan(ctx); err != nil {
			return fmt.Errorf("store: lease install job: %w", err)
		}
		r = new(Repo)
		if err := tx.NewSelect().Model(r).Where("id = ?", j.RepoID).Scan(ctx); err != nil {
			return fmt.Errorf("store: lease install repo: %w", err)
		}

		// The job is running once any of its files is being installed.
		if _, err := tx.ExecContext(ctx,
			"UPDATE jobs SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
			string(JobRunning), utc(now), j.ID, string(JobQueued)); err != nil {
			return fmt.Errorf("store: lease install job state: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	return jf, f, j, r, tok, nil
}

// CompleteInstall finishes an install lease: err == nil → done, otherwise
// error with last_error recorded. When every job_files row of the job is
// done, the job flips to done in the same tx.
func (s *Store) CompleteInstall(ctx context.Context, jobID, fileID int64, tok LeaseToken, err error) error {
	return s.inTx(ctx, "complete install", func(tx bun.Tx) error {
		now := utc(time.Now())
		status := JobFileDone
		var lastErr *string
		if err != nil {
			status = JobFileError
			msg := err.Error()
			lastErr = &msg
		}
		res, err := tx.ExecContext(ctx,
			"UPDATE job_files SET status = ?, last_error = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
				"WHERE job_id = ? AND file_id = ? AND status = ? AND lease_token = ?",
			string(status), lastErr, now, jobID, fileID, string(JobFileInstalling), string(tok))
		if err != nil {
			return fmt.Errorf("store: complete install %d/%d: %w", jobID, fileID, err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fenced("complete install", fileID)
		}

		if status == JobFileDone {
			var remaining int
			if err := tx.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM job_files WHERE job_id = ? AND status <> ?",
				jobID, string(JobFileDone)).Scan(&remaining); err != nil {
				return fmt.Errorf("store: complete install count: %w", err)
			}
			if remaining == 0 {
				if _, err := tx.ExecContext(ctx,
					"UPDATE jobs SET status = ?, updated_at = ? WHERE id = ? AND status IN (?, ?)",
					string(JobDone), now, jobID, string(JobQueued), string(JobRunning)); err != nil {
					return fmt.Errorf("store: complete install job done: %w", err)
				}
			}
		}
		return nil
	})
}
