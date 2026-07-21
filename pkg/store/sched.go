package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

// sched-facing batch transitions: job/file/repo lifecycle operations the
// queue manager composes beyond the per-row lease machine. All multi-row
// changes are one tx; lease-fenced ones assert exactly one row (ErrFenced).

// EnsureJobFiles idempotently creates pending job_files rows pairing jobID
// with already-listed files — used when a new job targets a repo whose
// listing completed before the job existed (no CompleteListing run).
func (s *Store) EnsureJobFiles(ctx context.Context, jobID int64, fileIDs []int64) error {
	if len(fileIDs) == 0 {
		return nil
	}
	return s.inTx(ctx, "ensure job files", func(tx bun.Tx) error {
		now := utc(time.Now())
		for _, fileID := range fileIDs {
			if _, err := tx.ExecContext(ctx,
				"INSERT INTO job_files (job_id, file_id, status, updated_at) VALUES (?, ?, ?, ?) "+
					"ON CONFLICT (job_id, file_id) DO NOTHING",
				jobID, fileID, string(JobFilePending), now); err != nil {
				return fmt.Errorf("store: ensure job files job=%d file=%d: %w", jobID, fileID, err)
			}
		}
		return nil
	})
}

// SetJobFileDests persists the resolved install target per job_files row of
// a job. Strict: every (job, file) pair must exist — a missing row is a
// scheduler bug, not a fence.
func (s *Store) SetJobFileDests(ctx context.Context, jobID int64, dests map[int64]string) error {
	if len(dests) == 0 {
		return nil
	}
	return s.inTx(ctx, "set job file dests", func(tx bun.Tx) error {
		now := utc(time.Now())
		for fileID, dest := range dests {
			res, err := tx.ExecContext(ctx,
				"UPDATE job_files SET dest_path = ?, updated_at = ? WHERE job_id = ? AND file_id = ?",
				dest, now, jobID, fileID)
			if err != nil {
				return fmt.Errorf("store: set job file dests job=%d file=%d: %w", jobID, fileID, err)
			}
			if n, _ := res.RowsAffected(); n != 1 {
				return fmt.Errorf("store: set job file dests: no job_files row job=%d file=%d", jobID, fileID)
			}
		}
		return nil
	})
}

// FinishJob terminally resolves a job: nil cause marks it done and flushes
// its still-pending job_files to done (files already cached, nothing to
// install); a non-nil cause errors the job and its pending rows. Rows
// in-flight (installing) or already resolved are untouched.
func (s *Store) FinishJob(ctx context.Context, jobID int64, cause error) error {
	return s.inTx(ctx, "finish job", func(tx bun.Tx) error {
		now := utc(time.Now())
		status := JobDone
		var lastErr *string
		if cause != nil {
			status = JobError
			msg := cause.Error()
			lastErr = &msg
		}
		res, err := tx.ExecContext(ctx,
			"UPDATE jobs SET status = ?, last_error = COALESCE(?, last_error), updated_at = ? "+
				"WHERE id = ? AND status IN (?, ?)",
			string(status), lastErr, now, jobID, string(JobQueued), string(JobRunning))
		if err != nil {
			return fmt.Errorf("store: finish job %d: %w", jobID, err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("store: finish job %d: job not queued/running", jobID)
		}
		jfStatus := JobFileDone
		if cause != nil {
			jfStatus = JobFileError
		}
		if _, err := tx.ExecContext(ctx,
			"UPDATE job_files SET status = ?, last_error = COALESCE(?, last_error), updated_at = ? "+
				"WHERE job_id = ? AND status = ?",
			string(jfStatus), lastErr, now, jobID, string(JobFilePending)); err != nil {
			return fmt.Errorf("store: finish job %d job_files: %w", jobID, err)
		}
		return nil
	})
}

// FailRepo terminally errors a repo whose listing failed, fenced by the
// listing token (listing → error, ErrFenced on 0 rows), and cascades the
// error to its queued/running jobs in the same tx.
func (s *Store) FailRepo(ctx context.Context, repoID int64, tok LeaseToken, cause error) error {
	return s.inTx(ctx, "fail repo", func(tx bun.Tx) error {
		var lastErr *string
		if cause != nil {
			msg := cause.Error()
			lastErr = &msg
		}
		now := utc(time.Now())
		if err := execGuarded(ctx, tx, "fail repo", repoID,
			"UPDATE repos SET status = ?, last_error = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
				"WHERE id = ? AND status = ? AND lease_token = ?",
			string(RepoError), lastErr, now, repoID, string(RepoListing), string(tok)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"UPDATE jobs SET status = ?, last_error = ?, updated_at = ? WHERE repo_id = ? AND status IN (?, ?)",
			string(JobError), lastErr, now, repoID, string(JobQueued), string(JobRunning)); err != nil {
			return fmt.Errorf("store: fail repo %d jobs: %w", repoID, err)
		}
		return nil
	})
}

// FailJobFilesForFile terminally errors a file's pending job_files rows and
// cascades the error to exactly the jobs that owned those rows (terminal
// file path: verify failed ≥2 times, retries exhausted). Jobs whose row for
// this file already resolved are untouched.
func (s *Store) FailJobFilesForFile(ctx context.Context, fileID int64, cause error) error {
	return s.inTx(ctx, "fail job files for file", func(tx bun.Tx) error {
		var lastErr *string
		if cause != nil {
			msg := cause.Error()
			lastErr = &msg
		}
		now := utc(time.Now())
		rows, err := tx.QueryContext(ctx,
			"UPDATE job_files SET status = ?, last_error = ?, updated_at = ? "+
				"WHERE file_id = ? AND status = ? RETURNING job_id",
			string(JobFileError), lastErr, now, fileID, string(JobFilePending))
		if err != nil {
			return fmt.Errorf("store: fail job files for file %d: %w", fileID, err)
		}
		var jobIDs []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return fmt.Errorf("store: fail job files for file %d: %w", fileID, err)
			}
			jobIDs = append(jobIDs, id)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("store: fail job files for file %d: %w", fileID, err)
		}
		// Close error after a fully-consumed result set carries no
		// information the scan loop did not already surface.
		_ = rows.Close()

		for _, jobID := range jobIDs {
			if _, err := tx.ExecContext(ctx,
				"UPDATE jobs SET status = ?, last_error = ?, updated_at = ? WHERE id = ? AND status IN (?, ?)",
				string(JobError), lastErr, now, jobID, string(JobQueued), string(JobRunning)); err != nil {
				return fmt.Errorf("store: fail job files for file %d job %d: %w", fileID, jobID, err)
			}
		}
		return nil
	})
}

// DeferMeta requeues a claimed repo (listing → pending) with a backoff but
// WITHOUT counting a retry — used when a gate (a 429 endpoint cooldown) blocks
// the repo's endpoint, so the listing lease is not held across the wait. A gate
// deferral is not a failure, so it must not consume the give-up budget. Fenced
// by the listing token; ErrFenced on 0 rows.
func (s *Store) DeferMeta(ctx context.Context, repoID int64, tok LeaseToken, availableAt time.Time) error {
	return execGuarded(ctx, s.db, "defer meta", repoID,
		"UPDATE repos SET status = ?, available_at = ?, "+
			"lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
			"WHERE id = ? AND status = ? AND lease_token = ?",
		string(RepoPending), utc(availableAt), utc(time.Now()), repoID, string(RepoListing), string(tok))
}

// ReleaseMeta requeues a claimed repo (listing → pending, retries++) with a
// durable backoff — availableAt hides the row from LeaseMeta until it elapses,
// so a persistent 5xx no longer hot-loops lease→fail→re-lease. Fenced by the
// listing token; ErrFenced on 0 rows. Pass a zero availableAt for an immediate
// requeue (no backoff).
func (s *Store) ReleaseMeta(ctx context.Context, repoID int64, tok LeaseToken, availableAt time.Time, cause error) error {
	var lastErr *string
	if cause != nil {
		msg := cause.Error()
		lastErr = &msg
	}
	var avail *time.Time
	if !availableAt.IsZero() {
		u := utc(availableAt)
		avail = &u
	}
	return execGuarded(ctx, s.db, "release meta", repoID,
		"UPDATE repos SET status = ?, retries = retries + 1, last_error = ?, available_at = ?, "+
			"lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
			"WHERE id = ? AND status = ? AND lease_token = ?",
		string(RepoPending), lastErr, avail, utc(time.Now()), repoID, string(RepoListing), string(tok))
}
