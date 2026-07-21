package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

// NextDownloadableFiles lists queued files whose repo revision is pinned
// (commit_sha present: downloads resolve at the immutable sha),
// oldest first. Salvaging files are leased through the salvage path instead.
func (s *Store) NextDownloadableFiles(ctx context.Context, limit int) ([]File, error) {
	var files []File
	if err := s.db.NewSelect().Model(&files).
		Join("JOIN repos AS r ON r.id = f.repo_id").
		Where("f.status = ?", string(FileQueued)).
		Where("r.commit_sha IS NOT NULL AND r.commit_sha <> ''").
		Order("f.id").
		Limit(limit).
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("store: next downloadable files: %w", err)
	}
	return files, nil
}

// LeaseFileForDownload flips a queued file to downloading and returns its
// fencing token (queued → downloading). ErrFenced if the file is not queued.
func (s *Store) LeaseFileForDownload(ctx context.Context, fileID int64, now time.Time) (LeaseToken, error) {
	tok := newToken()
	err := execGuarded(ctx, s.db, "lease file for download", fileID,
		"UPDATE files SET status = ?, lease_owner = ?, lease_token = ?, lease_until = ?, updated_at = ? "+
			"WHERE id = ? AND status = ?",
		string(FileDownloading), s.owner, string(tok), utc(now.Add(leaseDuration)), utc(now),
		fileID, string(FileQueued))
	if err != nil {
		return "", err
	}
	return tok, nil
}

// TransitionFile moves a file between machine states, fenced by its lease
// token; err records last_error. The transition ends the lease (lease fields
// cleared). downloading → downloaded additionally proves in the same tx that
// no live block leases remain, so a stale writer can never race the
// verifier (ErrLiveBlockLeases).
func (s *Store) TransitionFile(ctx context.Context, fileID int64, tok LeaseToken, from, to FileStatus, cause error) error {
	return s.inTx(ctx, "transition file", func(tx bun.Tx) error {
		now := utc(time.Now())

		// Ownership first: a fenced worker must see ErrFenced regardless of what
		// the row's new owner is doing with it.
		var owned bool
		if err := tx.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM files WHERE id = ? AND status = ? AND "+leaseGuard+")",
			fileID, string(from), string(tok), string(tok)).Scan(&owned); err != nil {
			return fmt.Errorf("store: transition file: %w", err)
		}
		if !owned {
			return fenced("transition file", fileID)
		}

		if from == FileDownloading && to == FileDownloaded {
			var live int
			if err := tx.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM blocks WHERE file_id = ? AND status = ? AND lease_until IS NOT NULL AND lease_until > ?",
				fileID, string(BlockActive), now).Scan(&live); err != nil {
				return fmt.Errorf("store: transition file live leases: %w", err)
			}
			if live > 0 {
				return fmt.Errorf("store: transition file id=%d: %w (%d live)", fileID, ErrLiveBlockLeases, live)
			}
		}

		var lastErr *string
		if cause != nil {
			msg := cause.Error()
			lastErr = &msg
		}
		return execGuarded(ctx, tx, "transition file", fileID,
			"UPDATE files SET status = ?, last_error = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
				"WHERE id = ? AND status = ? AND "+leaseGuard,
			string(to), lastErr, now, fileID, string(from), string(tok), string(tok))
	})
}

// SaveProgress persists the durable-only checkpoint blob (fsynced bytes
// only; serde owned by transfer, a plain []byte crosses the boundary).
// Guarded by the download lease; the tokenless rule covers salvage-apply,
// which claims via MarkSalvaging.
func (s *Store) SaveProgress(ctx context.Context, fileID int64, tok LeaseToken, blob []byte) error {
	return execGuarded(ctx, s.db, "save progress", fileID,
		"UPDATE files SET progress = ?, progress_ver = progress_ver + 1, updated_at = ? "+
			"WHERE id = ? AND status IN (?, ?) AND "+leaseGuard,
		blob, utc(time.Now()), fileID,
		string(FileDownloading), string(FileSalvaging), string(tok), string(tok))
}

// LoadProgress returns the durable checkpoint blob, nil when none was saved.
// The blob is trusted for offset resume only — integrity is enforced once
// per file by the full-file hash, so missing/corrupt blobs are the caller's
// signal to reset the file to queued.
func (s *Store) LoadProgress(ctx context.Context, fileID int64) ([]byte, error) {
	var blob []byte
	err := s.db.QueryRowContext(ctx, "SELECT progress FROM files WHERE id = ?", fileID).Scan(&blob)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("store: load progress id=%d: %w", fileID, err)
	}
	if err != nil {
		return nil, fmt.Errorf("store: load progress id=%d: %w", fileID, err)
	}
	return blob, nil
}

// LeaseVerify claims the oldest downloaded file for hash checking
// (downloaded → verifying). ErrNoWork when the verify queue is empty.
func (s *Store) LeaseVerify(ctx context.Context, now time.Time) (*File, LeaseToken, error) {
	tok := newToken()
	f := new(File)
	err := claimOne(ctx, s.db, f,
		"UPDATE files SET status = ?, lease_owner = ?, lease_token = ?, lease_until = ?, updated_at = ? "+
			"WHERE id = (SELECT id FROM files WHERE status = ? ORDER BY id LIMIT 1) AND status = ? "+
			"RETURNING *",
		string(FileVerifying), s.owner, string(tok), utc(now.Add(leaseDuration)), utc(now),
		string(FileDownloaded), string(FileDownloaded))
	if err != nil {
		return nil, "", fmt.Errorf("store: lease verify: %w", err)
	}
	return f, tok, nil
}

// CompleteVerify publishes a verified file into the cache
// (verifying → cached, cache_path set), ending the lease.
func (s *Store) CompleteVerify(ctx context.Context, fileID int64, tok LeaseToken, cachePath string) error {
	return execGuarded(ctx, s.db, "complete verify", fileID,
		"UPDATE files SET status = ?, cache_path = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
			"WHERE id = ? AND status = ? AND lease_token = ?",
		string(FileCached), cachePath, utc(time.Now()), fileID, string(FileVerifying), string(tok))
}

// FailVerify counts a hash failure against the file: below maxVerifyFails it
// resets all blocks to pending, drops the progress blob (failed bytes are
// untrusted — byte truth covers fsynced bytes only), and requeues the file;
// at maxVerifyFails the
// file errors out terminally.
func (s *Store) FailVerify(ctx context.Context, fileID int64, tok LeaseToken, cause error) error {
	return s.inTx(ctx, "fail verify", func(tx bun.Tx) error {
		now := utc(time.Now())
		var lastErr *string
		if cause != nil {
			msg := cause.Error()
			lastErr = &msg
		}

		var fails int
		err := tx.QueryRowContext(ctx,
			"UPDATE files SET verify_fails = verify_fails + 1, last_error = ?, updated_at = ? "+
				"WHERE id = ? AND status = ? AND lease_token = ? RETURNING verify_fails",
			lastErr, now, fileID, string(FileVerifying), string(tok)).Scan(&fails)
		if errors.Is(err, sql.ErrNoRows) {
			return fenced("fail verify", fileID)
		}
		if err != nil {
			return fmt.Errorf("store: fail verify id=%d: %w", fileID, err)
		}

		if fails >= maxVerifyFails {
			return execGuarded(ctx, tx, "fail verify (terminal)", fileID,
				"UPDATE files SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
					"WHERE id = ? AND status = ? AND lease_token = ?",
				string(FileError), now, fileID, string(FileVerifying), string(tok))
		}
		if _, err := tx.ExecContext(ctx,
			"UPDATE blocks SET status = ?, upstream = NULL, available_at = NULL, "+
				"lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
				"WHERE file_id = ?",
			string(BlockPending), now, fileID); err != nil {
			return fmt.Errorf("store: fail verify reset blocks: %w", err)
		}
		return execGuarded(ctx, tx, "fail verify (requeue)", fileID,
			"UPDATE files SET status = ?, progress = NULL, progress_ver = progress_ver + 1, "+
				"lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
				"WHERE id = ? AND status = ? AND lease_token = ?",
			string(FileQueued), now, fileID, string(FileVerifying), string(tok))
	})
}

// SalvageableTargets lists LFS files whose bytes are not yet in the cache —
// the scan input for whole-file salvage against --reference roots.
func (s *Store) SalvageableTargets(ctx context.Context) ([]File, error) {
	var files []File
	if err := s.db.NewSelect().Model(&files).
		Where("is_lfs <> 0").
		Where("sha256 IS NOT NULL AND sha256 <> ''").
		Where("status NOT IN (?, ?)", string(FileCached), string(FileError)).
		Order("id").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("store: salvageable targets: %w", err)
	}
	return files, nil
}

// MarkSalvaging claims a file for whole-file salvage
// (discovered|queued → salvaging). It is the one tokenless claim: the status
// flip itself is the atomic exclusion, and subsequent mutations use the
// tokenless guard (empty token + NULL lease_token). The file re-enters the
// normal machine at downloaded once salvage-apply finishes, so the verifier
// still hashes the copied bytes.
func (s *Store) MarkSalvaging(ctx context.Context, fileID int64) error {
	return execGuarded(ctx, s.db, "mark salvaging", fileID,
		"UPDATE files SET status = ?, updated_at = ? WHERE id = ? AND status IN (?, ?)",
		string(FileSalvaging), utc(time.Now()), fileID, string(FileDiscovered), string(FileQueued))
}

// ReplacePendingBlocks re-chunks a file on (re)start of its download:
// pending block rows are deleted and the new set inserted in one tx. Done or
// active blocks are never touched — they carry completed-byte truth for the
// current pass. The file must be queued or downloading.
func (s *Store) ReplacePendingBlocks(ctx context.Context, fileID int64, blocks []Block) error {
	return s.inTx(ctx, "replace pending blocks", func(tx bun.Tx) error {
		var ok bool
		if err := tx.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM files WHERE id = ? AND status IN (?, ?))",
			fileID, string(FileQueued), string(FileDownloading)).Scan(&ok); err != nil {
			return fmt.Errorf("store: replace pending blocks: %w", err)
		}
		if !ok {
			return fmt.Errorf("store: replace pending blocks id=%d: file not queued/downloading: %w", fileID, ErrFenced)
		}

		if _, err := tx.ExecContext(ctx,
			"DELETE FROM blocks WHERE file_id = ? AND status = ?",
			fileID, string(BlockPending)); err != nil {
			return fmt.Errorf("store: replace pending blocks delete: %w", err)
		}

		now := utc(time.Now())
		for i := range blocks {
			b := &blocks[i]
			if _, err := tx.ExecContext(ctx,
				"INSERT INTO blocks (file_id, idx, offset, length, status, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
				fileID, b.Idx, b.Offset, b.Length, string(BlockPending), now); err != nil {
				return fmt.Errorf("store: replace pending blocks insert idx=%d: %w", b.Idx, err)
			}
		}
		return nil
	})
}
