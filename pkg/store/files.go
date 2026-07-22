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
	if tok == "" && !tokenlessEdgeAllowed(from, to) {
		return fmt.Errorf("store: transition file id=%d %s→%s: %w", fileID, from, to, ErrTokenlessEdge)
	}
	return s.inTx(ctx, "transition file", func(tx bun.Tx) error {
		now := utc(time.Now())

		// Ownership first: a fenced worker must see ErrFenced regardless of what
		// the row's new owner is doing with it.
		guard := leaseGuard
		guardArgs := []any{string(tok)}
		if tok == "" {
			guard = "lease_token IS NULL"
			guardArgs = nil
		}
		var owned bool
		if err := tx.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM files WHERE id = ? AND status = ? AND "+guard+")",
			append([]any{fileID, string(from)}, guardArgs...)...).Scan(&owned); err != nil {
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
			// All blocks must be done: a file is byte-complete only when every
			// scheduling block finished. A crash that completed one block of two
			// (no live lease on the other) must not reach downloaded — that
			// leaves a pending range unfetched. Byte-complete resume (durable
			// blob covers the file, single-stream fallback) uses FinishDownloaded
			// instead, which clears the stale block rows.
			var notDone int
			if err := tx.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM blocks WHERE file_id = ? AND status <> ?",
				fileID, string(BlockDone)).Scan(&notDone); err != nil {
				return fmt.Errorf("store: transition file blocks done: %w", err)
			}
			if notDone > 0 {
				return fmt.Errorf("store: transition file id=%d: %w (%d not done)", fileID, ErrBlocksPending, notDone)
			}
		}

		var lastErr *string
		if cause != nil {
			msg := cause.Error()
			lastErr = &msg
		}
		args := []any{string(to), lastErr, now, fileID, string(from)}
		args = append(args, guardArgs...)
		return execGuarded(ctx, tx, "transition file", fileID,
			"UPDATE files SET status = ?, last_error = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
				"WHERE id = ? AND status = ? AND "+guard, args...)
	})
}

// tokenlessEdgeAllowed reports whether an empty-token TransitionFile may drive
// the given edge. Only a fixed set of edges legitimately mutate an unleased
// row without a lease token: salvage (salvaging→queued on no-match/error,
// salvaging→downloaded on apply), offline cache-serve (queued→downloaded), and
// --force-download re-queue of an already-cached file (cached→queued). Every
// other edge must be fenced by a real lease token.
func tokenlessEdgeAllowed(from, to FileStatus) bool {
	switch {
	case from == FileQueued && to == FileDownloaded:
		return true
	case from == FileCached && to == FileQueued:
		return true
	}
	return false
}

// FinishDownloaded transitions a byte-complete file downloading → downloaded
// when its durable progress already covers the whole file (resume at the
// finish line, single-stream fallback): the remaining pending/active block
// rows are stale scheduling state carrying no byte truth, so they are dropped
// in the same tx before the transition — which then trivially satisfies the
// all-blocks-done assertion. Fenced by the held file lease token.
func (s *Store) FinishDownloaded(ctx context.Context, fileID int64, tok LeaseToken) error {
	return s.inTx(ctx, "finish downloaded", func(tx bun.Tx) error {
		now := utc(time.Now())
		var owned bool
		if err := tx.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM files WHERE id = ? AND status = ? AND "+leaseGuard+")",
			fileID, string(FileDownloading), string(tok)).Scan(&owned); err != nil {
			return fmt.Errorf("store: finish downloaded: %w", err)
		}
		if !owned {
			return fenced("finish downloaded", fileID)
		}
		if _, err := tx.ExecContext(ctx,
			"DELETE FROM blocks WHERE file_id = ? AND status <> ?",
			fileID, string(BlockDone)); err != nil {
			return fmt.Errorf("store: finish downloaded drop blocks: %w", err)
		}
		return execGuarded(ctx, tx, "finish downloaded", fileID,
			"UPDATE files SET status = ?, last_error = NULL, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
				"WHERE id = ? AND status = ? AND "+leaseGuard,
			string(FileDownloaded), now, fileID, string(FileDownloading), string(tok))
	})
}

// SaveProgress persists the durable-only checkpoint blob (fsynced bytes
// only; serde owned by transfer, a plain []byte crosses the boundary).
// Guarded by the download or salvage lease.
func (s *Store) SaveProgress(ctx context.Context, fileID int64, tok LeaseToken, blob []byte) error {
	return execGuarded(ctx, s.db, "save progress", fileID,
		"UPDATE files SET progress = ?, progress_ver = progress_ver + 1, updated_at = ? "+
			"WHERE id = ? AND status IN (?, ?) AND "+leaseGuard,
		blob, utc(time.Now()), fileID,
		string(FileDownloading), string(FileSalvaging), string(tok))
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

// IncrFileRetries bumps a downloading file's retry counter (fenced by its
// download lease) and returns the new count — the give-up bound for phases
// that have no block rows yet, e.g. xet source preparation. ErrFenced if the
// caller no longer owns the row.
func (s *Store) IncrFileRetries(ctx context.Context, fileID int64, tok LeaseToken) (int, error) {
	var n int
	err := s.db.NewRaw(
		"UPDATE files SET retries = retries + 1, updated_at = ? "+
			"WHERE id = ? AND status = ? AND lease_token = ? RETURNING retries",
		utc(time.Now()), fileID, string(FileDownloading), string(tok)).Scan(ctx, &n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fenced("incr file retries", fileID)
	}
	if err != nil {
		return 0, fmt.Errorf("store: incr file retries id=%d: %w", fileID, err)
	}
	return n, nil
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

// MarkCachedFromBlob short-circuits a queued file to cached when its verified
// blob is already in the content-addressed store — the degenerate salvage case
// : a prior run, or another repo/revision, already downloaded and
// verified this exact content, so there is nothing to fetch, verify or copy.
// Tokenless: the file is unleased in the download queue and the status guard
// (queued) is the exclusion — a racer that already claimed it loses (ErrFenced).
func (s *Store) MarkCachedFromBlob(ctx context.Context, fileID int64, cachePath string) error {
	return execGuarded(ctx, s.db, "mark cached from blob", fileID,
		"UPDATE files SET status = ?, cache_path = ?, updated_at = ? WHERE id = ? AND status = ?",
		string(FileCached), cachePath, utc(time.Now()), fileID, string(FileQueued))
}

// LeaseSalvage claims one salvaging file for salvage-apply, adding an
// owner+token+lease_until without changing its status (it stays salvaging so
// the verifier re-hashes the copied bytes on completion). Oldest first,
// skipping rows with a live lease; ErrNoWork when none is claimable. The lease
// is heartbeated via RenewLease(LeaseFile) and Recover requeues an expired one
// (salvaging→queued), so a crash mid-salvage no longer strands the file — the
// in-memory claim it replaces had no durability.
func (s *Store) LeaseSalvage(ctx context.Context, now time.Time) (*File, LeaseToken, error) {
	tok := newToken()
	f := new(File)
	err := claimOne(ctx, s.db, f,
		"UPDATE files SET lease_owner = ?, lease_token = ?, lease_until = ?, updated_at = ? "+
			"WHERE id = (SELECT id FROM files WHERE status = ? AND (lease_until IS NULL OR lease_until <= ?) ORDER BY id LIMIT 1) AND status = ? "+
			"RETURNING *",
		s.owner, string(tok), utc(now.Add(leaseDuration)), utc(now),
		string(FileSalvaging), utc(now), string(FileSalvaging))
	if err != nil {
		return nil, "", fmt.Errorf("store: lease salvage: %w", err)
	}
	return f, tok, nil
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
// current pass. Fenced by the held download lease token: only the current
// downloading owner may re-chunk, so a stale worker whose lease expired can
// never delete the new owner's freshly-leased blocks.
func (s *Store) ReplacePendingBlocks(ctx context.Context, fileID int64, tok LeaseToken, blocks []Block) error {
	return s.inTx(ctx, "replace pending blocks", func(tx bun.Tx) error {
		var ok bool
		if err := tx.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM files WHERE id = ? AND status = ? AND "+leaseGuard+")",
			fileID, string(FileDownloading), string(tok)).Scan(&ok); err != nil {
			return fmt.Errorf("store: replace pending blocks: %w", err)
		}
		if !ok {
			return fmt.Errorf("store: replace pending blocks id=%d: not the downloading owner: %w", fileID, ErrFenced)
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
