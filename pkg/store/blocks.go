package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"
)

// BlockFilter narrows LeaseBlocks.
type BlockFilter struct {
	// FileIDs restricts leasing to these files; empty leases across the whole
	// active download set.
	FileIDs []int64
	// MaxPerFile caps blocks leased per file in this call (the per-file
	// connection cap); missing entries and non-positive values are
	// uncapped. Ignored for files not in FileIDs when FileIDs is non-empty.
	MaxPerFile map[int64]int
}

// LeasedBlock couples a claimed block with its fencing token; each block in
// a batch carries its own token so per-block completions are fenced
// independently.
type LeasedBlock struct {
	Block
	Token LeaseToken
}

// LeaseBlocks atomically claims up to n pending blocks (pending → active),
// skipping rows backed off into the future (available_at). Each claim
// is a self-guarding single-statement UPDATE ... RETURNING, so racers lose
// cleanly (0 rows → next candidate) and no enclosing tx is needed — a
// deferred read-tx upgraded to a write would SQLITE_BUSY under parallel
// workers. An empty result means no work right now (not an error).
func (s *Store) LeaseBlocks(ctx context.Context, n int, f BlockFilter, now time.Time) ([]LeasedBlock, error) {
	if n <= 0 {
		return nil, nil
	}

	sel := "SELECT id, file_id FROM blocks WHERE status = ? AND (available_at IS NULL OR available_at <= ?) " +
		"AND EXISTS (SELECT 1 FROM files WHERE files.id = blocks.file_id AND files.status = ?)"
	args := []any{string(BlockPending), utc(now), string(FileDownloading)}
	if len(f.FileIDs) > 0 {
		sel += " AND file_id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(f.FileIDs)), ",") + ")"
		for _, id := range f.FileIDs {
			args = append(args, id)
		}
	}
	sel += " ORDER BY file_id, idx"

	rows, err := s.db.QueryContext(ctx, sel, args...)
	if err != nil {
		return nil, fmt.Errorf("store: lease blocks select: %w", err)
	}
	type cand struct{ id, fileID int64 }
	var cands []cand
	for rows.Next() {
		var c cand
		if err := rows.Scan(&c.id, &c.fileID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("store: lease blocks scan: %w", err)
		}
		cands = append(cands, c)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("store: lease blocks select: %w", err)
	}
	// Close error after a fully-consumed result set carries no information
	// the scan loop did not already surface.
	_ = rows.Close()

	leased := make([]LeasedBlock, 0, n)
	perFile := make(map[int64]int)
	for _, c := range cands {
		if len(leased) >= n {
			break
		}
		if cap, ok := f.MaxPerFile[c.fileID]; ok && cap > 0 && perFile[c.fileID] >= cap {
			continue
		}
		tok := newToken()
		b := new(Block)
		err := s.db.NewRaw(
			"UPDATE blocks SET status = ?, lease_owner = ?, lease_token = ?, lease_until = ?, updated_at = ? "+
				"WHERE id = ? AND status = ? AND EXISTS (SELECT 1 FROM files WHERE files.id = blocks.file_id AND files.status = ?) RETURNING *",
			string(BlockActive), s.owner, string(tok), utc(now.Add(leaseDuration)), utc(now),
			c.id, string(BlockPending), string(FileDownloading)).Scan(ctx, b)
		if errors.Is(err, sql.ErrNoRows) {
			continue // raced out from under us; take the next candidate
		}
		if err != nil {
			return nil, fmt.Errorf("store: lease blocks claim id=%d: %w", c.id, err)
		}
		perFile[c.fileID]++
		leased = append(leased, LeasedBlock{Block: *b, Token: tok})
	}
	return leased, nil
}

// RequeueBlock returns a failed active block to the queue with durable
// backoff: per-item exp-backoff via available_at, retries++; sched gives
// up at 8 retries. Fenced single statement; ErrFenced on 0 rows. Returns the
// incremented retry count so the caller needs no follow-up query.
func (s *Store) RequeueBlock(ctx context.Context, blockID int64, tok LeaseToken, availableAt time.Time, cause error) (retries int, err error) {
	var lastErr *string
	if cause != nil {
		msg := cause.Error()
		lastErr = &msg
	}
	err = s.db.NewRaw(
		"UPDATE blocks SET status = ?, available_at = ?, retries = retries + 1, last_error = ?, "+
			"lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
			"WHERE id = ? AND status = ? AND lease_token = ? RETURNING retries",
		string(BlockPending), utc(availableAt), lastErr, utc(time.Now()),
		blockID, string(BlockActive), string(tok)).Scan(ctx, &retries)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fenced("requeue block", blockID)
	}
	if err != nil {
		return 0, fmt.Errorf("store: requeue block id=%d: %w", blockID, err)
	}
	return retries, nil
}

// CompleteBlock marks an active block done, fenced by its token. fileDone
// reports that the last pending/active block of the file finished — the
// caller then runs TransitionFile(downloading → downloaded).
func (s *Store) CompleteBlock(ctx context.Context, blockID int64, tok LeaseToken) (fileDone bool, err error) {
	var remaining int
	err = s.inTx(ctx, "complete block", func(tx bun.Tx) error {
		if err := execGuarded(ctx, tx, "complete block", blockID,
			"UPDATE blocks SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
				"WHERE id = ? AND status = ? AND lease_token = ?",
			string(BlockDone), utc(time.Now()), blockID, string(BlockActive), string(tok)); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM blocks WHERE file_id = (SELECT file_id FROM blocks WHERE id = ?) AND status <> ?",
			blockID, string(BlockDone)).Scan(&remaining); err != nil {
			return fmt.Errorf("store: complete block count: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return remaining == 0, nil
}
