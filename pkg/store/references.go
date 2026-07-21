package store

import (
	"context"
	"fmt"
	"time"
)

// AddReferenceFiles upserts --reference stat rows by path. Stat fields are
// refreshed on conflict but status is preserved (a hashed row stays hashed
// until InvalidateReference says the file changed).
func (s *Store) AddReferenceFiles(ctx context.Context, refs []ReferenceFile) error {
	now := utc(time.Now())
	for i := range refs {
		r := &refs[i]
		if _, err := s.db.ExecContext(ctx,
			"INSERT INTO reference_files (path, size, mtime_ns, dev, ino, status, updated_at) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?) "+
				"ON CONFLICT (path) DO UPDATE SET size = excluded.size, mtime_ns = excluded.mtime_ns, "+
				"dev = excluded.dev, ino = excluded.ino, updated_at = excluded.updated_at",
			r.Path, r.Size, r.MtimeNs, r.Dev, r.Ino, string(RefPending), now); err != nil {
			return fmt.Errorf("store: add reference %s: %w", r.Path, err)
		}
	}
	return nil
}

// InvalidateReference marks a reference whose stat changed so the caller
// re-stats it: back to pending with the stale hash dropped.
func (s *Store) InvalidateReference(ctx context.Context, id int64) error {
	return execGuarded(ctx, s.db, "invalidate reference", id,
		"UPDATE reference_files SET status = ?, sha256 = NULL, last_error = NULL, "+
			"lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ?",
		string(RefPending), utc(time.Now()), id)
}

// LeaseReferenceHash claims the oldest pending reference whose size matches
// some pending salvage target (a queued/downloading/salvaging file of equal
// size): size-mismatched references can never be whole-file
// matches, so they stay stat-only and are never read. ErrNoWork otherwise.
func (s *Store) LeaseReferenceHash(ctx context.Context, now time.Time) (*ReferenceFile, LeaseToken, error) {
	tok := newToken()
	r := new(ReferenceFile)
	err := claimOne(ctx, s.db, r,
		"UPDATE reference_files SET status = ?, lease_owner = ?, lease_token = ?, lease_until = ?, updated_at = ? "+
			"WHERE id = (SELECT rf.id FROM reference_files rf WHERE rf.status = ? AND EXISTS ("+
			"SELECT 1 FROM files f WHERE f.size = rf.size AND f.status IN (?, ?, ?)) "+
			"ORDER BY rf.id LIMIT 1) AND status = ? "+
			"RETURNING *",
		string(RefHashing), s.owner, string(tok), utc(now.Add(leaseDuration)), utc(now),
		string(RefPending),
		string(FileQueued), string(FileDownloading), string(FileSalvaging),
		string(RefPending))
	if err != nil {
		return nil, "", fmt.Errorf("store: lease reference hash: %w", err)
	}
	return r, tok, nil
}

// CompleteReferenceHash finishes a hash lease: err == nil → hashed with the
// whole-file sha256, otherwise error with last_error recorded.
func (s *Store) CompleteReferenceHash(ctx context.Context, id int64, tok LeaseToken, sha256 string, cause error) error {
	status := RefHashed
	var lastErr *string
	if cause != nil {
		status = RefError
		msg := cause.Error()
		lastErr = &msg
	}
	return execGuarded(ctx, s.db, "complete reference hash", id,
		"UPDATE reference_files SET status = ?, sha256 = NULLIF(?, ''), last_error = ?, "+
			"lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? "+
			"WHERE id = ? AND status = ? AND lease_token = ?",
		string(status), sha256, lastErr, utc(time.Now()), id, string(RefHashing), string(tok))
}
