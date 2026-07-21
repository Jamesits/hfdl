package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/uptrace/bun"
)

// LeaseToken is an unguessable per-claim ULID fencing every mutation of a
// leased row. A worker whose lease expired and was reclaimed can no longer
// publish progress, completions, or transitions: its guarded writes match 0
// rows and surface as ErrFenced.
type LeaseToken string

// LeaseKind selects the table RenewLease heartbeats. Job-files are leased on
// a composite key and have short install leases, so they are not renewable
// through this single-id API.
type LeaseKind string

const (
	LeaseRepo      LeaseKind = "repos"
	LeaseFile      LeaseKind = "files"
	LeaseBlock     LeaseKind = "blocks"
	LeaseReference LeaseKind = "reference_files"
)

func newToken() LeaseToken { return LeaseToken(ulid.Make().String()) }

// leaseGuard is the tokenless-aware ownership predicate shared by every
// guarded mutation: exact token match, or empty token against a row claimed
// without one (MarkSalvaging issues no token; see package doc).
const leaseGuard = "(lease_token = ? OR (? = '' AND lease_token IS NULL))"

// execGuarded runs a guarded UPDATE inside conn and asserts exactly one
// affected row, mapping 0 to ErrFenced.
func execGuarded(ctx context.Context, conn bun.IConn, op string, id int64, query string, args ...any) error {
	res, err := conn.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("store: %s id=%d: %w", op, id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: %s id=%d: %w", op, id, err)
	}
	if n != 1 {
		return fenced(op, id)
	}
	return nil
}

// RenewLease heartbeats lease_until on a row the caller still owns; ErrFenced
// on lost ownership. The guard requires the lease to still be live
// (lease_until > now): a nominally-expired lease that Recover may already have
// reclaimed (or is about to) cannot be resurrected — the heartbeat fails,
// surfacing as ErrFenced, and the worker abandons its stale work.
func (s *Store) RenewLease(ctx context.Context, kind LeaseKind, id int64, tok LeaseToken, until time.Time) error {
	var table string
	switch kind {
	case LeaseRepo:
		table = "repos"
	case LeaseFile:
		table = "files"
	case LeaseBlock:
		table = "blocks"
	case LeaseReference:
		table = "reference_files"
	default:
		return fmt.Errorf("store: renew lease: unknown lease kind %q", string(kind))
	}
	now := utc(time.Now())
	query := fmt.Sprintf(
		"UPDATE %s SET lease_until = ?, updated_at = ? WHERE id = ? AND lease_token = ? AND lease_until IS NOT NULL AND lease_until > ?",
		table)
	return execGuarded(ctx, s.db, "renew lease", id, query,
		utc(until), now, id, string(tok), now)
}

// claimOne runs a single-statement atomic claim (UPDATE ... WHERE id =
// (SELECT ... LIMIT 1) ... RETURNING *) and scans the leased row into dest,
// mapping no-row to ErrNoWork.
func claimOne(ctx context.Context, db bun.IDB, dest any, query string, args ...any) error {
	if err := db.NewRaw(query, args...).Scan(ctx, dest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoWork
		}
		return err
	}
	return nil
}
