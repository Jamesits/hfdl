package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

// RecoveryStats counts rows requeued by Recover, per queue.
type RecoveryStats struct {
	Repos      int64 // listing → pending
	Files      int64 // downloading → queued
	Verifying  int64 // verifying → downloaded
	Salvaging  int64 // salvaging → queued
	Blocks     int64 // active → pending
	JobFiles   int64 // installing → pending
	References int64 // hashing → pending
}

// Recover expires every stale lease (lease_until < now) and requeues each
// row to its machine's requeue point in one tx. Runs at startup and
// periodically; idempotent.
func (s *Store) Recover(ctx context.Context, now time.Time) (RecoveryStats, error) {
	var stats RecoveryStats
	err := s.inTx(ctx, "recover", func(tx bun.Tx) error {
		now = utc(now)
		requeue := []struct {
			dst  *int64
			stmt string
			args []any
		}{
			{&stats.Repos,
				"UPDATE repos SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? " +
					"WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?",
				[]any{string(RepoPending), now, string(RepoListing), now}},
			{&stats.Files,
				"UPDATE files SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? " +
					"WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?",
				[]any{string(FileQueued), now, string(FileDownloading), now}},
			{&stats.Verifying,
				"UPDATE files SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? " +
					"WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?",
				[]any{string(FileDownloaded), now, string(FileVerifying), now}},
			{&stats.Salvaging,
				"UPDATE files SET status = ?, updated_at = ? " +
					"WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?",
				[]any{string(FileQueued), now, string(FileSalvaging), now}},
			{&stats.Blocks,
				"UPDATE blocks SET status = ?, available_at = NULL, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? " +
					"WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?",
				[]any{string(BlockPending), now, string(BlockActive), now}},
			{&stats.JobFiles,
				"UPDATE job_files SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? " +
					"WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?",
				[]any{string(JobFilePending), now, string(JobFileInstalling), now}},
			{&stats.References,
				"UPDATE reference_files SET status = ?, lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = ? " +
					"WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?",
				[]any{string(RefPending), now, string(RefHashing), now}},
		}

		for _, r := range requeue {
			res, err := tx.ExecContext(ctx, r.stmt, r.args...)
			if err != nil {
				return fmt.Errorf("store: recover: %w", err)
			}
			*r.dst, _ = res.RowsAffected()
		}
		return nil
	})
	if err != nil {
		return RecoveryStats{}, err
	}
	return stats, nil
}
