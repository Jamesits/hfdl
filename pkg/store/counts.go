package store

import (
	"context"
	"fmt"
)

// Counts returns queue depths for the TUI: SELECT count(*) GROUP BY
// status per queue table, cheap enough to poll behind a short cache.
// Keys are "<table>.<status>"
// (e.g. "files.downloading", "blocks.pending").
func (s *Store) Counts(ctx context.Context) (map[string]int64, error) {
	tables := []string{"repos", "jobs", "files", "job_files", "blocks", "reference_files"}
	counts := make(map[string]int64, len(tables)*4)
	for _, table := range tables {
		rows, err := s.db.QueryContext(ctx,
			fmt.Sprintf("SELECT status, COUNT(*) FROM %s GROUP BY status", table))
		if err != nil {
			return nil, fmt.Errorf("store: counts %s: %w", table, err)
		}
		for rows.Next() {
			var status string
			var n int64
			if err := rows.Scan(&status, &n); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("store: counts %s: %w", table, err)
			}
			counts[table+"."+status] = n
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("store: counts %s: %w", table, err)
		}
		// Close error after a fully-consumed result set carries no
		// information the scan loop did not already surface.
		_ = rows.Close()
	}
	return counts, nil
}
