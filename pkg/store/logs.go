package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jamesits/hfdl/pkg/logging"
)

// logInsertChunk bounds multi-row INSERT statements feeding the DB sink.
const logInsertChunk = 256

// InsertLogs implements logging.LogSink: best-effort batched insert from the
// async sink goroutine. Errors propagate so the sink can count drops.
func (s *Store) InsertLogs(ctx context.Context, rows []logging.LogRow) error {
	for start := 0; start < len(rows); start += logInsertChunk {
		end := start + logInsertChunk
		if end > len(rows) {
			end = len(rows)
		}
		logs := make([]Log, 0, end-start)
		for _, r := range rows[start:end] {
			logs = append(logs, Log{
				Ts:     utc(r.Time),
				Level:  r.Level,
				Source: r.Source,
				Msg:    r.Msg,
				Attrs:  r.Attrs,
			})
		}
		if _, err := s.db.NewInsert().Model(&logs).
			ExcludeColumn("id").
			Exec(ctx); err != nil {
			return fmt.Errorf("store: insert logs: %w", err)
		}
	}
	return nil
}

// PruneLogs keeps the keep newest log rows (startup retention).
func (s *Store) PruneLogs(ctx context.Context, keep int) error {
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT ?)",
		keep); err != nil {
		return fmt.Errorf("store: prune logs: %w", err)
	}
	return nil
}

// QueryLogs returns log rows at or above minLevel, at or after since,
// oldest first (hfdl logs inspection; the TUI reads the ring, not the DB).
func (s *Store) QueryLogs(ctx context.Context, minLevel int, since time.Time, limit int) ([]Log, error) {
	var logs []Log
	if err := s.db.NewSelect().Model(&logs).
		Where("level >= ?", minLevel).
		Where("ts >= ?", utc(since)).
		Order("id").
		Limit(limit).
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("store: query logs: %w", err)
	}
	return logs, nil
}
