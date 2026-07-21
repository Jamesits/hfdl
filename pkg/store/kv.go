package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// GetKV returns the value for key, "" with nil error when absent.
func (s *Store) GetKV(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, "SELECT value FROM kv WHERE key = ?", key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: get kv %q: %w", key, err)
	}
	return value, nil
}

// SetKV upserts a kv row.
func (s *Store) SetKV(ctx context.Context, key, value string) error {
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
		key, value); err != nil {
		return fmt.Errorf("store: set kv %q: %w", key, err)
	}
	return nil
}

// GetCaps implements fcio's CapsCache structurally; the key ("volcaps:<dev>")
// is chosen by the caller. nil, nil when absent.
func (s *Store) GetCaps(ctx context.Context, key string) ([]byte, error) {
	value, err := s.GetKV(ctx, key)
	if err != nil || value == "" {
		return nil, err
	}
	return []byte(value), nil
}

// PutCaps implements fcio's CapsCache structurally; caps is an opaque JSON
// blob owned by fcio.
func (s *Store) PutCaps(ctx context.Context, key string, caps []byte) error {
	return s.SetKV(ctx, key, string(caps))
}
