package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/uptrace/bun/migrate"
)

//go:embed migrations
var migrationsFS embed.FS

// binaryMigrations returns the embedded migration set; a fresh instance per
// call because bun mutates migration state (status, group) on use.
func binaryMigrations() (*migrate.Migrations, error) {
	sub, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("store: embedded migrations: %w", err)
	}
	m := migrate.NewMigrations()
	if err := m.Discover(sub); err != nil {
		return nil, fmt.Errorf("store: discover embedded migrations: %w", err)
	}
	return m, nil
}

// migrate runs pending migrations. Forward-only at runtime: a DB carrying a
// migration unknown to this binary hard-fails (BinaryTooOldError), never a
// silent downgrade — .down.sql exists for dev tooling only.
func (s *Store) migrate(ctx context.Context) error {
	migrations, err := binaryMigrations()
	if err != nil {
		return err
	}
	migrator := migrate.NewMigrator(s.db, migrations)

	if err := migrator.Init(ctx); err != nil {
		return fmt.Errorf("store: init migration tables: %w", err)
	}

	applied, err := migrator.AppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("store: read applied migrations: %w", err)
	}
	known := make(map[string]bool)
	for _, m := range migrations.Sorted() {
		known[m.Name] = true
	}
	var unknown []string
	for _, m := range applied {
		if !known[m.Name] {
			unknown = append(unknown, m.Name)
		}
	}
	if len(unknown) > 0 {
		return &BinaryTooOldError{Unknown: unknown}
	}

	// The migration lock is taken explicitly; a stale lock left by a crashed
	// process is safe to break because the exclusive process lock (taken in
	// Open before this runs) guarantees no concurrent migrator.
	if err := migrator.Lock(ctx); err != nil {
		if uerr := migrator.Unlock(ctx); uerr != nil {
			return fmt.Errorf("store: break stale migration lock: %w", err)
		}
		if lerr := migrator.Lock(ctx); lerr != nil {
			return fmt.Errorf("store: acquire migration lock: %w", lerr)
		}
	}
	defer migrator.Unlock(ctx) //nolint:errcheck // best-effort release

	if _, err := migrator.Migrate(ctx); err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return nil
}
