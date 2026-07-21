package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(t.Context(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		// t.Context() is already canceled in cleanup; Close must still run.
		if err := st.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return st
}

// seedListedRepo drives the real meta flow: enqueue → lease → pin sha →
// complete listing. Returns repo and job IDs.
func seedListedRepo(t *testing.T, s *Store, name string, files []FileEntry) (repoID, jobID int64) {
	t.Helper()
	ctx := t.Context()
	job := &Job{
		Repo:     &Repo{Name: name, Endpoint: "https://hf.co"},
		DestMode: DestModeCache,
		DestDir:  t.TempDir(),
	}
	if err := s.EnqueueJob(ctx, job); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}
	repo, tok, err := s.LeaseMeta(ctx, time.Now())
	if err != nil {
		t.Fatalf("LeaseMeta: %v", err)
	}
	if err := s.SetCommitSHA(ctx, repo.ID, tok, "deadbeefsha"); err != nil {
		t.Fatalf("SetCommitSHA: %v", err)
	}
	if err := s.CompleteListing(ctx, repo.ID, tok, files); err != nil {
		t.Fatalf("CompleteListing: %v", err)
	}
	return repo.ID, job.ID
}

// insertFile writes a file row directly (targeted setups), returning its id.
func insertFile(t *testing.T, s *Store, repoID int64, path string, size int64, status FileStatus) int64 {
	t.Helper()
	var id int64
	err := s.db.QueryRowContext(t.Context(),
		"INSERT INTO files (repo_id, path, size, sha256, is_lfs, status) VALUES (?, ?, ?, 'sha-'+?, 1, ?) RETURNING id",
		repoID, path, size, path, string(status)).Scan(&id)
	if err != nil {
		t.Fatalf("insertFile %s: %v", path, err)
	}
	return id
}

// fileStatus reads one file's status.
func fileStatus(t *testing.T, s *Store, id int64) FileStatus {
	t.Helper()
	var st string
	if err := s.db.QueryRowContext(t.Context(), "SELECT status FROM files WHERE id = ?", id).Scan(&st); err != nil {
		t.Fatalf("fileStatus: %v", err)
	}
	return FileStatus(st)
}

// leaseFile puts a file into downloading with real blocks and returns its token.
func leaseFile(t *testing.T, s *Store, fileID int64, nblocks int) LeaseToken {
	t.Helper()
	tok, err := s.LeaseFileForDownload(t.Context(), fileID, time.Now())
	if err != nil {
		t.Fatalf("LeaseFileForDownload: %v", err)
	}
	blocks := make([]Block, nblocks)
	for i := range blocks {
		blocks[i] = Block{Idx: i, Offset: int64(i) * 100, Length: 100}
	}
	if err := s.ReplacePendingBlocks(t.Context(), fileID, tok, blocks); err != nil {
		t.Fatalf("ReplacePendingBlocks: %v", err)
	}
	return tok
}

func TestOpenFreshMigrates(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()

	wantTables := []string{
		"repos", "jobs", "files", "job_files", "blocks", "upstreams",
		"reference_files", "endpoint_cooldowns", "logs", "kv",
		"bun_migrations", "bun_migration_locks",
	}
	for _, table := range wantTables {
		var n int
		if err := s.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&n); err != nil {
			t.Fatalf("sqlite_master: %v", err)
		}
		if n != 1 {
			t.Errorf("table %s missing after migrate", table)
		}
	}
	for _, index := range []string{"idx_files_status", "idx_blocks_lease", "idx_reference_files_size"} {
		var n int
		if err := s.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", index).Scan(&n); err != nil {
			t.Fatalf("sqlite_master: %v", err)
		}
		if n != 1 {
			t.Errorf("index %s missing after migrate", index)
		}
	}

	var journal string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		t.Fatalf("journal_mode: %v", err)
	}
	if journal != "wal" {
		t.Errorf("journal_mode = %q, want wal", journal)
	}

	// Migration lock released after Open.
	var locks int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM bun_migration_locks").Scan(&locks); err != nil {
		t.Fatalf("migration locks: %v", err)
	}
	if locks != 0 {
		t.Errorf("bun_migration_locks has %d rows after Open, want 0", locks)
	}

	// Reopening an up-to-date DB takes the same code path, zero pending.
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(ctx, s.path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer s2.Close(ctx) //nolint:errcheck
}

func TestSecondOpenFailsLocked(t *testing.T) {
	s := openTestStore(t)
	_, err := Open(t.Context(), s.path)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open err = %v, want ErrLocked", err)
	}
}

func TestForwardOnlyGuard(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO bun_migrations (name, group_id) VALUES ('9999_future_feature', 99)"); err != nil {
		t.Fatalf("inject future migration: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = Open(ctx, path)
	var tooOld *BinaryTooOldError
	if !errors.As(err, &tooOld) {
		t.Fatalf("Open err = %v, want BinaryTooOldError", err)
	}
	if len(tooOld.Unknown) != 1 || tooOld.Unknown[0] != "9999_future_feature" {
		t.Errorf("Unknown = %v", tooOld.Unknown)
	}
}

func TestStaleMigrationLockBroken(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Simulate a crashed migrator's stale lock row.
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO bun_migration_locks (table_name) VALUES ('bun_migrations')"); err != nil {
		t.Fatalf("inject stale lock: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open with stale migration lock: %v", err)
	}
	defer s2.Close(ctx) //nolint:errcheck
}

func TestTimestampsUTC(t *testing.T) {
	s := openTestStore(t)
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{{Path: "a.bin", Size: 10, GitOID: "x"}})
	var updatedAt time.Time
	if err := s.db.QueryRowContext(t.Context(), "SELECT updated_at FROM repos WHERE id = ?", repoID).Scan(&updatedAt); err != nil {
		t.Fatal(err)
	}
	if _, off := updatedAt.Zone(); off != 0 {
		t.Errorf("updated_at zone offset = %d, want 0 (UTC)", off)
	}
}
