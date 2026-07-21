package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	_ "modernc.org/sqlite" // CGO-free driver, registered as "sqlite"
)

const (
	driverName = "sqlite" // modernc.org/sqlite registration

	// Per-connection pragmas live in the DSN query so every pooled connection
	// is configured; journal_mode=WAL is persistent and set once in Open.
	// _txlock=immediate makes every tx take the write lock at BEGIN —
	// deferred read-then-write txs die with SQLITE_BUSY_SNAPSHOT (517)
	// under WAL write contention, which busy_timeout cannot repair. This is
	// the raw query; the path is joined via url.URL so spaces/?/# in the path
	// are percent-encoded rather than corrupting the DSN.
	dsnQuery = "_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)&_txlock=immediate"

	// leaseDuration bounds how long a claim survives without a RenewLease
	// heartbeat before Recover may requeue the row.
	leaseDuration = 30 * time.Second

	// maxVerifyFails bounds FailVerify resets before the file errors out.
	maxVerifyFails = 2
)

// Store is the handle to the state database. Safe for concurrent use;
// SQLite serializes writers and busy_timeout absorbs contention.
type Store struct {
	db    *bun.DB
	path  string
	lock  *os.File // <path>.lock, held for the process lifetime
	owner string   // per-Open instance ULID, used as lease_owner on claims

	closeOnce sync.Once
	closeErr  error
}

// dsn builds the modernc.org/sqlite file URI for path with the per-connection
// pragma query. url.URL percent-encodes the path, so spaces or reserved
// characters (?, #) in the state-db path never corrupt the DSN.
func dsn(path string) string {
	u := url.URL{Scheme: "file", Path: path, RawQuery: dsnQuery}
	return u.String()
}

// Open opens (creating if needed) and migrates the state database at path.
// It refuses network filesystems (WAL is unsafe there), takes an exclusive
// advisory lock so a second hfdl process fails fast, establishes WAL once,
// then runs all pending embedded migrations before returning.
func Open(ctx context.Context, path string) (*Store, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create state db directory: %w", err)
	}

	kind, netfs, err := probeNetFS(dir)
	if err != nil {
		return nil, fmt.Errorf("store: probe state db filesystem: %w", err)
	}
	if netfs {
		return nil, &NetFSError{Path: dir, Kind: kind}
	}

	lock, err := acquireLock(path + ".lock")
	if err != nil {
		return nil, err
	}

	sqldb, err := sql.Open(driverName, dsn(path))
	if err != nil {
		_ = releaseLock(lock)
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}

	// WAL is persistent in the DB file: set it once, explicitly, before
	// migrating — never from a migration (it cannot change inside a tx).
	if _, err := sqldb.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		_ = sqldb.Close()
		_ = releaseLock(lock)
		return nil, fmt.Errorf("store: enable WAL: %w", err)
	}

	s := &Store{
		db:    bun.NewDB(sqldb, sqlitedialect.New()),
		path:  path,
		lock:  lock,
		owner: ulid.Make().String(),
	}

	if err := s.migrate(ctx); err != nil {
		_ = s.db.Close()
		_ = releaseLock(lock)
		return nil, err
	}
	return s, nil
}

// Close releases the database and the process lock. Idempotent and safe to
// call concurrently: the teardown runs exactly once (sync.Once) and every
// caller sees the same result. The handle fields are not nil'd, so a
// concurrent DB()/query observes a closed handle (a returned error) rather
// than a nil-pointer panic.
func (s *Store) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		var err error
		if s.db != nil {
			err = s.db.Close()
		}
		if lerr := releaseLock(s.lock); err == nil {
			err = lerr
		}
		if err != nil {
			s.closeErr = fmt.Errorf("store: close: %w", err)
		}
	})
	return s.closeErr
}

// DB exposes the underlying handle for read-only queries not covered by the
// API (e.g. hfdl logs inspection); it must never be used to bypass guards.
func (s *Store) DB() *bun.DB { return s.db }

func utc(t time.Time) time.Time { return t.UTC() }
