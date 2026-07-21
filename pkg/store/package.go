// Package store is hfdl's persistent state: a single-process SQLite database
// (modernc.org/sqlite, CGO-free) accessed through uptrace/bun. All queue and
// download state lives here as durable rows; workers lease rows and every
// mutation of a leased row is fenced by an unguessable LeaseToken (ULID).
//
// Design invariants:
//
//   - Open refuses network filesystems (WAL shared memory is unsafe there),
//     takes an exclusive advisory lock (<path>.lock), establishes
//     journal_mode=WAL once via an explicit PRAGMA, and configures
//     busy_timeout/foreign_keys/synchronous per-connection via DSN options so
//     every pooled connection is covered. PRAGMAs never live in migrations.
//   - Migrations are embedded SQL run by bun/migrate at Open; fresh DBs and
//     upgrades share one code path. Runtime is forward-only: a DB carrying a
//     migration unknown to this binary hard-fails with BinaryTooOldError.
//   - Claims are UPDATE ... RETURNING (or guarded UPDATE in one tx), never
//     read-then-update. Every mutation of a leased row is guarded
//     WHERE id=? AND status=? AND lease_token=? and asserts exactly one
//     affected row; 0 rows means the lease was lost (expired + reclaimed) and
//     surfaces as ErrFenced — the worker must abandon its work.
//   - Tokenless claims: MarkSalvaging flips status atomically but issues no
//     token. Rows whose lease_token IS NULL may be mutated with an empty
//     token by the tokenless rule (lease_token = tok OR (tok=” AND
//     lease_token IS NULL)). Salvaged files go salvaging→downloaded and are
//     re-hashed by the verifier like any downloaded file.
//   - Timestamps are stored UTC; all inbound times are normalized with .UTC().
//   - Block rows are ephemeral scheduling state; byte truth lives in
//     files.progress (fsynced bytes only, opaque []byte owned by transfer).
//
// lease_owner is a per-Open instance ULID identifying this hfdl process;
// lease_token is a per-claim ULID. RenewLease heartbeats lease_until;
// Recover requeues every expired lease to its row's requeue point in one tx.
//
// The package implements logging.LogSink (InsertLogs) and fcio's CapsCache
// (GetCaps/PutCaps) structurally; cmd wires the interfaces.
package store
