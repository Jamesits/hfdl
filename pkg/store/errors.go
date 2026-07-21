package store

import (
	"errors"
	"fmt"
	"strings"
)

// ErrFenced reports a guarded mutation that matched 0 rows: the caller's
// lease token no longer owns the row (expired and reclaimed), the row is not
// in the expected status, or a claim target was taken by a racer. The worker
// must abandon its in-flight work for that row.
var ErrFenced = errors.New("store: fenced (guard matched 0 rows)")

// ErrNoWork is returned by claim methods (LeaseMeta, LeaseBlocks is exempt —
// it returns an empty slice — LeaseVerify, LeaseInstall, LeaseReferenceHash)
// when no row is currently leasable.
var ErrNoWork = errors.New("store: no leasable row")

// ErrLocked reports that the state database's advisory lock is held by
// another hfdl process: hfdl is single-process per state DB.
var ErrLocked = errors.New("store: state database is in use by another hfdl process")

// ErrLiveBlockLeases rejects TransitionFile(downloading→downloaded) while
// live block leases remain: a stale writer must never race the verifier.
var ErrLiveBlockLeases = errors.New("store: file still has live block leases")

// NetFSError rejects a state DB located on a network filesystem: SQLite WAL
// shared memory is unsafe on NFS/SMB/CIFS.
type NetFSError struct {
	Path string // directory that was probed
	Kind string // "nfs" | "smb" | "cifs"
}

func (e *NetFSError) Error() string {
	return fmt.Sprintf("store: state database directory %q is on a network filesystem (%s); "+
		"WAL is unsafe there — pass --state-db to place the state database on a local filesystem",
		e.Path, e.Kind)
}

// BinaryTooOldError hard-fails Open when the DB carries migrations unknown to
// this binary (applied by a newer hfdl). Forward-only: never a downgrade.
type BinaryTooOldError struct {
	Unknown []string // migration names present in the DB but not in the binary
}

func (e *BinaryTooOldError) Error() string {
	return fmt.Sprintf("store: state database has migrations newer than this binary (%s): "+
		"the binary is too old, upgrade hfdl", strings.Join(e.Unknown, ", "))
}

// fenced wraps ErrFenced with the operation that was fenced.
func fenced(op string, id int64) error {
	return fmt.Errorf("store: %s id=%d: %w", op, id, ErrFenced)
}
