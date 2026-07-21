package store

import (
	"errors"
	"testing"
	"time"
)

func TestReferenceHashSizeGate(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{
		{Path: "target.bin", Size: 100, GitOID: "g", SHA256: "s", IsLFS: true},
	})
	targetID := mustFileID(t, s, repoID, "target.bin")
	if targetID == 0 {
		t.Fatal("no target")
	}

	refs := []ReferenceFile{
		{Path: "/ref/match", Size: 100, MtimeNs: 1, Dev: 8, Ino: 1001},
		{Path: "/ref/nomatch", Size: 999, MtimeNs: 1, Dev: 8, Ino: 1002},
	}
	if err := s.AddReferenceFiles(ctx, refs); err != nil {
		t.Fatalf("AddReferenceFiles: %v", err)
	}

	// Only the size-matching reference is ever leased; the mismatched one
	// stays stat-only.
	r, tok, err := s.LeaseReferenceHash(ctx, time.Now())
	if err != nil {
		t.Fatalf("LeaseReferenceHash: %v", err)
	}
	if r.Path != "/ref/match" || r.Status != RefHashing || tok == "" {
		t.Fatalf("LeaseReferenceHash = (%+v, %q)", r, tok)
	}
	if _, _, err := s.LeaseReferenceHash(ctx, time.Now()); !errors.Is(err, ErrNoWork) {
		t.Fatalf("LeaseReferenceHash 2 err = %v, want ErrNoWork (size gate holds)", err)
	}

	// Complete: hash stored, lease ended.
	if err := s.CompleteReferenceHash(ctx, r.ID, tok, "abc123", nil); err != nil {
		t.Fatalf("CompleteReferenceHash: %v", err)
	}
	var st, sha string
	if err := s.db.QueryRowContext(ctx,
		"SELECT status, sha256 FROM reference_files WHERE id = ?", r.ID).Scan(&st, &sha); err != nil {
		t.Fatal(err)
	}
	if st != string(RefHashed) || sha != "abc123" {
		t.Errorf("reference = (%s, %q), want (hashed, abc123)", st, sha)
	}

	// Wrong token is fenced.
	if err := s.CompleteReferenceHash(ctx, r.ID, tok, "zzz", nil); !errors.Is(err, ErrFenced) {
		t.Errorf("replay CompleteReferenceHash err = %v, want ErrFenced", err)
	}

	// Invalidate: stat changed → back to pending, hash dropped.
	if err := s.InvalidateReference(ctx, r.ID); err != nil {
		t.Fatalf("InvalidateReference: %v", err)
	}
	var shaPtr *string
	if err := s.db.QueryRowContext(ctx,
		"SELECT status, sha256 FROM reference_files WHERE id = ?", r.ID).Scan(&st, &shaPtr); err != nil {
		t.Fatal(err)
	}
	if st != string(RefPending) || shaPtr != nil {
		t.Errorf("after invalidate = (%s, %v), want (pending, nil)", st, shaPtr)
	}
}

func TestReferenceErrorPath(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	repoID, _ := seedListedRepo(t, s, "org/repo", []FileEntry{
		{Path: "t", Size: 10, GitOID: "g", SHA256: "s", IsLFS: true},
	})
	_ = repoID
	if err := s.AddReferenceFiles(ctx, []ReferenceFile{{Path: "/ref/x", Size: 10, MtimeNs: 1}}); err != nil {
		t.Fatal(err)
	}
	r, tok, err := s.LeaseReferenceHash(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteReferenceHash(ctx, r.ID, tok, "", errors.New("read error")); err != nil {
		t.Fatal(err)
	}
	var st, lastErr string
	if err := s.db.QueryRowContext(ctx,
		"SELECT status, last_error FROM reference_files WHERE id = ?", r.ID).Scan(&st, &lastErr); err != nil {
		t.Fatal(err)
	}
	if st != string(RefError) || lastErr != "read error" {
		t.Errorf("reference = (%s, %q), want (error, read error)", st, lastErr)
	}
}

func TestReferenceUpsertPreservesStatus(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	if err := s.AddReferenceFiles(ctx, []ReferenceFile{{Path: "/ref/y", Size: 5, MtimeNs: 1}}); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := s.db.QueryRowContext(ctx, "SELECT id FROM reference_files WHERE path = '/ref/y'").Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "UPDATE reference_files SET status = ?, sha256 = 'h' WHERE id = ?", string(RefHashed), id); err != nil {
		t.Fatal(err)
	}
	// Re-stat upsert refreshes stat fields but must not reset a hashed row.
	if err := s.AddReferenceFiles(ctx, []ReferenceFile{{Path: "/ref/y", Size: 5, MtimeNs: 2, Dev: 9, Ino: 55}}); err != nil {
		t.Fatal(err)
	}
	var st, sha string
	var mtime, dev, ino int64
	if err := s.db.QueryRowContext(ctx,
		"SELECT status, sha256, mtime_ns, dev, ino FROM reference_files WHERE id = ?", id).
		Scan(&st, &sha, &mtime, &dev, &ino); err != nil {
		t.Fatal(err)
	}
	if st != string(RefHashed) || sha != "h" {
		t.Errorf("upsert clobbered status: (%s, %q)", st, sha)
	}
	if mtime != 2 || dev != 9 || ino != 55 {
		t.Errorf("stat fields = (%d, %d, %d), want (2, 9, 55)", mtime, dev, ino)
	}
}
