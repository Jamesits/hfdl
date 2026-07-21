package sched

import (
	"errors"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/store"
)

// TestPendingStateRealTimestamp plants a REAL future available_at row via
// the real store (the gpt2-soak case: modernc returns TIMESTAMP as string
// in raw queries) and asserts the probe decodes count + earliest, and that
// Lease waits the backoff out and returns the block.
func TestPendingStateRealTimestamp(t *testing.T) {
	hub := newFixtureHub(t, map[string][]byte{"x": []byte("y")})
	env := newTestEnv(t, hub)
	ctx := t.Context()

	// Seed a listed repo + queued file + one block, then requeue the block
	// into the future through the real store path.
	if err := env.manager.Submit(ctx, Job{Repo: "org/repo"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	repo, rtok, err := env.st.LeaseMeta(ctx, time.Now())
	if err != nil {
		t.Fatalf("LeaseMeta: %v", err)
	}
	if err := env.st.SetCommitSHA(ctx, repo.ID, rtok, hub.sha); err != nil {
		t.Fatalf("SetCommitSHA: %v", err)
	}
	if err := env.st.CompleteListing(ctx, repo.ID, rtok, []store.FileEntry{
		{Path: "b.bin", Size: 100, GitOID: gitBlobSHA1(make([]byte, 100))},
	}); err != nil {
		t.Fatalf("CompleteListing: %v", err)
	}
	var fileID int64
	if err := env.st.DB().QueryRowContext(ctx,
		"SELECT id FROM files WHERE path = 'b.bin'").Scan(&fileID); err != nil {
		t.Fatal(err)
	}
	ftok, err := env.st.LeaseFileForDownload(ctx, fileID, time.Now())
	if err != nil {
		t.Fatalf("LeaseFileForDownload: %v", err)
	}
	if err := env.st.ReplacePendingBlocks(ctx, fileID, ftok, []store.Block{
		{FileID: fileID, Idx: 0, Offset: 0, Length: 100},
	}); err != nil {
		t.Fatalf("ReplacePendingBlocks: %v", err)
	}
	lbs, err := env.st.LeaseBlocks(ctx, 1, store.BlockFilter{FileIDs: []int64{fileID}}, time.Now())
	if err != nil || len(lbs) != 1 {
		t.Fatalf("LeaseBlocks: %v (%d)", err, len(lbs))
	}
	avail := time.Now().Add(300 * time.Millisecond)
	if _, err := env.st.RequeueBlock(ctx, lbs[0].ID, lbs[0].Token, avail, errors.New("boom 503")); err != nil {
		t.Fatalf("RequeueBlock: %v", err)
	}

	leaser := newBlockLeaser(env.manager, fileID, ftok)
	pending, earliest, err := leaser.pendingState(ctx)
	if err != nil {
		t.Fatalf("pendingState: %v", err)
	}
	if pending != 1 {
		t.Errorf("pending = %d, want 1", pending)
	}
	if earliest == nil {
		t.Fatalf("earliest = nil, want ~%v", avail)
	}
	if d := earliest.Sub(avail); d < -time.Second || d > time.Second {
		t.Errorf("earliest = %v, want ~%v (Δ %v)", *earliest, avail, d)
	}

	// Lease must wait the backoff out and then return the block (not
	// ok=false, not an error).
	start := time.Now()
	b, ok, err := leaser.Lease(ctx, fileID)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if !ok {
		t.Fatalf("Lease ok=false with a real backoff row pending")
	}
	if b.Offset != 0 || b.Length != 100 {
		t.Errorf("leased block = %+v, want [0,100)", b)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Errorf("Lease returned after %v — did not wait out the backoff", elapsed)
	}
}
