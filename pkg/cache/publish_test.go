package cache

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func stagePart(t *testing.T, s *Store, fileID int64, data []byte) {
	t.Helper()
	if err := os.WriteFile(s.IncompletePath(fileID), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestPublish(t *testing.T) {
	s, _ := newTestStore(t)
	data := []byte("verified blob bytes")
	stagePart(t, s, 7, data)
	got, err := s.Publish(t.Context(), 7, "blobA")
	if err != nil {
		t.Fatal(err)
	}
	if want := s.BlobPath("blobA"); got != want {
		t.Errorf("Publish path = %q, want %q", got, want)
	}
	onDisk, err := os.ReadFile(got)
	if err != nil || !bytes.Equal(onDisk, data) {
		t.Errorf("blob content = %q, err=%v", onDisk, err)
	}
	if _, err := os.Lstat(s.IncompletePath(7)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf(".part still exists: %v", err)
	}
}

// Two publishers racing the same blob id must both succeed; the blob keeps
// the correct content and no staging file survives.
func TestPublishRaceEEXIST(t *testing.T) {
	s, _ := newTestStore(t)
	data := bytes.Repeat([]byte("x"), 4096)
	stagePart(t, s, 1, data)
	stagePart(t, s, 2, data)

	var wg sync.WaitGroup
	paths := make([]string, 2)
	errs := make([]error, 2)
	for i, id := range []int64{1, 2} {
		wg.Add(1)
		go func(i int, id int64) {
			defer wg.Done()
			paths[i], errs[i] = s.Publish(t.Context(), id, "blobX")
		}(i, id)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("publisher %d: %v", i, errs[i])
		}
	}
	if paths[0] != paths[1] || paths[0] != s.BlobPath("blobX") {
		t.Errorf("paths = %q, %q", paths[0], paths[1])
	}
	onDisk, err := os.ReadFile(paths[0])
	if err != nil || !bytes.Equal(onDisk, data) {
		t.Errorf("blob content mismatch, err=%v", err)
	}
	entries, err := os.ReadDir(filepath.Join(s.Root(), ".hfdl", "incomplete"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("%d staging files left", len(entries))
	}
}

// EEXIST with a different size is corruption, not a benign race.
func TestPublishSizeConflict(t *testing.T) {
	s, _ := newTestStore(t)
	if err := os.WriteFile(s.BlobPath("conf"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	stagePart(t, s, 9, []byte("newer"))
	_, err := s.Publish(t.Context(), 9, "conf")
	var cerr *BlobConflictError
	if !errors.As(err, &cerr) {
		t.Fatalf("err = %v, want *BlobConflictError", err)
	}
	if cerr.BlobID != "conf" || cerr.Want != 5 || cerr.Have != 3 {
		t.Errorf("BlobConflictError = %+v", cerr)
	}
}
