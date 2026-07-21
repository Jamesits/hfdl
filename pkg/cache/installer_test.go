package cache

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/otel"
	"github.com/jamesits/hfdl/pkg/throttle"
)

const (
	testSHA    = "1234567890abcdef1234567890abcdef12345678"
	testBlobID = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
)

var testBlobData = []byte("blob payload " + strings.Repeat("0123456789", 2000))

// newInstallEnv returns a store with one published blob and an installer
// wired to real fcio engine, VolumeSet and duty limiter.
func newInstallEnv(t *testing.T) (*Store, *Installer) {
	t.Helper()
	s, e := newTestStore(t)
	in := NewInstaller(s, e, fcio.NewVolumeSet(), throttle.NewDutyLimiter(100, throttle.MediaSSD), discardLogger(), otel.Noop())
	stagePart(t, s, 1, testBlobData)
	if _, err := s.Publish(t.Context(), 1, testBlobID); err != nil {
		t.Fatal(err)
	}
	return s, in
}

func cacheRequest(cacheDir, repoPath string) InstallRequest {
	return InstallRequest{
		DestMode:  DestModeCache,
		CacheDir:  cacheDir,
		RepoType:  "model",
		RepoName:  "org/repo",
		Revision:  "main",
		CommitSHA: testSHA,
		RepoPath:  repoPath,
		BlobID:    testBlobID,
		Size:      int64(len(testBlobData)),
	}
}

func localRequest(destDir, repoPath string) InstallRequest {
	r := cacheRequest("", repoPath)
	r.DestMode = DestModeLocalDir
	r.DestDir = destDir
	r.CacheDir = ""
	return r
}

func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(d.Name(), tmpPrefix) {
			t.Errorf("temp file left behind: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestInstallCacheModeLayout(t *testing.T) {
	s, in := newInstallEnv(t)
	cacheDir := s.Root() // production: the store root IS the HF cache dir

	final, err := in.Install(t.Context(), cacheRequest(cacheDir, "a/b.txt"))
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(cacheDir, "models--org--repo")

	// refs/main contains exactly the commit sha, no trailing newline.
	ref, err := os.ReadFile(filepath.Join(base, "refs", "main"))
	if err != nil {
		t.Fatal(err)
	}
	if string(ref) != testSHA {
		t.Errorf("refs/main = %q, want %q", ref, testSHA)
	}

	// snapshots/<sha>/a/b.txt is a relative symlink into blobs/ with the
	// correct depth for the nested repo path.
	wantFinal := filepath.Join(base, "snapshots", testSHA, "a", "b.txt")
	if final != wantFinal {
		t.Errorf("final = %q, want %q", final, wantFinal)
	}
	fi, err := os.Lstat(final)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("%s is not a symlink (mode %v)", final, fi.Mode())
	}
	target, err := os.Readlink(final)
	if err != nil {
		t.Fatal(err)
	}
	if want := "../../../../blobs/" + testBlobID; target != want {
		t.Errorf("symlink target = %q, want %q", target, want)
	}
	content, err := os.ReadFile(final)
	if err != nil || string(content) != string(testBlobData) {
		t.Errorf("resolved content mismatch, err=%v", err)
	}

	// Root-level file: one less "..".
	final2, err := in.Install(t.Context(), cacheRequest(cacheDir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	target2, err := os.Readlink(final2)
	if err != nil {
		t.Fatal(err)
	}
	if want := "../../../blobs/" + testBlobID; target2 != want {
		t.Errorf("symlink target = %q, want %q", target2, want)
	}

	// Reinstalling the same file is idempotent.
	if _, err := in.Install(t.Context(), cacheRequest(cacheDir, "a/b.txt")); err != nil {
		t.Fatalf("reinstall: %v", err)
	}
	if _, err := os.ReadFile(final); err != nil {
		t.Fatal(err)
	}

	// hf leaves one empty lock per downloaded file, named by blob id under
	// the type-prefixed model dir.
	lock, err := os.Stat(filepath.Join(cacheDir, ".locks", "models--org--repo", testBlobID+".lock"))
	if err != nil {
		t.Fatal(err)
	}
	if lock.Size() != 0 {
		t.Errorf("lock size = %d, want empty", lock.Size())
	}
	// The HF cache root has no .gitignore (that is local-dir only).
	if _, err := os.Lstat(filepath.Join(cacheDir, ".gitignore")); err == nil {
		t.Error("cache root must not get a .gitignore")
	}
	if _, err := os.Lstat(filepath.Join(base, ".gitignore")); err == nil {
		t.Error("model dir must not get a .gitignore")
	}
}

func TestInstallCacheFallbackChain(t *testing.T) {
	s, in := newInstallEnv(t)
	cacheDir := s.Root()

	// Symlink EPERM → hardlink to the same inode.
	in.symlinkFn = func(oldname, newname string) error { return os.ErrPermission }
	final, err := in.Install(t.Context(), cacheRequest(cacheDir, "a/b.txt"))
	if err != nil {
		t.Fatal(err)
	}
	blobInfo, err := os.Stat(s.BlobPath(testBlobID))
	if err != nil {
		t.Fatal(err)
	}
	finalInfo, err := os.Stat(final)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(blobInfo, finalInfo) {
		t.Fatal("hardlink fallback did not produce the same inode")
	}

	// Symlink + hardlink EPERM → streaming copy with equal content on a
	// different inode.
	in.linkFn = func(oldname, newname string) error { return os.ErrPermission }
	final2, err := in.Install(t.Context(), cacheRequest(cacheDir, "a/c.txt"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(final2)
	if err != nil || string(content) != string(testBlobData) {
		t.Fatalf("copy fallback content mismatch, err=%v", err)
	}
	final2Info, err := os.Stat(final2)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(blobInfo, final2Info) {
		t.Fatal("copy fallback must not share the blob inode")
	}
	assertNoTempFiles(t, filepath.Join(cacheDir, "models--org--repo"))
}

func TestInstallLocalDir(t *testing.T) {
	s, in := newInstallEnv(t)
	destDir := t.TempDir()

	var fsyncs atomic.Int64
	real := in.fsyncDirFn
	in.fsyncDirFn = func(dir string) error {
		fsyncs.Add(1)
		return real(dir)
	}

	final, err := in.Install(t.Context(), localRequest(destDir, "sub/dir/file.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(destDir, "sub", "dir", "file.bin"); final != want {
		t.Errorf("final = %q, want %q", final, want)
	}
	content, err := os.ReadFile(final)
	if err != nil || string(content) != string(testBlobData) {
		t.Fatalf("dest content mismatch, err=%v", err)
	}
	if got := fsyncs.Load(); got < 2 {
		t.Errorf("parent-dir fsyncs = %d, want >= 2 (dest dir + metadata dir)", got)
	}
	assertNoTempFiles(t, destDir)

	// huggingface_hub bookkeeping under .cache/huggingface/.
	hfDir := filepath.Join(destDir, ".cache", "huggingface")
	gitignore, err := os.ReadFile(filepath.Join(hfDir, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	if string(gitignore) != "*" {
		t.Errorf(".gitignore = %q, want exactly %q", gitignore, "*")
	}
	tag, err := os.ReadFile(filepath.Join(hfDir, "CACHEDIR.TAG"))
	if err != nil {
		t.Fatal(err)
	}
	// Literal bytes pinned against hf 1.24.0's real file (line 4 is
	// "#\thttps://..." — hash THEN tab), not the package const.
	wantTag := "Signature: 8a477f597d28d172789f06886806bc55\n" +
		"# This file is a cache directory tag created by huggingface_hub.\n" +
		"# For information about cache directory tags, see:\n" +
		"#\thttps://bford.info/cachedir/\n"
	if string(tag) != wantTag {
		t.Errorf("CACHEDIR.TAG = %q, want %q", tag, wantTag)
	}
	lock, err := os.Stat(filepath.Join(hfDir, "download", "sub", "dir", "file.bin.lock"))
	if err != nil {
		t.Fatal(err)
	}
	if lock.Size() != 0 {
		t.Errorf("lock size = %d, want empty", lock.Size())
	}

	// huggingface_hub metadata stamp: sha, etag, unix timestamp float.
	meta, err := os.ReadFile(filepath.Join(destDir, ".cache", "huggingface", "download", "sub", "dir", "file.bin.metadata"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(meta), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("metadata lines = %v", lines)
	}
	if lines[0] != testSHA {
		t.Errorf("metadata[0] = %q, want commit sha %q", lines[0], testSHA)
	}
	if lines[1] != testBlobID {
		t.Errorf("metadata[1] = %q, want etag %q", lines[1], testBlobID)
	}
	ts, err := strconv.ParseFloat(lines[2], 64)
	if err != nil {
		t.Fatalf("metadata[2] = %q not a float: %v", lines[2], err)
	}
	if delta := time.Since(time.Unix(int64(ts), 0)); delta < -time.Minute || delta > time.Minute {
		t.Errorf("metadata timestamp off by %v", delta)
	}
	_ = s
}

// The copy path runs under the real VolumeSet with the RW lock covering
// both the blob and destination volumes.
func TestInstallLocalDirCopyPath(t *testing.T) {
	_, in := newInstallEnv(t)
	in.reflinkFn = func(src, dst string) error { return errors.New("no reflink") }
	destDir := t.TempDir()
	final, err := in.Install(t.Context(), localRequest(destDir, "f.bin"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(final)
	if err != nil || string(content) != string(testBlobData) {
		t.Fatalf("copy content mismatch, err=%v", err)
	}
	assertNoTempFiles(t, destDir)
}

// A crash mid-copy must leave the temp file, never a partial destination.
func TestInstallLocalDirCopyFailure(t *testing.T) {
	_, in := newInstallEnv(t)
	in.reflinkFn = func(src, dst string) error { return errors.New("no reflink") }
	in.copyFn = func(ctx context.Context, src, dst string, size int64, lockVolumes bool) error {
		if err := os.WriteFile(dst, []byte("partial"), 0o644); err != nil {
			t.Error(err)
		}
		return errors.New("disk died mid-copy")
	}
	destDir := t.TempDir()
	dest := filepath.Join(destDir, "file.bin")
	if _, err := in.Install(t.Context(), localRequest(destDir, "file.bin")); err == nil {
		t.Fatal("want copy failure")
	}
	if _, err := os.Lstat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("partial file visible at dest: %v", err)
	}
	// The failure was a returned error (not a crash), so the temp is cleaned.
	assertNoTempFiles(t, destDir)
}

// Reflink success must skip the copy entirely.
func TestInstallLocalDirReflinkSuccess(t *testing.T) {
	_, in := newInstallEnv(t)
	var copied atomic.Bool
	in.copyFn = func(ctx context.Context, src, dst string, size int64, lockVolumes bool) error {
		copied.Store(true)
		return errors.New("copy must not run")
	}
	in.reflinkFn = func(src, dst string) error {
		data, err := os.ReadFile(src)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, data, 0o644)
	}
	destDir := t.TempDir()
	final, err := in.Install(t.Context(), localRequest(destDir, "f.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if copied.Load() {
		t.Error("copy ran despite reflink success")
	}
	if content, err := os.ReadFile(final); err != nil || string(content) != string(testBlobData) {
		t.Errorf("reflink content mismatch, err=%v", err)
	}
}

func TestInstallErrors(t *testing.T) {
	_, in := newInstallEnv(t)
	if _, err := in.Install(t.Context(), localRequest(t.TempDir(), "../evil")); err == nil {
		t.Error("path escape accepted")
	}
	r := localRequest(t.TempDir(), "f.bin")
	r.BlobID = "missing"
	if _, err := in.Install(t.Context(), r); err == nil {
		t.Error("missing blob accepted")
	}
	r = localRequest(t.TempDir(), "f.bin")
	r.DestMode = "weird"
	if _, err := in.Install(t.Context(), r); err == nil {
		t.Error("unknown dest mode accepted")
	}
}
