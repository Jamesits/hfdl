package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/otel"
	"github.com/jamesits/hfdl/pkg/throttle"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Destination modes for InstallRequest.DestMode.
const (
	DestModeCache    = "cache"
	DestModeLocalDir = "local-dir"
)

// tmpPrefix names in-progress local-dir materializations: the temp lives in
// the destination directory so the final rename stays on one filesystem.
const tmpPrefix = ".hfdl-tmp-"

// InstallRequest is one job_files row's worth of install work.
type InstallRequest struct {
	DestMode string // "cache" | "local-dir"
	DestDir  string // local-dir mode only
	CacheDir string // cache mode: HF cache root (models--org--name lives under it)

	RepoType  string // "model" | "dataset" | "space" (empty = model)
	RepoName  string // org/repo
	Revision  string
	CommitSHA string
	RepoPath  string // path inside repo
	BlobID    string
	Size      int64
}

// Installer materializes blobs from the Store into the HF cache layout or a
// local directory. Blobs are never moved out of the cache — installs link
// or copy so snapshots and later jobs keep working.
type Installer struct {
	s   *Store
	e   *fcio.Engine
	v   *fcio.VolumeSet
	d   *throttle.DutyLimiter
	log *slog.Logger

	tracer trace.Tracer

	// Seams (tests inject failures): link fallbacks, reflink, copy and the
	// namespace-durability fsync counter.
	symlinkFn  func(oldname, newname string) error
	linkFn     func(oldname, newname string) error
	reflinkFn  func(src, dst string) error
	copyFn     func(ctx context.Context, src, dst string, size int64, lockVolumes bool) error
	fsyncDirFn func(dir string) error
}

// NewInstaller builds an Installer. prov may be nil (noop providers).
func NewInstaller(s *Store, e *fcio.Engine, v *fcio.VolumeSet, d *throttle.DutyLimiter, log *slog.Logger, prov *otel.Providers) *Installer {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if prov == nil {
		prov = otel.Noop()
	}
	in := &Installer{
		s:          s,
		e:          e,
		v:          v,
		d:          d,
		log:        log,
		tracer:     prov.Tracer("hfdl.cache"),
		symlinkFn:  os.Symlink,
		linkFn:     os.Link,
		reflinkFn:  reflinkFile,
		fsyncDirFn: fsyncDir,
	}
	in.copyFn = in.copyFile
	return in
}

// Install returns the final absolute path of the installed file.
func (in *Installer) Install(ctx context.Context, r InstallRequest) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	blobPath, ok := in.s.HasBlob(r.BlobID)
	if !ok {
		return "", fmt.Errorf("cache: blob %s not published under %s", r.BlobID, in.s.Root())
	}
	switch r.DestMode {
	case DestModeCache:
		return in.installCache(ctx, r, blobPath)
	case DestModeLocalDir:
		return in.installLocalDir(ctx, r, blobPath)
	default:
		return "", fmt.Errorf("cache: unknown dest mode %q", r.DestMode)
	}
}

// modelDirName is huggingface_hub's per-repo cache directory name:
// <type>s--org--name (repo-type prefix pluralized, slashes doubled).
func modelDirName(repoType, repoName string) string {
	if repoType == "" {
		repoType = "model"
	}
	return repoType + "s--" + strings.ReplaceAll(repoName, "/", "--")
}

// installCache reproduces the huggingface_hub layout:
// <CacheDir>/<type>s--org--name/{refs/<revision>,snapshots/<sha>/<repoPath>}
// with the snapshot entry a relative symlink into blobs/, plus the empty
// .locks/<type>s--org--name/<blob_id>.lock hf leaves per downloaded file.
func (in *Installer) installCache(ctx context.Context, r InstallRequest, blobPath string) (string, error) {
	dirName := modelDirName(r.RepoType, r.RepoName)
	base := filepath.Join(r.CacheDir, dirName)

	lockDir := filepath.Join(r.CacheDir, ".locks", dirName)
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		return "", fmt.Errorf("cache: create locks dir: %w", err)
	}
	if err := createEmptyFile(filepath.Join(lockDir, r.BlobID+".lock")); err != nil {
		return "", err
	}
	if err := in.fsyncDirFn(lockDir); err != nil {
		return "", err
	}

	// refs/<revision> contains exactly the commit sha (no trailing newline,
	// like huggingface_hub's ref_path.write_text(commit_hash)).
	refPath, err := SafeJoin(filepath.Join(base, "refs"), r.Revision)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(refPath), 0o755); err != nil {
		return "", fmt.Errorf("cache: create refs dir: %w", err)
	}
	if err := writeFileSync(refPath, []byte(r.CommitSHA), 0o644); err != nil {
		return "", err
	}
	if err := in.fsyncDirFn(filepath.Dir(refPath)); err != nil {
		return "", err
	}

	final, err := SafeJoin(filepath.Join(base, "snapshots", r.CommitSHA), r.RepoPath)
	if err != nil {
		return "", err
	}
	parent := filepath.Dir(final)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", fmt.Errorf("cache: create snapshot dir: %w", err)
	}

	// Relative symlink into blobs/, same shape huggingface_hub computes with
	// os.path.relpath(blob_path, pointer_dir) — depth grows with nested
	// repo paths (../../../blobs/<id> for root files, one more ".." per
	// directory component).
	relTarget, err := filepath.Rel(parent, blobPath)
	if err != nil {
		return "", fmt.Errorf("cache: relative blob target: %w", err)
	}

	// Fallback chain symlink → hardlink → copy: symlinks may be unavailable
	// (EPERM on Windows without developer mode), and hardlinks fail
	// cross-device or on link-less filesystems.
	if err := in.placeSymlink(relTarget, final, blobPath); err != nil {
		in.log.DebugContext(ctx, "symlink unavailable, trying hardlink",
			"path", final, "err", err)
		if err := in.placeHardlink(blobPath, final); err != nil {
			in.log.DebugContext(ctx, "hardlink unavailable, copying",
				"path", final, "err", err)
			if err := in.installCopy(ctx, blobPath, final, r.Size, parent); err != nil {
				return "", err
			}
		}
	}
	if err := in.fsyncDirFn(parent); err != nil {
		return "", err
	}
	return final, nil
}

// installLocalDir materializes a real file at <DestDir>/<repoPath>:
// reflink where supported, else a streaming copy through the fcio engine
// with duty checkpoints and the VolumeSet RW lock held across both the blob
// (read) and destination (write) volumes. The temp file
// lives in the destination directory; fsync → atomic rename → parent-dir
// fsync makes the step durable, then the huggingface_hub metadata stamp is
// written for hf parity.
func (in *Installer) installLocalDir(ctx context.Context, r InstallRequest, blobPath string) (string, error) {
	dest, err := SafeJoin(r.DestDir, r.RepoPath)
	if err != nil {
		return "", err
	}
	parent := filepath.Dir(dest)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", fmt.Errorf("cache: create dest dir: %w", err)
	}
	tmp := tmpPath(parent)

	ctx, span := in.tracer.Start(ctx, "fcio.copy")
	defer span.End()
	span.SetAttributes(
		attribute.String("op", "install"),
		attribute.Int64("bytes", r.Size),
		attribute.String("path", r.RepoPath),
	)
	in.volumeAttrs(ctx, span, blobPath, parent)

	mode := "reflink"
	if err := in.reflinkFn(blobPath, tmp); err != nil {
		// A failed FICLONE may leave the just-created empty dst behind.
		_ = os.Remove(tmp)
		mode = "copy"
		in.log.DebugContext(ctx, "reflink unavailable, copying",
			"src", blobPath, "dst", dest, "err", err)
		if cerr := in.copyFn(ctx, blobPath, tmp, r.Size, true); cerr != nil {
			_ = os.Remove(tmp) // error return: don't leave a partial temp behind
			err := fmt.Errorf("cache: install copy %s: %w", dest, cerr)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return "", err
		}
	}
	span.SetAttributes(attribute.String("mode", mode))

	if err := syncFile(tmp); err != nil {
		_ = os.Remove(tmp)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	if err := replaceFile(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("cache: rename %s → %s: %w", tmp, dest, err)
	}
	if err := in.fsyncDirFn(parent); err != nil {
		return "", err
	}
	// hf bookkeeping parity: .gitignore + CACHEDIR.TAG + empty .lock, then
	// the per-file metadata stamp (whose parent-dir fsync also covers the
	// lock sibling).
	if err := in.writeLocalDirStamps(r.DestDir, r.RepoPath); err != nil {
		return "", err
	}
	if err := in.writeDownloadMetadata(r.DestDir, r.RepoPath, r.CommitSHA, r.BlobID); err != nil {
		return "", err
	}
	return dest, nil
}

// installCopy is the copy fallback shared by both modes: copy to a temp
// file in the destination directory, then atomic rename.
func (in *Installer) installCopy(ctx context.Context, src, final string, size int64, parent string) error {
	ctx, span := in.tracer.Start(ctx, "fcio.copy")
	defer span.End()
	span.SetAttributes(
		attribute.String("op", "install"),
		attribute.String("mode", "copy"),
		attribute.Int64("bytes", size),
	)
	in.volumeAttrs(ctx, span, src, parent)
	tmp := tmpPath(parent)
	if err := in.copyFn(ctx, src, tmp, size, false); err != nil {
		_ = os.Remove(tmp)
		err = fmt.Errorf("cache: install copy %s: %w", final, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if err := replaceFile(tmp, final); err != nil {
		_ = os.Remove(tmp)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("cache: rename %s → %s: %w", tmp, final, err)
	}
	return nil
}

// copyFile streams src → dst through the fcio engine readahead pipeline
// (offset-ordered chunks) with a duty checkpoint per chunk. When
// lockVolumes is set the VolumeSet RW lock covers both the source and
// destination volumes for the whole copy: read/write-mixed work on one
// HDD spindle is mutually exclusive, so the lock serializes them.
func (in *Installer) copyFile(ctx context.Context, src, dst string, size int64, lockVolumes bool) error {
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("cache: copy source %s: %w", src, err)
	}
	if lockVolumes && in.v != nil {
		readVol, err := in.v.VolumeOf(ctx, src)
		if err != nil {
			return fmt.Errorf("cache: volume of %s: %w", src, err)
		}
		writeVol, err := in.v.VolumeOf(ctx, dst)
		if err != nil {
			return fmt.Errorf("cache: volume of %s: %w", dst, err)
		}
		fs, err := fcio.ProbeFs(ctx, dst)
		if err != nil {
			fs = fcio.FsUnknown // conservative occupancy rules
		}
		release, err := in.v.AcquireRW(ctx, readVol, writeVol, fs)
		if err != nil {
			return err
		}
		defer release()
	}
	sf, err := in.e.Open(ctx, src, -1, fcio.Hints{Sequential: true})
	if err != nil {
		return err
	}
	defer sf.Close()
	df, err := in.e.Open(ctx, dst, size, fcio.Hints{})
	if err != nil {
		return err
	}
	defer df.Close()
	var calc throttle.DutyCalc
	err = in.e.ReadAll(ctx, sf, func(p []byte, off int64) error {
		if err := df.WriteUnaligned(p, off); err != nil {
			return err
		}
		if in.d != nil {
			if err := in.d.Checkpoint(ctx, &calc); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("cache: copy %s → %s: %w", src, dst, err)
	}
	if err := df.Fsync(); err != nil {
		return fmt.Errorf("cache: fsync %s: %w", dst, err)
	}
	return nil
}

// placeSymlink creates final as a relative symlink to relTarget. An
// existing entry that already designates the same blob is accepted
// (idempotent reinstall); a conflicting one is replaced.
func (in *Installer) placeSymlink(relTarget, final, blobPath string) error {
	err := in.symlinkFn(relTarget, final)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	if ok, serr := sameBlob(final, relTarget, blobPath); serr == nil && ok {
		return nil
	}
	if rerr := os.Remove(final); rerr != nil {
		return fmt.Errorf("cache: replace %s: %w", final, rerr)
	}
	return in.symlinkFn(relTarget, final)
}

// placeHardlink is placeSymlink for hardlinks (link-less-FS fallback).
func (in *Installer) placeHardlink(blobPath, final string) error {
	err := in.linkFn(blobPath, final)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	if ok, serr := sameBlob(final, "", blobPath); serr == nil && ok {
		return nil
	}
	if rerr := os.Remove(final); rerr != nil {
		return fmt.Errorf("cache: replace %s: %w", final, rerr)
	}
	return in.linkFn(blobPath, final)
}

// sameBlob reports whether final already designates blobPath: either a
// symlink with the exact expected target, or a hardlink to the same inode.
func sameBlob(final, relTarget, blobPath string) (bool, error) {
	fi, err := os.Lstat(final)
	if err != nil {
		return false, err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(final)
		if err != nil {
			return false, err
		}
		return target == relTarget, nil
	}
	bi, err := os.Stat(blobPath)
	if err != nil {
		return false, err
	}
	fi2, err := os.Stat(final)
	if err != nil {
		return false, err
	}
	return os.SameFile(bi, fi2), nil
}

// replaceFile renames src onto dst atomically, removing a stale dst first
// where the platform requires it (Windows rename refuses existing targets).
func replaceFile(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		if rerr := os.Remove(dst); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			return err
		}
		return os.Rename(src, dst)
	}
	return nil
}

// volumeAttrs records src/dst volume ids on an fcio.copy span.
func (in *Installer) volumeAttrs(ctx context.Context, span trace.Span, src, dstDir string) {
	if in.v == nil {
		return
	}
	if rv, err := in.v.VolumeOf(ctx, src); err == nil {
		span.SetAttributes(attribute.String("src_volume", string(rv)))
	}
	if wv, err := in.v.VolumeOf(ctx, dstDir); err == nil {
		span.SetAttributes(attribute.String("dst_volume", string(wv)))
	}
}

func tmpPath(dir string) string {
	return filepath.Join(dir, tmpPrefix+strconv.Itoa(os.Getpid())+"-"+strconv.FormatInt(time.Now().UnixNano(), 36))
}
