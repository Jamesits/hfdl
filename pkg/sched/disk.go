package sched

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/throttle"
	"github.com/jamesits/hfdl/pkg/verify"
)

// The disk queue serializes all heavy disk IO behind the DutyLimiter
// and the VolumeSet R/W exclusion. Three item kinds, tried in order:
// reference-hash (so salvage matches appear before downloads finish),
// salvage-apply, then verify.

// diskWorkerCount is the IO depth by destination media: 2 on SSD, 1 on
// HDD, NetFS or anything unrecognized.
func diskWorkerCount(fs fcio.FsType) int {
	switch fs {
	case fcio.FsSSD:
		return 2
	default: // HDD, NetFS, Unknown: 1
		return 1
	}
}

func (m *Manager) diskWorker(ctx context.Context) {
	defer m.wg.Done()
	var calc throttle.DutyCalc
	for {
		if ctx.Err() != nil {
			return
		}
		done, err := m.tryReferenceHash(ctx, &calc)
		if err != nil {
			m.log.Warn("reference-hash failed", "err", err)
		}
		if done {
			continue
		}
		done, err = m.trySalvageApply(ctx, &calc)
		if err != nil {
			m.log.Warn("salvage-apply failed", "err", err)
		}
		if done {
			continue
		}
		done, err = m.tryVerify(ctx)
		if err != nil {
			m.log.Warn("verify failed", "err", err)
		}
		if done {
			continue
		}
		if !m.waitForWork(ctx, m.wakeDisk) {
			return
		}
	}
}

// tryReferenceHash leases one size-gated reference file and sha256s it.
func (m *Manager) tryReferenceHash(ctx context.Context, calc *throttle.DutyCalc) (bool, error) {
	ref, tok, err := m.st.LeaseReferenceHash(ctx, m.nowFn())
	if errors.Is(err, store.ErrNoWork) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// Stale-stat guard: a changed reference must not be hashed under its
	// old invalidation key (size+mtime_ns+dev+ino) — refresh the row instead.
	info, statErr := os.Stat(ref.Path)
	if statErr == nil {
		dev, ino := devIno(info)
		if info.Size() != ref.Size || info.ModTime().UnixNano() != ref.MtimeNs || dev != ref.Dev || ino != ref.Ino {
			if ierr := m.storeCall(ctx, func() error { return m.st.InvalidateReference(ctx, ref.ID) }); ierr != nil {
				return true, ierr
			}
			dev2, ino2 := devIno(info)
			if aerr := m.st.AddReferenceFiles(ctx, []store.ReferenceFile{{
				Path: ref.Path, Size: info.Size(), MtimeNs: info.ModTime().UnixNano(), Dev: dev2, Ino: ino2,
			}}); aerr != nil {
				return true, aerr
			}
			return true, nil
		}
	}

	hbCtx, hbStop := context.WithCancel(ctx)
	defer hbStop()
	go m.heartbeat(hbCtx, func(until time.Time) error {
		return m.st.RenewLease(ctx, store.LeaseReference, ref.ID, tok, until)
	}, nil)

	complete := func(sha string, cause error) (bool, error) {
		if cerr := m.storeCall(ctx, func() error {
			return m.st.CompleteReferenceHash(ctx, ref.ID, tok, sha, cause)
		}); cerr != nil {
			return true, cerr
		}
		wake(m.wakeDownload) // new salvage matches may exist
		return true, cause
	}

	if statErr != nil {
		return complete("", statErr)
	}

	// VolumeSet read exclusion: hashing must not share an HDD spindle with
	// a running copy: read and read+write on the same spindle are mutually
	// exclusive.
	release, err := m.acquireRead(ctx, ref.Path)
	if err != nil {
		return complete("", err)
	}
	defer release()

	f, err := m.cfg.Engine.Open(ctx, ref.Path, -1, fcio.Hints{Sequential: true})
	if err != nil {
		return complete("", err)
	}
	defer f.Close()

	sha, err := m.cfg.Verifier.Hash(ctx, f, ref.Size, true) // reference hashes are always sha256
	if err != nil {
		return complete("", err)
	}
	m.log.Debug("reference hashed", "path", ref.Path, "sha256", sha)
	return complete(sha, nil)
}

// trySalvageApply claims one 'salvaging' file and copies its matching
// reference into the cache staging file: R/W-mixed, so the VolumeSet lock
// covers both the reference and cache volumes. Durable-only order:
// copy → fsync → SaveProgress(full interval) → transition to downloaded.
func (m *Manager) trySalvageApply(ctx context.Context, calc *throttle.DutyCalc) (bool, error) {
	f := m.pickSalvagingFile(ctx)
	if f == nil {
		return false, nil
	}
	defer m.releaseSalvageClaim(f.ID)

	ref, err := m.findSalvageMatch(ctx, f)
	if err != nil {
		return true, err
	}
	if ref == nil {
		// The match disappeared (reference invalidated between gate and
		// apply): hand the file back to the download queue.
		if terr := m.storeCall(ctx, func() error {
			return m.st.TransitionFile(ctx, f.ID, "", store.FileSalvaging, store.FileQueued, nil)
		}); terr != nil {
			return true, terr
		}
		wake(m.wakeDownload)
		return true, nil
	}

	dstPath := m.cfg.Cache.IncompletePath(f.ID)
	release, err := m.acquireRW(ctx, ref.Path, dstPath)
	if err != nil {
		return true, err
	}
	defer release()

	size, err := m.copyFile(ctx, calc, ref.Path, dstPath, f.Size)
	if err != nil {
		if m.noteIOError(ctx, err) {
			return true, nil // paused; row stays 'salvaging', retried later
		}
		// A corrupt/vanishing reference invalidates the salvage, not the
		// file: back to the download queue.
		m.log.Warn("salvage copy failed, falling back to download",
			"file_id", f.ID, "reference", ref.Path, "err", err)
		if terr := m.storeCall(ctx, func() error {
			return m.st.TransitionFile(ctx, f.ID, "", store.FileSalvaging, store.FileQueued, err)
		}); terr != nil {
			return true, terr
		}
		wake(m.wakeDownload)
		return true, nil
	}

	// flush → fsync → persist (the copy is the flush).
	dst, err := m.cfg.Engine.Open(ctx, dstPath, -1, fcio.Hints{})
	if err != nil {
		return true, err
	}
	if err := dst.Fsync(); err != nil {
		_ = dst.Close()
		if m.noteIOError(ctx, err) {
			return true, nil
		}
		return true, err
	}
	if err := dst.Close(); err != nil {
		return true, err
	}

	blob := fullIntervalBlob(size)
	if err := m.st.SaveProgress(ctx, f.ID, "", blob); err != nil {
		return true, err
	}
	if err := m.storeCall(ctx, func() error {
		return m.st.TransitionFile(ctx, f.ID, "", store.FileSalvaging, store.FileDownloaded, nil)
	}); err != nil {
		return true, err
	}
	m.cfg.Stats.AddSalvaged(size)
	m.salvageBytes.Add(ctx, size, salvageAttr)
	m.log.Info("file salvaged from reference", "file_id", f.ID, "path", f.Path,
		"reference", ref.Path, "bytes", size)
	wake(m.wakeDisk)
	return true, nil
}

// fullIntervalBlob encodes the salvage progress record: the whole file as
// one [0,size) interval in transfer's pinned serde format ("HFDP" magic |
// version 1 | uvarint size | uvarint count=1 | delta-varints 0, size).
// IntervalSet's size field is unexported, so the one-interval record is
// encoded inline — the checkpoint serde format is fixed: HFDP magic,
// version byte, uvarint ranges.
func fullIntervalBlob(size int64) []byte {
	out := []byte{'H', 'F', 'D', 'P', 1}
	out = binary.AppendUvarint(out, uint64(size))
	out = binary.AppendUvarint(out, 1)
	out = binary.AppendUvarint(out, 0)
	out = binary.AppendUvarint(out, uint64(size))
	return out
}

// copyFile streams src → dst through the fcio engine with per-chunk duty
// checkpoints. Returns the number of bytes written.
func (m *Manager) copyFile(ctx context.Context, calc *throttle.DutyCalc, src, dst string, size int64) (int64, error) {
	srcF, err := m.cfg.Engine.Open(ctx, src, -1, fcio.Hints{Sequential: true})
	if err != nil {
		return 0, fmt.Errorf("sched: copy open src %s: %w", src, err)
	}
	defer srcF.Close()
	dstF, err := m.cfg.Engine.Open(ctx, dst, size, fcio.Hints{})
	if err != nil {
		return 0, fmt.Errorf("sched: copy open dst %s: %w", dst, err)
	}
	defer dstF.Close()

	var written int64
	err = m.cfg.Engine.ReadAll(ctx, srcF, func(p []byte, off int64) error {
		if err := dstF.WriteUnaligned(p, off); err != nil {
			return err
		}
		written += int64(len(p))
		return m.cfg.Duty.Checkpoint(ctx, calc)
	})
	if err != nil {
		return written, err
	}
	if written != size {
		return written, fmt.Errorf("sched: copy %s: size changed during copy (%d != %d)", src, written, size)
	}
	return written, nil
}

// tryVerify leases one downloaded file through the verify pipeline:
// fsync → de-sparse → fsync → hash → publish → cached.
func (m *Manager) tryVerify(ctx context.Context) (bool, error) {
	f, tok, err := m.st.LeaseVerify(ctx, m.nowFn())
	if errors.Is(err, store.ErrNoWork) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	hbCtx, hbStop := context.WithCancel(ctx)
	defer hbStop()
	go m.heartbeat(hbCtx, func(until time.Time) error {
		return m.st.RenewLease(ctx, store.LeaseFile, f.ID, tok, until)
	}, nil)

	if verr := m.verifyFile(ctx, f); verr != nil {
		if m.noteIOError(ctx, verr) {
			// Not a hash failure: back to 'downloaded', retried after the
			// ENOSPC pause lifts.
			if terr := m.storeCall(ctx, func() error {
				return m.st.TransitionFile(ctx, f.ID, tok, store.FileVerifying, store.FileDownloaded, verr)
			}); terr != nil && !errors.Is(terr, store.ErrFenced) {
				return true, terr
			}
			return true, nil
		}
		var mm *verify.MismatchError
		if errors.As(verr, &mm) {
			if ferr := m.storeCall(ctx, func() error { return m.st.FailVerify(ctx, f.ID, tok, verr) }); ferr != nil {
				return true, ferr
			}
			m.log.Warn("hash mismatch, file requeued or errored", "file_id", f.ID, "path", f.Path, "err", verr)
			m.afterFailVerify(ctx, f)
			wake(m.wakeDownload)
			return true, nil
		}
		// IO/infra failure: same requeue ladder as a mismatch (bounded by
		// the store's verify_fails counter).
		if ferr := m.storeCall(ctx, func() error { return m.st.FailVerify(ctx, f.ID, tok, verr) }); ferr != nil {
			return true, ferr
		}
		m.afterFailVerify(ctx, f)
		wake(m.wakeDownload)
		return true, nil
	}

	blobID := blobID(f)
	cachePath, err := m.cfg.Cache.Publish(ctx, f.ID, blobID)
	if err != nil {
		return true, fmt.Errorf("sched: publish file %d: %w", f.ID, err)
	}
	if err := m.storeCall(ctx, func() error { return m.st.CompleteVerify(ctx, f.ID, tok, cachePath) }); err != nil {
		return true, err
	}
	m.filesCompleted.Add(ctx, 1)
	m.log.Info("file verified and published", "file_id", f.ID, "path", f.Path, "blob", blobID)
	wake(m.wakeInstall)
	return true, nil
}

// verifyFile runs the physical-layout steps of the verify pipeline.
func (m *Manager) verifyFile(ctx context.Context, f *store.File) error {
	path := m.cfg.Cache.IncompletePath(f.ID)
	release, err := m.acquireRead(ctx, path)
	if err != nil {
		return err
	}
	defer release()

	cf, err := m.cfg.Engine.Open(ctx, path, -1, fcio.Hints{Sequential: true})
	if err != nil {
		return err
	}
	defer cf.Close()

	if err := cf.Fsync(); err != nil {
		return err
	}
	if _, _, err := m.cfg.Verifier.DeSparse(ctx, cf, f.Size); err != nil {
		return err
	}
	if err := cf.Fsync(); err != nil {
		return err
	}
	return m.cfg.Verifier.Verify(ctx, cf, f.Size, f.IsLFS, blobID(f))
}

// afterFailVerify checks whether the store's bounded ladder errored the
// file terminally (≥2 failures) and cascades to its jobs.
func (m *Manager) afterFailVerify(ctx context.Context, f *store.File) {
	// A verify failure after salvage means the reference's recorded hash
	// lied (it changed after hashing): drop those hash rows so the next
	// pass re-hashes instead of re-salvaging the same bad bytes forever.
	// A network-corruption mismatch invalidates nothing but a re-hash,
	// which is cheap and rare.
	if f.IsLFS && f.SHA256 != "" {
		var ids []int64
		if err := m.st.DB().NewSelect().Model((*store.ReferenceFile)(nil)).
			Column("id").
			Where("status = ?", string(store.RefHashed)).
			Where("size = ?", f.Size).
			Where("sha256 = ?", f.SHA256).
			Scan(ctx, &ids); err == nil {
			for _, id := range ids {
				if ierr := m.st.InvalidateReference(ctx, id); ierr != nil {
					m.log.Warn("invalidate suspect reference failed", "id", id, "err", ierr)
				}
			}
			if len(ids) > 0 {
				m.log.Info("references invalidated after verify failure", "file_id", f.ID, "count", len(ids))
			}
		}
	}

	var status string
	if err := m.st.DB().QueryRowContext(ctx,
		"SELECT status FROM files WHERE id = ?", f.ID).Scan(&status); err != nil {
		return
	}
	if status != string(store.FileError) {
		return
	}
	if err := m.storeCall(ctx, func() error {
		return m.st.FailJobFilesForFile(ctx, f.ID, fmt.Errorf("sched: file %s: verify failed", f.Path))
	}); err != nil {
		m.log.Warn("fail job files after verify failure failed", "file_id", f.ID, "err", err)
	}
	m.filesFailed.Add(ctx, 1)
	m.recordError(fmt.Errorf("sched: file %s: verify failed", f.Path))
}

// pickSalvagingFile claims one 'salvaging' file in memory (the claim is
// tokenless by design — the status flip itself is the exclusion, and hfdl
// is single-process per state DB).
func (m *Manager) pickSalvagingFile(ctx context.Context) *store.File {
	var files []store.File
	if err := m.st.DB().NewSelect().Model(&files).
		Where("status = ?", string(store.FileSalvaging)).
		Order("id").
		Limit(8).
		Scan(ctx); err != nil {
		return nil
	}
	m.salvageMu.Lock()
	defer m.salvageMu.Unlock()
	for i := range files {
		if !m.salvageClaim[files[i].ID] {
			m.salvageClaim[files[i].ID] = true
			return &files[i]
		}
	}
	return nil
}

func (m *Manager) releaseSalvageClaim(fileID int64) {
	m.salvageMu.Lock()
	delete(m.salvageClaim, fileID)
	m.salvageMu.Unlock()
}

// acquireRead takes the VolumeSet read slot for a path.
func (m *Manager) acquireRead(ctx context.Context, path string) (func(), error) {
	if m.cfg.Volumes == nil {
		return func() {}, nil
	}
	vol, err := m.cfg.Volumes.VolumeOf(ctx, path)
	if err != nil {
		return nil, err
	}
	fs, err := fcio.ProbeFs(ctx, path)
	if err != nil {
		fs = fcio.FsUnknown
	}
	return m.cfg.Volumes.AcquireRead(ctx, vol, fs)
}

// acquireRW takes the VolumeSet mixed R/W lock across both volumes.
func (m *Manager) acquireRW(ctx context.Context, readPath, writePath string) (func(), error) {
	if m.cfg.Volumes == nil {
		return func() {}, nil
	}
	rv, err := m.cfg.Volumes.VolumeOf(ctx, readPath)
	if err != nil {
		return nil, err
	}
	wv, err := m.cfg.Volumes.VolumeOf(ctx, writePath)
	if err != nil {
		return nil, err
	}
	fs, err := fcio.ProbeFs(ctx, writePath)
	if err != nil {
		fs = fcio.FsUnknown
	}
	return m.cfg.Volumes.AcquireRW(ctx, rv, wv, fs)
}
