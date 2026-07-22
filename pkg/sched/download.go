package sched

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/store"
	"github.com/jamesits/hfdl/pkg/transfer"
	"github.com/jamesits/hfdl/pkg/xet"
)

// downloadOrchestrator keeps at most Limits.MaxWorkers files in
// 'downloading': it runs the salvage gate, prepares the transfer
// task (progress load → re-chunk → pending blocks) and hands each file to
// one transfer.Downloader.Run in its own goroutine.
func (m *Manager) downloadOrchestrator(ctx context.Context) {
	defer m.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := m.waitResumable(ctx); err != nil {
			return
		}
		if m.cfg.Offline {
			// Nothing to download: offline serves come from the cache
			// layout (wireListedJob / processRepoOffline); unservable
			// files already failed terminally there.
			if !m.waitForWork(ctx, m.wakeDownload) {
				return
			}
			continue
		}

		limits := m.currentLimits()
		maxWorkers := limits.MaxWorkers
		if maxWorkers < 1 {
			maxWorkers = 1
		}
		m.activeMu.Lock()
		free := maxWorkers - len(m.active)
		m.activeMu.Unlock()
		if free <= 0 {
			if !m.waitForWork(ctx, m.wakeDownload) {
				return
			}
			continue
		}

		files, err := m.st.NextDownloadableFiles(ctx, free+2)
		if err != nil {
			m.log.Error("next downloadable files failed", "err", err)
			if !m.waitForWork(ctx, m.wakeDownload) {
				return
			}
			continue
		}
		started := false
		for i := range files {
			m.activeMu.Lock()
			_, isActive := m.active[files[i].ID]
			atCap := len(m.active) >= maxWorkers
			m.activeMu.Unlock()
			if atCap {
				break
			}
			if isActive {
				continue
			}
			// Warm-cache reuse (degenerate salvage): the file's verified
			// blob is already in the content-addressed store — a prior run over
			// the same cache, or the same content under another repo/revision.
			// Skip download, verify and copy; mark it cached against the
			// existing blob and let install link it out. This is what makes a
			// fresh state DB over a warm cache not redownload.
			if path, ok := m.cfg.Cache.HasBlob(blobID(&files[i])); ok {
				if err := m.storeCall(ctx, func() error {
					return m.st.MarkCachedFromBlob(ctx, files[i].ID, path)
				}); err != nil {
					if !errors.Is(err, store.ErrFenced) {
						m.log.Warn("mark cached from warm blob failed", "file_id", files[i].ID, "err", err)
					}
				} else {
					m.log.Info("file reused from warm cache blob", "file_id", files[i].ID, "path", files[i].Path)
					wake(m.wakeInstall)
					continue
				}
			}
			handled, deferred := m.salvageGate(ctx, &files[i])
			if handled || deferred {
				continue
			}
			launched, err := m.startDownload(ctx, &files[i])
			if err != nil {
				m.log.Error("start download failed", "file_id", files[i].ID, "path", files[i].Path, "err", err)
			}
			if launched {
				started = true
			}
		}
		if !started {
			if !m.waitForWork(ctx, m.wakeDownload) {
				return
			}
		}
	}
}

// waitForWork sleeps until a wake signal or the poll deadline; false on
// cancellation.
func (m *Manager) waitForWork(ctx context.Context, ch chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case <-ch:
		return true
	case <-time.After(queuePollInterval):
		return true
	}
}

// salvageCandidate reports whether the file is eligible for whole-file
// salvage at all (references configured, LFS with a content hash).
func (m *Manager) salvageCandidate(f *store.File) bool {
	m.salvageMu.Lock()
	enabled := m.refsEnabled
	m.salvageMu.Unlock()
	return enabled && f.IsLFS && f.SHA256 != ""
}

// salvageGate applies the whole-file salvage fast path before any network
// fetch: a hashed reference with matching size+sha256 moves the file to
// 'salvaging' (the disk queue copies it); a still-unhashed size-matching
// reference defers the file so the reference-hash queue can catch up.
func (m *Manager) salvageGate(ctx context.Context, f *store.File) (handled, deferred bool) {
	if !m.salvageCandidate(f) {
		return false, false
	}
	ref, err := m.findSalvageMatch(ctx, f)
	if err != nil {
		m.log.Warn("salvage match probe failed", "file_id", f.ID, "err", err)
		return false, false
	}
	if ref != nil {
		if err := m.st.MarkSalvaging(ctx, f.ID); err != nil {
			m.log.Warn("mark salvaging failed", "file_id", f.ID, "err", err)
			return false, false
		}
		m.log.Info("file claimed for salvage", "file_id", f.ID, "path", f.Path, "reference", ref.Path)
		wake(m.wakeDisk)
		return true, false
	}
	pending, err := m.pendingRefSizeMatch(ctx, f.Size)
	if err != nil {
		m.log.Warn("pending reference probe failed", "err", err)
		return false, false
	}
	if pending {
		wake(m.wakeDisk) // make sure the hash queue is awake
		return false, true
	}
	return false, false
}

// startDownload prepares and launches one file's transfer run. launched
// is false when the file was reset/requeued instead of started (the
// orchestrator then idles instead of hot-looping).
func (m *Manager) startDownload(ctx context.Context, f *store.File) (launched bool, err error) {
	repo, err := m.repoFor(ctx, f.RepoID)
	if err != nil {
		return false, err
	}
	limits := m.currentLimits()

	tok, err := m.st.LeaseFileForDownload(ctx, f.ID, m.nowFn())
	if err != nil {
		return false, err // raced or not queued; another pass will see it
	}

	// Salvage re-check under the lease: a reference hash that completed
	// between the gate's probes and the lease must still divert the file
	// — otherwise the hash→download race leaks a network fetch.
	if m.salvageCandidate(f) {
		if ref, merr := m.findSalvageMatch(ctx, f); merr == nil && ref != nil {
			if terr := m.storeCall(ctx, func() error {
				return m.st.TransitionFile(ctx, f.ID, tok, store.FileDownloading, store.FileSalvaging, nil)
			}); terr == nil {
				m.log.Info("file claimed for salvage", "file_id", f.ID, "path", f.Path, "reference", ref.Path)
				wake(m.wakeDisk)
				return false, nil
			}
		}
	}

	// Durable progress: corrupt blob → clear it and reset to queued. The
	// blob is trusted for offset resume only; a corrupt blob is dropped
	// so the next pass re-chunks the whole file.
	blob, err := m.st.LoadProgress(ctx, f.ID)
	if err != nil {
		return false, fmt.Errorf("load progress: %w", err)
	}
	var seed transfer.IntervalSet
	if len(blob) > 0 {
		if uerr := seed.UnmarshalBinary(blob); uerr != nil {
			m.log.Warn("progress blob corrupt, resetting file", "file_id", f.ID, "err", uerr)
			if serr := m.st.SaveProgress(ctx, f.ID, tok, nil); serr != nil {
				m.log.Warn("clear corrupt progress failed", "file_id", f.ID, "err", serr)
			}
			m.resetFileToQueued(ctx, f, tok, uerr)
			return false, nil
		}
	}
	missing := seed.Missing(f.Size)

	// Already complete per the durable blob (resume at the finish line):
	// skip the transfer entirely; the verifier decides.
	if len(missing) == 0 {
		if err := m.storeCall(ctx, func() error {
			return m.st.FinishDownloaded(ctx, f.ID, tok)
		}); err != nil {
			return false, fmt.Errorf("transition completed file: %w", err)
		}
		wake(m.wakeDisk)
		return false, nil
	}

	// Source selection: xet-backed when the tree carried a xet hash and a
	// xet client is wired, else multi-upstream http.
	var src transfer.BlockSource
	if f.XetHash != "" && m.cfg.Xet != nil {
		xs, err := m.prepareXetSource(ctx, f, repo)
		if err != nil {
			m.handleXetPrepareError(ctx, f, tok, err)
			return false, nil
		}
		src = xs
	}

	blockSize := limits.BlockSize
	if blockSize <= 0 {
		blockSize = transfer.AdaptiveBlockSize(f.Size, limits.Conns)
	}
	bounds := missing
	if src != nil {
		bounds = src.Boundaries(missing, blockSize)
	}
	// Re-chunked blocks must not collide with surviving done/active rows on
	// (file_id, idx): number them past the current maximum (block rows are
	// ephemeral scheduling state; idx carries no byte truth).
	startIdx, err := m.maxBlockIdx(ctx, f.ID)
	if err != nil {
		m.resetFileToQueued(ctx, f, tok, err)
		return false, nil
	}
	blocks := chunkBlocks(f.ID, bounds, blockSize, startIdx+1)
	if err := m.storeCall(ctx, func() error {
		return m.st.ReplacePendingBlocks(ctx, f.ID, tok, blocks)
	}); err != nil {
		m.resetFileToQueued(ctx, f, tok, err)
		return false, nil
	}

	sinkPath := m.cfg.Cache.IncompletePath(f.ID)
	sink, err := m.cfg.Engine.Open(ctx, sinkPath, f.Size, fcio.Hints{})
	if err != nil {
		// fallocate of this file is the actual failed demand: the resume
		// probe must prove an allocation of this size.
		m.noteIOErrorAt(ctx, err, filepath.Dir(sinkPath), f.Size)
		m.resetFileToQueued(ctx, f, tok, err)
		return false, nil
	}

	upstreams, err := m.transferUpstreams(ctx)
	if err != nil {
		_ = sink.Close()
		m.resetFileToQueued(ctx, f, tok, err)
		return false, nil
	}

	leaser := newBlockLeaser(m, f.ID, tok)
	task := &transfer.FileTask{
		FileID:        f.ID,
		Path:          f.Path,
		Size:          f.Size,
		BlobID:        blobID(f),
		Source:        src,
		Repo:          resolveRepoPath(repo),
		SHA:           repo.CommitSHA,
		Upstreams:     upstreams,
		Policy:        limits.UpstreamPolicy,
		BlockSize:     blockSize,
		Conns:         max(limits.Conns, 1),
		StallWindow:   limits.StallTimeout,
		StallMinBytes: limits.StallMinBytes,
		Leaser:        leaser,
		Progress:      blob,
		Sequential:    limits.IOMode == config.IOSequential,
	}

	fileCtx, cancel := context.WithCancel(ctx)
	m.activeMu.Lock()
	m.active[f.ID] = &activeFile{file: *f, conns: task.Conns, cancel: cancel}
	m.activeMu.Unlock()
	m.wg.Add(1)
	go m.runFile(fileCtx, cancel, f, tok, task, sink, leaser)
	return true, nil
}

// maxBlockIdx returns the highest block idx currently stored for a file
// (-1 when none) so re-chunking can number new blocks past it.
func (m *Manager) maxBlockIdx(ctx context.Context, fileID int64) (int64, error) {
	var idx int64
	err := m.st.DB().QueryRowContext(ctx,
		"SELECT COALESCE(MAX(idx), -1) FROM blocks WHERE file_id = ?", fileID).Scan(&idx)
	if err != nil {
		return -1, fmt.Errorf("sched: max block idx file %d: %w", fileID, err)
	}
	return idx, nil
}

// runFile drives one transfer.Downloader.Run to completion, heartbeat
// included, and classifies the outcome per the retry policy: 404/403/401
// terminal, 429/503 cooldowns, give up after 8 block retries.
func (m *Manager) runFile(ctx context.Context, cancel context.CancelFunc, f *store.File, tok store.LeaseToken, task *transfer.FileTask, sink *fcio.File, leaser *blockLeaser) {
	defer m.wg.Done()
	defer cancel()
	defer sink.Close()
	defer func() {
		m.activeMu.Lock()
		delete(m.active, f.ID)
		m.activeMu.Unlock()
		wake(m.wakeDownload)
	}()

	// File + block lease heartbeats; losing the file lease aborts the run.
	hbCtx, hbStop := context.WithCancel(ctx)
	defer hbStop()
	go m.heartbeat(hbCtx, func(until time.Time) error {
		if err := m.st.RenewLease(ctx, store.LeaseFile, f.ID, tok, until); err != nil {
			return err
		}
		leaser.renew(ctx, until)
		return nil
	}, cancel)

	progress := func(pctx context.Context, fileID int64, blob []byte) error {
		err := m.st.SaveProgress(pctx, fileID, tok, blob)
		if errors.Is(err, store.ErrFenced) {
			return nil // file left 'downloading'; nothing to persist
		}
		return err
	}

	err := m.runDownload(ctx, task, sink, progress)
	dctx := m.detachedCtxOr(ctx)
	switch {
	case err == nil:
		// The last block's Complete normally transitioned the file. Verify
		// instead of assuming: a run that returned with blocks still
		// pending leaves the row in 'downloading' — requeue it now (seconds)
		// rather than letting it park until lease expiry.
		var status string
		if qerr := m.st.DB().QueryRowContext(dctx,
			"SELECT status FROM files WHERE id = ?", f.ID).Scan(&status); qerr == nil &&
			status == string(store.FileDownloading) {
			m.log.Warn("download run returned without completing the file; requeueing",
				"file_id", f.ID, "path", f.Path)
			m.resetFileToQueued(dctx, f, tok, nil)
		}
	case errors.Is(err, errGiveUp):
		// The leaser already errored the file and its job_files.
	case errors.Is(err, store.ErrFenced):
		// The row was reclaimed; Recover owns it now.
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		// Graceful stop: blocks were requeued via the detached ctx, the
		// final checkpoint persisted; requeue the file promptly.
		m.log.Debug("download run cancelled, requeueing file", "file_id", f.ID, "path", f.Path)
		m.resetFileToQueued(dctx, f, tok, nil)
	default:
		var rfe *transfer.ResetFileError
		var the *transfer.TerminalHTTPError
		switch {
		case errors.As(err, &rfe):
			m.log.Warn("file reset requested by transfer", "file_id", f.ID, "reason", rfe.Reason)
			m.resetFileToQueued(dctx, f, tok, err)
		case errors.As(err, &the):
			m.terminalFileError(dctx, f, tok, err)
		case m.noteIOError(dctx, err):
			m.resetFileToQueued(dctx, f, tok, err)
		default:
			// Transient: blocks carry their own retry state; requeue.
			m.log.Warn("download run failed, requeueing file", "file_id", f.ID, "err", err)
			m.resetFileToQueued(dctx, f, tok, err)
		}
	}
}

// resetFileToQueued returns a leased file to queued (fencing-tolerant) and
// restores a full-range pending block set, so the next pass re-chunks from
// whatever durable progress survives.
func (m *Manager) resetFileToQueued(ctx context.Context, f *store.File, tok store.LeaseToken, cause error) {
	if err := m.storeCall(ctx, func() error {
		return m.st.TransitionFile(ctx, f.ID, tok, store.FileDownloading, store.FileQueued, cause)
	}); err != nil {
		if !errors.Is(err, store.ErrFenced) {
			m.log.Warn("reset to queued failed", "file_id", f.ID, "err", err)
		}
		return
	}
}

// terminalFileError parks a file in 'error' and cascades to its jobs.
func (m *Manager) terminalFileError(ctx context.Context, f *store.File, tok store.LeaseToken, cause error) {
	if err := m.storeCall(ctx, func() error {
		return m.st.TransitionFile(ctx, f.ID, tok, store.FileDownloading, store.FileError, cause)
	}); err != nil && !errors.Is(err, store.ErrFenced) {
		m.log.Warn("transition to error failed", "file_id", f.ID, "err", err)
	}
	if err := m.storeCall(ctx, func() error {
		return m.st.FailJobFilesForFile(ctx, f.ID, cause)
	}); err != nil {
		m.log.Warn("fail job files failed", "file_id", f.ID, "err", err)
	}
	m.filesFailed.Add(ctx, 1)
	m.recordError(fmt.Errorf("sched: file %s: %w", f.Path, cause))
}

// handleXetPrepareError classifies a failed xet source preparation. Terminal
// Hub/CAS auth, permission (gated repo), and not-found errors error the file
// immediately — no retry (Hub 404/403/401 → terminal, CAS 401 refresh
// failure → file error). Everything else is transient and requeues, but
// bounded by the file's retry counter so a persistently-failing prepare can no
// longer requeue forever (a 429 additionally parked the endpoint gate).
func (m *Manager) handleXetPrepareError(ctx context.Context, f *store.File, tok store.LeaseToken, err error) {
	var authErr *hfapi.AuthError
	var gated *hfapi.GatedError
	var notFound *hfapi.NotFoundError
	var xetAuth *xet.AuthError
	if errors.As(err, &authErr) || errors.As(err, &gated) || errors.As(err, &notFound) || errors.As(err, &xetAuth) {
		m.terminalFileError(ctx, f, tok, err)
		return
	}
	if n, ierr := m.st.IncrFileRetries(ctx, f.ID, tok); ierr == nil && n >= maxBlockRetries {
		m.terminalFileError(ctx, f, tok,
			fmt.Errorf("sched: xet prepare gave up after %d attempts: %w", n, err))
		return
	}
	m.resetFileToQueued(ctx, f, tok, err)
}

// prepareXetSource resolves the refresh route (api-gated HEAD) and prepares
// the xet reconstruction (cas-gated). Errors are requeue-worthy; a 429 sets
// the matching (endpoint, kind) cooldown first.
func (m *Manager) prepareXetSource(ctx context.Context, f *store.File, repo *store.Repo) (*xet.Source, error) {
	client := m.clientFor(repo.Endpoint)
	rt := hfapi.RepoType(repo.Type)

	if err := m.apiGate(ctx, repo.Endpoint); err != nil {
		return nil, err
	}
	xd, err := client.ResolveXet(ctx, rt, repo.Name, repo.CommitSHA, f.Path)
	if err != nil {
		var rl *hfapi.RateLimitError
		if errors.As(err, &rl) {
			// Thread the file's accumulated retry count as the backoff attempt
			// so repeated 429s (no Retry-After) climb the 1s→5m ladder instead
			// of pinning the first rung forever.
			_ = m.setCooldown(ctx, repo.Endpoint, store.CooldownAPI, rl.RetryAfter, f.Retries, "resolve 429")
		}
		return nil, fmt.Errorf("sched: resolve xet %s: %w", f.Path, err)
	}
	if xd.Hash == "" {
		// The resolve endpoint disagrees with the tree: treat as plain
		// http rather than failing the file.
		return nil, nil
	}

	if err := m.casGate(ctx, repo.Endpoint); err != nil {
		return nil, err
	}
	src := m.cfg.Xet.NewSource(f.XetHash, f.Size, xd.RefreshRoute)
	if err := src.Prepare(ctx); err != nil {
		var rl *hfapi.RateLimitError
		if errors.As(err, &rl) {
			_ = m.setCooldown(ctx, repo.Endpoint, store.CooldownCAS, rl.RetryAfter, f.Retries, "cas 429")
		}
		return nil, fmt.Errorf("sched: prepare xet %s: %w", f.Path, err)
	}
	return src, nil
}

// transferUpstreams maps the persisted upstream state (EMA, gates) onto
// transfer's view, one per configured endpoint.
func (m *Manager) transferUpstreams(ctx context.Context) ([]*transfer.Upstream, error) {
	rows, err := m.st.UpstreamState(ctx)
	if err != nil {
		return nil, err
	}
	byEndpoint := make(map[string]store.Upstream, len(rows))
	for _, r := range rows {
		byEndpoint[r.Endpoint] = r
	}
	out := make([]*transfer.Upstream, 0, len(m.cfg.Clients))
	for ep := range m.cfg.Clients {
		u := &transfer.Upstream{Endpoint: ep}
		if r, ok := byEndpoint[ep]; ok {
			u.EMABps = r.EmaBps
			if r.CooldownUntil != nil {
				u.CooldownUntil = *r.CooldownUntil
			}
			if r.BlacklistUntil != nil {
				u.BlacklistUntil = *r.BlacklistUntil
			}
		}
		out = append(out, u)
	}
	return out, nil
}

// resolveRepoPath prefixes dataset/space repos for resolve URLs, matching
// hfapi.ResolveURL's website layout (transfer builds the URL itself). The
// prefix mapping is hfapi.RepoType.URLPrefix so the security-neutral but
// drift-prone dataset/space layout lives in exactly one place.
func resolveRepoPath(repo *store.Repo) string {
	return hfapi.RepoType(repo.Type).URLPrefix() + repo.Name
}

// chunkBlocks splits the missing ranges at blockSize into pending block
// rows, numbered from startIdx (past any surviving rows of prior passes).
func chunkBlocks(fileID int64, bounds []transfer.Interval, blockSize, startIdx int64) []store.Block {
	var out []store.Block
	idx := startIdx
	for _, iv := range bounds {
		for start := iv.Start; start < iv.End; {
			end := start + blockSize
			if end > iv.End {
				end = iv.End
			}
			out = append(out, store.Block{FileID: fileID, Idx: int(idx), Offset: start, Length: end - start})
			idx++
			start = end
		}
	}
	return out
}
