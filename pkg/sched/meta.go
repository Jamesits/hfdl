package sched

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jamesits/hfdl/pkg/cache"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/store"
)

// metaWorker runs the meta queue: lease a pending repo, pin its
// revision to a commit sha, discover files (explicit paths-info or filtered
// recursive tree), persist the listing, resolve per-job dest paths. All Hub
// calls pass the api gate (bucket + (endpoint,'api') cooldown).
func (m *Manager) metaWorker(ctx context.Context) {
	defer m.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		repo, tok, err := m.st.LeaseMeta(ctx, m.nowFn())
		switch {
		case errors.Is(err, store.ErrNoWork):
			select {
			case <-ctx.Done():
				return
			case <-m.wakeMeta:
			case <-time.After(queuePollInterval):
			}
			continue
		case err != nil:
			m.log.Error("lease meta failed", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(queuePollInterval):
			}
			continue
		}
		m.processRepo(ctx, repo, tok)
	}
}

// processRepo runs one leased repo through listing.
func (m *Manager) processRepo(ctx context.Context, repo *store.Repo, tok store.LeaseToken) {
	log := m.log.With("repo", repo.Name, "revision", repo.Revision, "repo_id", repo.ID)

	// Heartbeat keeps the listing lease alive across long paginations.
	hbCtx, hbStop := context.WithCancel(ctx)
	defer hbStop()
	go m.heartbeat(hbCtx, func(until time.Time) error {
		return m.st.RenewLease(ctx, store.LeaseRepo, repo.ID, tok, until)
	}, nil)

	// Any early return must put the row back promptly (a graceful shutdown
	// would otherwise strand it until lease expiry).
	release := func(cause error) {
		if rerr := m.storeCall(m.detachedCtxOr(ctx), func() error {
			return m.st.ReleaseMeta(m.detachedCtxOr(ctx), repo.ID, tok, cause)
		}); rerr != nil && !errors.Is(rerr, store.ErrFenced) {
			log.Warn("release meta failed", "err", rerr)
		}
	}

	if m.cfg.Offline {
		m.processRepoOffline(ctx, repo, tok, log)
		return
	}

	client := m.clientFor(repo.Endpoint)
	if client == nil {
		err := fmt.Errorf("sched: no hfapi client for endpoint %q", repo.Endpoint)
		if ferr := m.storeCall(ctx, func() error { return m.st.FailRepo(ctx, repo.ID, tok, err) }); ferr != nil {
			log.Warn("fail repo failed", "err", ferr)
		}
		m.recordError(err)
		return
	}
	rt := hfapi.RepoType(repo.Type)

	// Pin rev → commit_sha once; subsequent calls use the immutable sha.
	sha := repo.CommitSHA
	if sha == "" {
		if err := m.apiGate(ctx, repo.Endpoint); err != nil {
			release(err)
			return
		}
		resolved, err := client.Revision(ctx, rt, repo.Name, repo.Revision)
		if err != nil {
			m.handleMetaError(ctx, repo, tok, err, log)
			return
		}
		sha = resolved
		if err := m.storeCall(ctx, func() error { return m.st.SetCommitSHA(ctx, repo.ID, tok, sha) }); err != nil {
			if !errors.Is(err, store.ErrFenced) {
				log.Error("set commit sha failed", "err", err)
			}
			return
		}
		repo.CommitSHA = sha
	}

	jobs, err := m.jobsForRepo(ctx, repo.ID)
	if err != nil {
		release(err)
		return
	}

	// Discovery: explicit positional filenames go through paths-info; a
	// snapshot listing walks the recursive tree and applies the union of
	// the repo's jobs' include/exclude globs. listingAll is the raw,
	// unfiltered listing for the commit tree cache.
	var entries, listingAll []hfapi.FileEntry
	var filenames []string
	sels := make([]*selection, 0, len(jobs))
	for i := range jobs {
		sel := parseJobSelection(&jobs[i])
		sels = append(sels, sel)
		for name := range sel.filenames {
			filenames = append(filenames, name)
		}
	}
	// hf parity: hf ALWAYS lists the full tree (snapshot flow) and
	// filters client-side — explicit filenames and include/exclude alike
	// pay the same listing cost, and the tree cache gets the complete
	// commit listing. PathsInfo stays reserved for hash backfill.
	if err := m.apiGate(ctx, repo.Endpoint); err != nil {
		release(err)
		return
	}
	tree, err := client.Tree(ctx, rt, repo.Name, sha, "")
	if err != nil {
		m.handleMetaError(ctx, repo, tok, err, log)
		return
	}
	listingAll = tree
	if len(filenames) > 0 {
		// An explicit filename the repo does not carry is terminal, never
		// a silent zero-file success.
		covered := make(map[string]bool, len(tree))
		for _, e := range tree {
			covered[e.Path] = true
		}
		var missing []string
		for _, name := range filenames {
			if !covered[name] {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			m.handleMetaError(ctx, repo, tok, &NotInRepoError{Paths: missing}, log)
			return
		}
	}
	entries = entries[:0]
	for _, e := range tree {
		if unionMatches(sels, e.Path) {
			entries = append(entries, e)
		}
	}

	// Hash backfill: rows from a pre-crash discovery whose hashes
	// were never persisted (git_oid IS NULL) get a paths-info pass and
	// rejoin the listing.
	backfill, berr := m.hashBackfillPaths(ctx, repo.ID, entries)
	if berr != nil {
		log.Warn("hash backfill probe failed", "err", berr)
	} else if len(backfill) > 0 {
		if err := m.apiGate(ctx, repo.Endpoint); err != nil {
			release(err)
			return
		}
		var bf []hfapi.FileEntry
		bf, err = client.PathsInfo(ctx, rt, repo.Name, sha, backfill)
		if err != nil {
			m.handleMetaError(ctx, repo, tok, err, log)
			return
		}
		entries = append(entries, bf...)
		log.Info("hash backfill merged", "paths", len(bf))
	}

	// Persist: upsert files + job_files for every job of the repo, mark
	// listed — one fenced tx.
	storeEntries := make([]store.FileEntry, 0, len(entries))
	for _, e := range entries {
		storeEntries = append(storeEntries, store.FileEntry{
			Path:    e.Path,
			Size:    e.Size,
			GitOID:  e.GitOID,
			SHA256:  e.SHA256,
			XetHash: e.XetHash,
			IsLFS:   e.IsLFS,
		})
	}
	if err := m.storeCall(ctx, func() error {
		return m.st.CompleteListing(ctx, repo.ID, tok, storeEntries)
	}); err != nil {
		if !errors.Is(err, store.ErrFenced) {
			log.Error("complete listing failed", "err", err)
			release(err)
		}
		return
	}
	m.repoMu.Lock()
	m.repoByID[repo.ID] = repo
	m.repoMu.Unlock()
	log.Info("listing complete", "commit_sha", sha, "files", len(storeEntries))

	// Commit tree cache (hf parity): the COMPLETE commit listing, written
	// at listing time for every job destination (cache-mode model dir and
	// local-dir). The install-completion fallback never overwrites this.
	if !m.cfg.DryRun {
		m.writeTreeCaches(ctx, jobs, repo, treeEntriesFromListing(listingAll), log)
	}

	// Per-job dest resolution (SafeJoin before any filesystem join)
	// + zero-selection jobs finish immediately.
	files, err := m.filesForRepo(ctx, repo.ID)
	if err != nil {
		m.recordError(err)
		return
	}
	for i := range jobs {
		j := &jobs[i]
		sel := parseJobSelection(j)
		var matched []store.File
		for k := range files {
			if sel.matches(files[k].Path) {
				matched = append(matched, files[k])
			}
		}
		if len(matched) == 0 {
			if ferr := m.storeCall(ctx, func() error { return m.st.FinishJob(ctx, j.ID, nil) }); ferr != nil {
				log.Warn("finish zero-file job failed", "job_id", j.ID, "err", ferr)
			}
			continue
		}
		if err := m.resolveAndStoreDests(ctx, j, repo, matched); err != nil {
			m.recordError(err)
			if ferr := m.storeCall(ctx, func() error { return m.st.FinishJob(ctx, j.ID, err) }); ferr != nil {
				log.Warn("finish job failed", "job_id", j.ID, "err", ferr)
			}
			continue
		}
		if m.cfg.DryRun {
			m.logWouldDownload(j, matched)
			if ferr := m.storeCall(ctx, func() error { return m.st.FinishJob(ctx, j.ID, nil) }); ferr != nil {
				log.Warn("finish dry-run job failed", "job_id", j.ID, "err", ferr)
			}
		}
	}

	wake(m.wakeDownload)
	wake(m.wakeDisk)
	wake(m.wakeInstall)
}

// NotInRepoError is terminal: an explicitly requested path the repo does
// not carry (hf parity: "File not found in repository.", exit 1).
type NotInRepoError struct{ Paths []string }

func (e *NotInRepoError) Error() string {
	return "sched: file not found in repository: " + strings.Join(e.Paths, ", ")
}

// handleMetaError applies the retry table to a Hub API failure: 429
// cools down (endpoint,'api'), 404 is terminal, transient errors requeue
// up to the give-up bound.
func (m *Manager) handleMetaError(ctx context.Context, repo *store.Repo, tok store.LeaseToken, err error, log *slog.Logger) {
	var rl *hfapi.RateLimitError
	var nf *hfapi.NotFoundError
	var g *hfapi.GatedError
	var a *hfapi.AuthError
	var nr *NotInRepoError
	dctx := m.detachedCtxOr(ctx)
	switch {
	case errors.As(err, &rl):
		// 429: (endpoint,'api') cooldown + immediate requeue.
		if cerr := m.setCooldown(ctx, repo.Endpoint, store.CooldownAPI, rl.RetryAfter, repo.Retries, "hub api 429"); cerr != nil {
			log.Warn("set cooldown failed", "err", cerr)
		}
		if rerr := m.storeCall(dctx, func() error { return m.st.ReleaseMeta(dctx, repo.ID, tok, err) }); rerr != nil && !errors.Is(rerr, store.ErrFenced) {
			log.Warn("release meta failed", "err", rerr)
		}
	case errors.As(err, &nf), errors.As(err, &g), errors.As(err, &a), errors.As(err, &nr):
		// 404/403/401/missing-explicit-file: terminal, no retry.
		if ferr := m.storeCall(dctx, func() error { return m.st.FailRepo(dctx, repo.ID, tok, err) }); ferr != nil {
			log.Warn("fail repo failed", "err", ferr)
		}
		m.recordError(err)
		log.Warn("repo terminally failed", "err", err)
	default:
		// Transient (5xx/network): requeue with the repos.retries ladder;
		// give up at the 8-retry bound.
		if repo.Retries+1 >= maxBlockRetries {
			if ferr := m.storeCall(dctx, func() error { return m.st.FailRepo(dctx, repo.ID, tok, err) }); ferr != nil {
				log.Warn("fail repo failed", "err", ferr)
			}
			m.recordError(err)
			return
		}
		if rerr := m.storeCall(dctx, func() error { return m.st.ReleaseMeta(dctx, repo.ID, tok, err) }); rerr != nil && !errors.Is(rerr, store.ErrFenced) {
			log.Warn("release meta failed", "err", rerr)
		}
	}
	wake(m.wakeMeta)
}

// hashBackfillPaths lists repo file rows that still lack a git_oid (a
// pre-crash discovery), excluding paths the fresh listing already covers.
func (m *Manager) hashBackfillPaths(ctx context.Context, repoID int64, entries []hfapi.FileEntry) ([]string, error) {
	covered := make(map[string]bool, len(entries))
	for _, e := range entries {
		covered[e.Path] = true
	}
	var paths []string
	err := m.st.DB().NewSelect().Model((*store.File)(nil)).
		Column("path").
		Where("repo_id = ?", repoID).
		Where("git_oid IS NULL OR git_oid = ''").
		Scan(ctx, &paths)
	if err != nil {
		return nil, fmt.Errorf("sched: hash backfill probe: %w", err)
	}
	out := paths[:0]
	for _, p := range paths {
		if !covered[p] {
			out = append(out, p)
		}
	}
	return out, nil
}

// destFor resolves the install target of one file for a job: SafeJoin
// before any filesystem join rejects absolute paths, .. and symlink
// escapes. repo may be nil in dry-run logging
// when the commit sha is irrelevant to the message.
func (m *Manager) destFor(j *store.Job, repo *store.Repo, repoPath string) (string, error) {
	if j.DestMode == store.DestModeLocalDir {
		return cache.SafeJoin(j.DestDir, repoPath)
	}
	sha := ""
	repoName, repoType := "", "model"
	if repo != nil {
		sha = repo.CommitSHA
		repoName = repo.Name
		repoType = repo.Type
	}
	base := filepath.Join(j.DestDir, repoCacheDirName(repoType, repoName), "snapshots", sha)
	return cache.SafeJoin(base, repoPath)
}

// resolveAndStoreDests computes every matched file's dest_path and
// persists the batch.
func (m *Manager) resolveAndStoreDests(ctx context.Context, j *store.Job, repo *store.Repo, files []store.File) error {
	dests := make(map[int64]string, len(files))
	for i := range files {
		dest, err := m.destFor(j, repo, files[i].Path)
		if err != nil {
			return fmt.Errorf("sched: resolve dest for %s: %w", files[i].Path, err)
		}
		dests[files[i].ID] = dest
	}
	if err := m.storeCall(ctx, func() error { return m.st.SetJobFileDests(ctx, j.ID, dests) }); err != nil {
		return fmt.Errorf("sched: store dest paths job %d: %w", j.ID, err)
	}
	return nil
}

// detachedCtxOr returns the manager's detached ctx when running, else the
// passed ctx (callers on the cancel path still reach the store).
func (m *Manager) detachedCtxOr(ctx context.Context) context.Context {
	if d := m.detachedCtx(); d != nil {
		return d
	}
	return ctx
}
