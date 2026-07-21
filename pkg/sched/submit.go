package sched

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/logging"
	"github.com/jamesits/hfdl/pkg/store"
	"go.opentelemetry.io/otel/attribute"
)

// Submit enqueues one download job: repo + job rows, then reference-file
// expansion when Job.References is non-empty. If the repo was already
// listed (restart, offline re-run), the job is wired to the existing file
// rows immediately — no metadata pass runs for it.
func (m *Manager) Submit(ctx context.Context, j Job) error {
	if j.RepoType == "" {
		j.RepoType = hfapi.RepoTypeModel
	}
	if j.Revision == "" {
		j.Revision = "main"
	}
	if j.DestMode == "" {
		j.DestMode = store.DestModeCache
	}
	if j.DestDir == "" {
		j.DestDir = m.cfg.CacheDir
	}
	if j.Repo == "" {
		return errors.New("sched: submit: empty repo")
	}

	// Per-job limits, when supplied, apply manager-wide (hfdl is one
	// invocation per run; see Manager.SetLimits).
	if j.Limits != (config.Limits{}) {
		m.SetLimits(j.Limits)
	}

	// Register the destination for the reference exclusion test (this job's
	// own dest is checked directly; this covers jobs submitted earlier).
	if root := jobDestRoot(j); root != "" {
		m.destsMu.Lock()
		m.dests = append(m.dests, root)
		m.destsMu.Unlock()
	}

	sj := &store.Job{
		Filenames: jsonOrEmpty(j.Filenames),
		Include:   jsonOrEmpty(j.Include),
		Exclude:   jsonOrEmpty(j.Exclude),
		DestMode:  j.DestMode,
		DestDir:   j.DestDir,
		Repo: &store.Repo{
			Type:     string(j.RepoType),
			Name:     j.Repo,
			Revision: j.Revision,
			Endpoint: m.metaEndpoint,
		},
	}
	if err := m.st.EnqueueJob(ctx, sj); err != nil {
		return fmt.Errorf("sched: submit %s: %w", j.Repo, err)
	}
	// EnqueueJob fills only the repo id; the already-listed fast path below
	// needs status + commit_sha.
	if err := m.st.DB().QueryRowContext(ctx,
		"SELECT status, COALESCE(commit_sha, '') FROM repos WHERE id = ?", sj.Repo.ID).
		Scan((*string)(&sj.Repo.Status), &sj.Repo.CommitSHA); err != nil {
		return fmt.Errorf("sched: submit %s: repo state: %w", j.Repo, err)
	}

	// sched.job span, parented by cmd's hfdl.run via ctx.
	_, span := m.tracer.Start(ctx, "sched.job")
	span.SetAttributes(
		attribute.Int64("hfdl.job_id", sj.ID),
		attribute.String("hfdl.repo", j.Repo),
		attribute.String("hfdl.revision", j.Revision),
	)
	m.spansMu.Lock()
	m.spans[sj.ID] = span
	m.spansMu.Unlock()

	m.primaryMu.Lock()
	m.primary = &jobHeader{jobID: sj.ID, repoID: sj.Repo.ID, repo: j.Repo, revision: j.Revision, repoType: j.RepoType}
	m.primaryMu.Unlock()

	if len(j.References) > 0 {
		refs, err := m.expandReferences(ctx, j)
		if err != nil {
			return fmt.Errorf("sched: submit %s: expand references: %w", j.Repo, err)
		}
		if err := m.st.AddReferenceFiles(ctx, refs); err != nil {
			return fmt.Errorf("sched: submit %s: add references: %w", j.Repo, err)
		}
		m.salvageMu.Lock()
		m.refsEnabled = true
		m.salvageMu.Unlock()
		m.log.Info("reference roots expanded",
			"job_id", sj.ID,
			"patterns", logging.JSONValue(j.References),
			"files", len(refs))
	}

	// Repo already listed (restart with the same state DB): wire the job to
	// the existing files right away — no meta pass will ever run for it.
	repo := sj.Repo
	if repo.Status == store.RepoListed && repo.CommitSHA != "" {
		if err := m.wireListedJob(ctx, sj); err != nil {
			return fmt.Errorf("sched: submit %s: wire listed job: %w", j.Repo, err)
		}
	}

	m.log.Info("job submitted",
		"job_id", sj.ID, "repo", j.Repo, "revision", j.Revision,
		"repo_id", repo.ID, "repo_status", repo.Status,
		"dest_mode", j.DestMode,
		"filenames", logging.JSONValue(j.Filenames))
	wake(m.wakeMeta)
	wake(m.wakeDownload)
	wake(m.wakeDisk)
	wake(m.wakeInstall)
	return nil
}

// wireListedJob pairs a freshly submitted job with the file rows of its
// already-listed repo: selection filter, pending job_files, resolved dest
// paths. Zero matches finish the job immediately.
func (m *Manager) wireListedJob(ctx context.Context, j *store.Job) error {
	repo, err := m.repoFor(ctx, j.RepoID)
	if err != nil {
		return err
	}
	files, err := m.filesForRepo(ctx, repo.ID)
	if err != nil {
		return err
	}
	// --force-download: cached files re-enter the download machine instead
	// of serving cache hits (hf parity).
	if m.cfg.ForceDownload {
		for i := range files {
			if files[i].Status != store.FileCached {
				continue
			}
			// Cached rows carry no lease, so the tokenless guard applies.
			if terr := m.storeCall(ctx, func() error {
				return m.st.TransitionFile(ctx, files[i].ID, "", store.FileCached, store.FileQueued, nil)
			}); terr != nil {
				return fmt.Errorf("sched: force re-download file %d: %w", files[i].ID, terr)
			}
			files[i].Status = store.FileQueued
		}
	}
	sel := parseJobSelection(j)
	var ids []int64
	matched := make([]store.File, 0, len(files))
	for i := range files {
		if sel.matches(files[i].Path) {
			ids = append(ids, files[i].ID)
			matched = append(matched, files[i])
		}
	}
	// hf parity: an explicit filename the repo does not carry is terminal.
	if len(sel.filenames) > 0 {
		have := make(map[string]bool, len(matched))
		for i := range matched {
			have[matched[i].Path] = true
		}
		var missing []string
		for name := range sel.filenames {
			if !have[name] {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			err := &NotInRepoError{Paths: missing}
			m.recordError(err)
			return m.storeCall(ctx, func() error { return m.st.FinishJob(ctx, j.ID, err) })
		}
	}
	if err := m.storeCall(ctx, func() error { return m.st.EnsureJobFiles(ctx, j.ID, ids) }); err != nil {
		return err
	}
	if len(matched) == 0 {
		return m.storeCall(ctx, func() error { return m.st.FinishJob(ctx, j.ID, nil) })
	}

	// Offline: serve listed-but-uncached files from the cache layout (or
	// the local-dir destination); anything unservable fails the job fast,
	// naming the path.
	if m.cfg.Offline {
		for i := range matched {
			f := &matched[i]
			if f.Status == store.FileCached {
				continue
			}
			if f.Status != store.FileQueued {
				continue
			}
			served, err := m.serveJobFileOffline(ctx, j, f, repo)
			if err != nil {
				return err
			}
			if !served {
				oerr := &OfflineError{Op: fmt.Sprintf("serve %s: not in offline cache", f.Path)}
				m.recordError(oerr)
				if ferr := m.storeCall(ctx, func() error {
					return m.st.FailJobFilesForFile(ctx, f.ID, oerr)
				}); ferr != nil {
					return ferr
				}
			}
		}
	}
	if err := m.resolveAndStoreDests(ctx, j, repo, matched); err != nil {
		return err
	}
	if m.cfg.DryRun {
		m.logWouldDownload(j, matched)
		return m.storeCall(ctx, func() error { return m.st.FinishJob(ctx, j.ID, nil) })
	}
	return nil
}

// filesForRepo lists all file rows of a repo (read-only; no store method
// covers it and the store.DB handle is documented for this).
func (m *Manager) filesForRepo(ctx context.Context, repoID int64) ([]store.File, error) {
	var files []store.File
	if err := m.st.DB().NewSelect().Model(&files).
		Where("repo_id = ?", repoID).
		Order("id").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("sched: files for repo %d: %w", repoID, err)
	}
	return files, nil
}

// jobsForRepo lists all job rows of a repo (read-only, same rationale).
func (m *Manager) jobsForRepo(ctx context.Context, repoID int64) ([]store.Job, error) {
	var jobs []store.Job
	if err := m.st.DB().NewSelect().Model(&jobs).
		Where("repo_id = ?", repoID).
		Order("id").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("sched: jobs for repo %d: %w", repoID, err)
	}
	return jobs, nil
}

// parseJobSelection decodes a job row's filenames/include/exclude JSON.
func parseJobSelection(j *store.Job) *selection {
	s := &selection{}
	if j.Filenames != "" {
		var names []string
		if json.Unmarshal([]byte(j.Filenames), &names) == nil && len(names) > 0 {
			s.filenames = make(map[string]bool, len(names))
			for _, n := range names {
				s.filenames[n] = true
			}
		}
	}
	if j.Include != "" {
		_ = json.Unmarshal([]byte(j.Include), &s.include)
	}
	if j.Exclude != "" {
		_ = json.Unmarshal([]byte(j.Exclude), &s.exclude)
	}
	return s
}

// jsonOrEmpty marshals a string slice as JSON; empty stays empty so the
// column stores NULL (store models use nullzero).
func jsonOrEmpty(v []string) string {
	if len(v) == 0 {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// repoCacheDirName is the huggingface_hub cache directory name for a repo
// (models--org--name); duplicated from pkg/cache where it is unexported.
func repoCacheDirName(repoType, repo string) string {
	if repoType == "" {
		repoType = "model"
	}
	return repoType + "s--" + strings.ReplaceAll(repo, "/", "--")
}

// blobID is the cache key + verify target for a file row: SHA256 for LFS
// files, else the git oid.
func blobID(f *store.File) string {
	if f.IsLFS {
		return f.SHA256
	}
	return f.GitOID
}
