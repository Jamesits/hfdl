package hfapi

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// RepoType is the Hub repository kind.
type RepoType string

const (
	RepoTypeModel   RepoType = "model"
	RepoTypeDataset RepoType = "dataset"
	RepoTypeSpace   RepoType = "space"
)

// plural maps a RepoType to its /api/ path segment.
func (rt RepoType) plural() (string, error) {
	switch rt {
	case RepoTypeModel:
		return "models", nil
	case RepoTypeDataset:
		return "datasets", nil
	case RepoTypeSpace:
		return "spaces", nil
	}
	return "", fmt.Errorf("hfapi: unknown repo type %q", string(rt))
}

// urlPrefix is the website URL prefix for resolve URLs: datasets and spaces
// live under /datasets/ and /spaces/; models sit at the root.
func (rt RepoType) urlPrefix() string {
	switch rt {
	case RepoTypeDataset:
		return "datasets/"
	case RepoTypeSpace:
		return "spaces/"
	}
	return ""
}

// FileEntry is one file in a repo tree (or a paths-info row).
type FileEntry struct {
	Path    string
	Size    int64
	GitOID  string // tree `oid`: git blob sha1 (the LFS *pointer* blob for LFS files)
	SHA256  string // tree `lfs.oid`: raw content sha256; empty for non-LFS
	XetHash string // tree `xetHash`: CAS reconstruction id (keyed-BLAKE3 domain — NEVER a verify target)
	IsLFS   bool
}

// BlobID is the cache key and verify target: content sha256 for LFS/xet
// files, git blob sha1 for regular files.
func (f FileEntry) BlobID() string {
	if f.IsLFS {
		return f.SHA256
	}
	return f.GitOID
}

// RepoInfo is the subset of /api/{type}s/{repo} hfdl needs.
type RepoInfo struct {
	ID       string
	SHA      string
	Private  bool
	Gated    bool
	Siblings []string
}

// XetFileData is the HEAD-resolve xet detection result.
type XetFileData struct {
	Hash         string // X-Xet-Hash
	RefreshRoute string // token refresh route: Link rel="xet-auth" preferred, else X-Xet-Refresh-Route
}

// XetToken is a CAS bearer token, cached by the caller until Exp.
type XetToken struct {
	AccessToken string
	Exp         time.Time
	CasURL      string
}

// entryJSON is the wire shape of tree and paths-info entries. The Hub sends
// the LFS sha256 under lfs.oid; lfs.sha256 is accepted as well for safety.
type entryJSON struct {
	Type string `json:"type"`
	Path string `json:"path"`
	Size int64  `json:"size"`
	OID  string `json:"oid"`
	LFS  *struct {
		OID    string `json:"oid"`
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	} `json:"lfs"`
	XetHash string `json:"xetHash"`
}

func (e *entryJSON) toFileEntry() FileEntry {
	fe := FileEntry{
		Path:    e.Path,
		Size:    e.Size,
		GitOID:  e.OID,
		XetHash: e.XetHash,
	}
	if e.LFS != nil {
		fe.SHA256 = e.LFS.OID
		if fe.SHA256 == "" {
			fe.SHA256 = e.LFS.SHA256
		}
		fe.IsLFS = fe.SHA256 != ""
		if fe.Size == 0 {
			fe.Size = e.LFS.Size
		}
	}
	return fe
}

// repoInfoJSON is the wire shape of GET /api/{type}s/{repo}. `gated` is
// polymorphic upstream: false | true | "auto" | "manual".
type repoInfoJSON struct {
	ID       string          `json:"id"`
	SHA      string          `json:"sha"`
	Private  bool            `json:"private"`
	Gated    json.RawMessage `json:"gated"`
	Siblings []struct {
		RFilename string `json:"rfilename"`
	} `json:"siblings"`
}

func (r *repoInfoJSON) toRepoInfo() *RepoInfo {
	ri := &RepoInfo{
		ID:      r.ID,
		SHA:     r.SHA,
		Private: r.Private,
		Gated:   parseGated(r.Gated),
	}
	if len(r.Siblings) > 0 {
		ri.Siblings = make([]string, 0, len(r.Siblings))
		for _, s := range r.Siblings {
			ri.Siblings = append(ri.Siblings, s.RFilename)
		}
	}
	return ri
}

// parseGated treats anything other than JSON false/null/absent as gated
// (true, "auto", "manual").
func parseGated(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s != "" && s != "null" && s != "false"
}

// escPath percent-encodes each /-separated segment of p, keeping the
// separators (repo paths are nested). Contrast with url.PathEscape used
// bare on a revision, which must be ONE path component (refs/pr/3 ->
// refs%2Fpr%2F3).
func escPath(p string) string {
	segs := strings.Split(p, "/")
	for i := range segs {
		segs[i] = url.PathEscape(segs[i])
	}
	return strings.Join(segs, "/")
}
