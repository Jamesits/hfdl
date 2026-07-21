package hfapi

import (
	"fmt"
	"strings"
)

// RepoRef is a parsed repo positional: type, "org/name" (or canonical
// single-name) id, and optional pinned revision.
type RepoRef struct {
	RepoType RepoType
	Repo     string
	Revision string
}

// schemePrefixes maps hf:// URI type segments to repo types.
var schemePrefixes = map[string]RepoType{
	"models":   RepoTypeModel,
	"datasets": RepoTypeDataset,
	"spaces":   RepoTypeSpace,
}

// ParseRepoRef parses a repo positional: plain "org/repo" or hf:// URIs
// ("hf://org/repo", "hf://models/org/repo", "hf://datasets/org/repo@rev"),
// each with an optional "@rev" suffix (split at the FIRST "@" — repo ids
// never contain one, so revisions keep any slashes, e.g. refs/pr/3). Without a type segment the URI form
// behaves like the plain form and takes defaultType.
func ParseRepoRef(positional string, defaultType RepoType) (RepoRef, error) {
	s := strings.TrimSpace(positional)
	if s == "" {
		return RepoRef{}, fmt.Errorf("%w: empty reference", ErrInvalidRepoRef)
	}

	rt := defaultType
	if rest, ok := strings.CutPrefix(s, "hf://"); ok {
		s = rest
		if head, tail, found := strings.Cut(s, "/"); found {
			if t, isType := schemePrefixes[head]; isType {
				rt, s = t, tail
			}
		}
	}

	repo, rev, hasRev := strings.Cut(s, "@") // first "@": repo ids never contain one
	if hasRev && rev == "" {
		return RepoRef{}, fmt.Errorf("%w: empty revision in %q", ErrInvalidRepoRef, positional)
	}

	segs := strings.Split(repo, "/")
	if len(segs) < 1 || len(segs) > 2 {
		return RepoRef{}, fmt.Errorf("%w: repo id must be name or org/name in %q", ErrInvalidRepoRef, positional)
	}
	for _, seg := range segs {
		if seg == "" || strings.ContainsAny(seg, " \t\n") {
			return RepoRef{}, fmt.Errorf("%w: bad repo id %q", ErrInvalidRepoRef, repo)
		}
	}
	return RepoRef{RepoType: rt, Repo: repo, Revision: rev}, nil
}
