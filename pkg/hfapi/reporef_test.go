package hfapi

import (
	"errors"
	"testing"
)

func TestParseRepoRef(t *testing.T) {
	cases := []struct {
		in          string
		defaultType RepoType
		want        RepoRef
		wantErr     bool
	}{
		{in: "org/repo", defaultType: RepoTypeModel, want: RepoRef{RepoTypeModel, "org/repo", ""}},
		{in: "org/repo@dev", defaultType: RepoTypeModel, want: RepoRef{RepoTypeModel, "org/repo", "dev"}},
		{in: "gpt2", defaultType: RepoTypeModel, want: RepoRef{RepoTypeModel, "gpt2", ""}},
		{in: "hf://org/repo", defaultType: RepoTypeDataset, want: RepoRef{RepoTypeDataset, "org/repo", ""}},
		{in: "hf://models/org/repo", defaultType: RepoTypeDataset, want: RepoRef{RepoTypeModel, "org/repo", ""}},
		{in: "hf://datasets/org/repo@v1", defaultType: RepoTypeModel, want: RepoRef{RepoTypeDataset, "org/repo", "v1"}},
		{in: "hf://spaces/o/r", defaultType: RepoTypeModel, want: RepoRef{RepoTypeSpace, "o/r", ""}},
		{in: "hf://datasets/o/r@refs/pr/3", defaultType: RepoTypeModel, want: RepoRef{RepoTypeDataset, "o/r", "refs/pr/3"}},
		{in: "hf://models/gpt2", defaultType: RepoTypeDataset, want: RepoRef{RepoTypeModel, "gpt2", ""}},
		{in: "  org/repo  ", defaultType: RepoTypeModel, want: RepoRef{RepoTypeModel, "org/repo", ""}},

		{in: "", wantErr: true},
		{in: "   ", wantErr: true},
		{in: "org/repo@", wantErr: true},
		{in: "@rev", wantErr: true},
		{in: "a/b/c", wantErr: true},
		{in: "org//repo", wantErr: true},
		{in: "hf://foo/org/repo", wantErr: true},
		{in: "hf://datasets/", wantErr: true},
		{in: "hf://", wantErr: true},
		{in: "or g/repo", wantErr: true},
	}
	for _, tc := range cases {
		got, err := ParseRepoRef(tc.in, tc.defaultType)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseRepoRef(%q) = %+v, want error", tc.in, got)
			} else if !errors.Is(err, ErrInvalidRepoRef) {
				t.Errorf("ParseRepoRef(%q) error %v does not wrap ErrInvalidRepoRef", tc.in, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseRepoRef(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseRepoRef(%q) = %+v, want %+v", tc.in, got, tc.want)
		}
	}
}

func TestBlobID(t *testing.T) {
	lfs := FileEntry{GitOID: "pointer-sha1", SHA256: "content-sha256", IsLFS: true}
	if got := lfs.BlobID(); got != "content-sha256" {
		t.Errorf("LFS BlobID = %q, want content sha256", got)
	}
	plain := FileEntry{GitOID: "blob-sha1"}
	if got := plain.BlobID(); got != "blob-sha1" {
		t.Errorf("non-LFS BlobID = %q, want git blob sha1", got)
	}
}
