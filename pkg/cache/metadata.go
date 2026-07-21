package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// writeDownloadMetadata stamps
// <DestDir>/.cache/huggingface/download/<repoPath>.metadata exactly like
// huggingface_hub: first line the commit sha, second line the etag (our
// blob id), third line a Unix timestamp float, each newline-terminated.
func (in *Installer) writeDownloadMetadata(destDir, repoPath, commitSHA, etag string) error {
	metaRoot := filepath.Join(destDir, ".cache", "huggingface", "download")
	metaPath, err := SafeJoinContent(metaRoot, repoPath+".metadata")
	if err != nil {
		return err
	}
	parent := filepath.Dir(metaPath)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("cache: create metadata dir: %w", err)
	}
	ts := strconv.FormatFloat(float64(time.Now().UnixNano())/1e9, 'f', -1, 64)
	content := commitSHA + "\n" + etag + "\n" + ts + "\n"
	if err := writeFileSync(metaPath, []byte(content), 0o644); err != nil {
		return err
	}
	return in.fsyncDirFn(parent)
}
