package cache

import (
	"fmt"
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
	if err := mkdirAllSync(parent, 0o755, in.fsyncDirFn); err != nil {
		return fmt.Errorf("cache: create metadata dir: %w", err)
	}
	ts := strconv.FormatFloat(float64(time.Now().UnixNano())/1e9, 'f', -1, 64)
	content := commitSHA + "\n" + etag + "\n" + ts + "\n"
	return atomicWriteFileSync(metaPath, []byte(content), 0o644, in.fsyncDirFn)
}
