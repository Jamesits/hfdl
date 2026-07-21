package cache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// cachedirTagContent is the verbatim CACHEDIR.TAG huggingface_hub writes
// into <local-dir>/.cache/huggingface/ (bford.info cache directory tag).
const cachedirTagContent = `Signature: 8a477f597d28d172789f06886806bc55
# This file is a cache directory tag created by huggingface_hub.
# For information about cache directory tags, see:
#	https://bford.info/cachedir/
`

// gitignoreContent keeps hf's own bookkeeping out of the user's file tree;
// huggingface_hub writes exactly "*" with no trailing newline.
const gitignoreContent = "*"

// writeLocalDirStamps reproduces the bookkeeping files huggingface_hub
// leaves under <local-dir>/.cache/huggingface/ beside the per-file
// metadata: the "*" .gitignore and CACHEDIR.TAG (both written only when
// absent, like hf) and the empty download/<repoPath>.lock that hf's
// FileLock leaves behind after a successful download.
func (in *Installer) writeLocalDirStamps(destDir, repoPath string) error {
	cacheDir := filepath.Join(destDir, ".cache", "huggingface")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return fmt.Errorf("cache: create local-dir cache dir: %w", err)
	}
	for path, content := range map[string]string{
		filepath.Join(cacheDir, ".gitignore"):   gitignoreContent,
		filepath.Join(cacheDir, "CACHEDIR.TAG"): cachedirTagContent,
	} {
		_, err := os.Lstat(path)
		switch {
		case err == nil:
			continue // hf never rewrites these once present
		case !errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("cache: stat %s: %w", path, err)
		}
		if err := writeFileSync(path, []byte(content), 0o644); err != nil {
			return err
		}
	}
	if err := in.fsyncDirFn(cacheDir); err != nil {
		return err
	}

	lockPath, err := SafeJoin(filepath.Join(cacheDir, "download"), repoPath+".lock")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return fmt.Errorf("cache: create lock dir: %w", err)
	}
	return createEmptyFile(lockPath)
}
