package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/jamesits/hfdl/pkg/fcio"
)

func atomicWriteFileSync(path string, data []byte, perm os.FileMode, syncDir func(string) error) error {
	dir := filepath.Dir(path)
	tmp, err := createTempFileSync(dir, data, perm)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp) }()
	if err := fcio.ReplaceFile(tmp, path); err != nil {
		return fmt.Errorf("cache: replace %s: %w", path, err)
	}
	return syncDir(dir)
}

func createTempFileSync(dir string, data []byte, perm os.FileMode) (string, error) {
	f, err := os.CreateTemp(dir, tmpPrefix)
	if err != nil {
		return "", fmt.Errorf("cache: create temp in %s: %w", dir, err)
	}
	tmp := f.Name()
	if err := f.Chmod(perm); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", fmt.Errorf("cache: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", fmt.Errorf("cache: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("cache: close %s: %w", tmp, err)
	}
	return tmp, nil
}

func mkdirAllSync(path string, perm os.FileMode, syncDir func(string) error) error {
	var created []string
	for p := path; ; p = filepath.Dir(p) {
		_, err := os.Stat(p)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		created = append(created, p)
		if filepath.Dir(p) == p {
			break
		}
	}
	if err := os.MkdirAll(path, perm); err != nil {
		return err
	}
	for _, c := range slices.Backward(created) {
		if err := syncDir(c); err != nil {
			return err
		}
	}
	if len(created) > 0 {
		return syncDir(filepath.Dir(created[len(created)-1]))
	}
	return nil
}
