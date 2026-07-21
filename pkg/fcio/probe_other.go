//go:build !linux && !windows && !darwin

package fcio

import (
	"context"
	"fmt"
	"path/filepath"
)

// ProbeFs has no media-class probing on genuinely-unknown platforms (Windows
// and macOS have real probes): always FsUnknown.
func ProbeFs(ctx context.Context, path string) (FsType, error) {
	if err := ctx.Err(); err != nil {
		return FsUnknown, err
	}
	return FsUnknown, nil
}

// statVolumeID degrades to the absolute path of the nearest existing
// ancestor — there is no portable st_dev across windows/darwin here, and the
// plain tier never mixes tiers on a spindle anyway.
func statVolumeID(path string) (VolumeID, error) {
	abs, err := filepath.Abs(existingAncestor(path))
	if err != nil {
		return "", fmt.Errorf("fcio: resolve volume %s: %w", path, err)
	}
	return VolumeID(abs), nil
}
