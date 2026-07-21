//go:build !linux && !darwin

package cache

import "errors"

// errReflinkUnsupported sends the installer straight to the copy path on
// platforms without a reflink fast path (Linux uses FICLONE, macOS clonefile;
// Windows and unknown platforms copy).
var errReflinkUnsupported = errors.New("cache: reflink unsupported on this platform")

func reflinkFile(src, dst string) error { return errReflinkUnsupported }
