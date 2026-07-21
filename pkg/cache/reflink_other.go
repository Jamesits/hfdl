//go:build !linux

package cache

import "errors"

// errReflinkUnsupported sends the installer straight to the copy path on
// platforms without a reflink fast path (darwin clonefile is deliberately
// skipped — the copy path is the portable default there).
var errReflinkUnsupported = errors.New("cache: reflink unsupported on this platform")

func reflinkFile(src, dst string) error { return errReflinkUnsupported }
