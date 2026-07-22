//go:build !unix && !windows

package netcfg

import "errors"

// setTOS: no socket option access on this platform; the dialer logs one
// warning and connections proceed untagged.
func setTOS(fd uintptr, network string, tos int) error {
	return errors.ErrUnsupported
}
