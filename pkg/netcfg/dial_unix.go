//go:build unix

package netcfg

import (
	"strings"

	"golang.org/x/sys/unix"
)

// setTOS sets the TOS byte (IPv4) or Traffic Class byte (IPv6) on a socket
// about to connect. The network string is the concrete per-attempt one from
// the dialer ("tcp4"/"tcp6"); when the family is not encoded, both options
// are attempted and either succeeding is enough (a v4 socket rejects
// IPPROTO_IPV6 and vice versa).
func setTOS(fd uintptr, network string, tos int) error {
	switch {
	case strings.HasSuffix(network, "4"):
		return unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_TOS, tos)
	case strings.HasSuffix(network, "6"):
		return unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_TCLASS, tos)
	default:
		err4 := unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_TOS, tos)
		err6 := unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_TCLASS, tos)
		if err4 == nil || err6 == nil {
			return nil
		}
		return err4
	}
}
