//go:build windows

package netcfg

import (
	"strings"

	"golang.org/x/sys/windows"
)

// sockoptIPv6TrafficClass is ws2ipdef.h's IPV6_TCLASS, which x/sys/windows
// does not define.
const sockoptIPv6TrafficClass = 39

// setTOS sets the TOS/Traffic Class byte on a socket about to connect.
//
// Windows pitfall: the setsockopt call succeeds, but the stack ignores
// user-set TOS unless the DisableUserTOSSetting registry value under
// Services\Tcpip\QoS is 0 (the supported path is qWAVE/Group Policy).
// hfdl still sets it — on systems configured to honor it, it works; nothing
// breaks elsewhere.
func setTOS(fd uintptr, network string, tos int) error {
	h := windows.Handle(fd)
	switch {
	case strings.HasSuffix(network, "4"):
		return windows.SetsockoptInt(h, windows.IPPROTO_IP, windows.IP_TOS, tos)
	case strings.HasSuffix(network, "6"):
		return windows.SetsockoptInt(h, windows.IPPROTO_IPV6, sockoptIPv6TrafficClass, tos)
	default:
		err4 := windows.SetsockoptInt(h, windows.IPPROTO_IP, windows.IP_TOS, tos)
		err6 := windows.SetsockoptInt(h, windows.IPPROTO_IPV6, sockoptIPv6TrafficClass, tos)
		if err4 == nil || err6 == nil {
			return nil
		}
		return err4
	}
}
