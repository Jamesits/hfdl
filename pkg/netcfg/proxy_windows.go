//go:build windows

package netcfg

import (
	"context"
	"fmt"
	"log/slog"
	"unsafe"

	"github.com/jamesits/goinvoke"
	"golang.org/x/net/http/httpproxy"
	"golang.org/x/sys/windows"
)

// winHTTP declares the winhttp.dll exports goinvoke resolves by field name.
// The bare DLL name matters: goinvoke restricts it to the Windows system
// directory (NewLazySystemDLL) instead of the general DLL search order.
type winHTTP struct {
	WinHttpGetIEProxyConfigForCurrentUser *windows.LazyProc
	WinHttpGetDefaultProxyConfiguration   *windows.LazyProc
}

// kernel32 exists because the WinHTTP out-strings are GlobalAlloc'd and
// documented to be released with GlobalFree, which x/sys/windows does not
// wrap (it only wraps LocalFree).
type kernel32 struct {
	GlobalFree *windows.LazyProc
}

// freeGlobalStrings releases GlobalAlloc'd strings returned by winhttp.dll.
func (k *kernel32) freeGlobalStrings(ptrs ...*uint16) {
	for _, p := range ptrs {
		if p != nil {
			_, _, _ = k.GlobalFree.Call(uintptr(unsafe.Pointer(p)))
		}
	}
}

// ieProxyConfig mirrors WINHTTP_CURRENT_USER_IE_PROXY_CONFIG (x/sys/windows
// does not define it). The strings are GlobalAlloc'd by winhttp.dll and must
// be released with GlobalFree.
type ieProxyConfig struct {
	autoDetect    int32
	autoConfigURL *uint16
	proxy         *uint16
	proxyBypass   *uint16
}

// proxyInfo mirrors WINHTTP_PROXY_INFO (same GlobalFree contract).
type proxyInfo struct {
	accessType  uint32
	proxy       *uint16
	proxyBypass *uint16
}

// systemProxyConfig reads the current user's WinINet ("Internet Options")
// proxy settings, falling back to the machine-wide WinHTTP default proxy
// (`netsh winhttp set proxy`) when the user has none. PAC files and WPAD
// auto-detection are reported but not evaluated.
func systemProxyConfig(ctx context.Context, log *slog.Logger) (*httpproxy.Config, error) {
	var dll winHTTP
	if err := goinvoke.Unmarshal("winhttp.dll", &dll); err != nil {
		return nil, fmt.Errorf("load winhttp.dll: %w", err)
	}
	var k32 kernel32
	if err := goinvoke.Unmarshal("kernel32.dll", &k32); err != nil {
		return nil, fmt.Errorf("load kernel32.dll: %w", err)
	}

	var ie ieProxyConfig
	// BOOL return: zero means failure, and only then is the last-error
	// value meaningful.
	if r, _, err := dll.WinHttpGetIEProxyConfigForCurrentUser.Call(uintptr(unsafe.Pointer(&ie))); r == 0 {
		return nil, fmt.Errorf("WinHttpGetIEProxyConfigForCurrentUser: %w", err)
	}
	defer k32.freeGlobalStrings(ie.autoConfigURL, ie.proxy, ie.proxyBypass)

	proxy := windows.UTF16PtrToString(ie.proxy)
	bypass := windows.UTF16PtrToString(ie.proxyBypass)
	if proxy == "" {
		if ie.autoDetect != 0 || ie.autoConfigURL != nil {
			log.LogAttrs(ctx, slog.LevelWarn,
				"windows proxy is PAC/auto-detect only, which hfdl does not evaluate; going direct",
				slog.String("pac_url", windows.UTF16PtrToString(ie.autoConfigURL)))
		}
		var pi proxyInfo
		if r, _, _ := dll.WinHttpGetDefaultProxyConfiguration.Call(uintptr(unsafe.Pointer(&pi))); r != 0 {
			defer k32.freeGlobalStrings(pi.proxy, pi.proxyBypass)
			proxy = windows.UTF16PtrToString(pi.proxy)
			bypass = windows.UTF16PtrToString(pi.proxyBypass)
		}
	}
	if proxy == "" {
		return nil, nil
	}
	return winhttpConfig(proxy, bypass), nil
}
