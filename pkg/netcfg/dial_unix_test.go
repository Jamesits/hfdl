//go:build unix

package netcfg

import (
	"log/slog"
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// TestDialerSetsTOS dials a loopback listener and reads the option back off
// the connected socket: the end-to-end proof that the control function runs
// on the dialed family and the byte sticks.
func TestDialerSetsTOS(t *testing.T) {
	const tos = 0x48 // af21
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no IPv4 loopback: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.Close()
		}
	}()

	d := Dialer(tos, slog.New(slog.DiscardHandler))
	conn, err := d.DialContext(t.Context(), "tcp4", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	sc, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var got int
	var soErr error
	if err := sc.Control(func(fd uintptr) {
		got, soErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_TOS)
	}); err != nil {
		t.Fatal(err)
	}
	if soErr != nil {
		t.Fatal(soErr)
	}
	// Some stacks mask ECN bits; compare the DSCP part.
	if got&^0x03 != tos&^0x03 {
		t.Fatalf("IP_TOS = %#x, want %#x", got, tos)
	}
}

// TestDialerNone must not install a control function at all.
func TestDialerNone(t *testing.T) {
	if d := Dialer(TOSNone, slog.New(slog.DiscardHandler)); d.ControlContext != nil {
		t.Fatal("TOSNone: expected no ControlContext")
	}
}
