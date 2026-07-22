//go:build windows

package main

import (
	"os"
	"os/signal"
)

func defaultSignalChan() chan os.Signal {
	ch := make(chan os.Signal, 2)
	// Go maps CTRL_C_EVENT and CTRL_BREAK_EVENT to os.Interrupt; Windows has
	// no SIGTERM equivalent to register.
	signal.Notify(ch, os.Interrupt)
	return ch
}
