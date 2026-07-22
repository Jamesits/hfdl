//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func defaultSignalChan() chan os.Signal {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	return ch
}
