package main

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestWatchSignalsCancelsOnFirstSignal(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sigCh := make(chan os.Signal, 2)
	stop := watchSignals(ctx, cancel, sigCh)
	defer stop()

	sigCh <- syscall.SIGINT
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("ctx not canceled after SIGINT")
	}
}

func TestWatchSignalsStopsCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	sigCh := make(chan os.Signal, 1)
	stop := watchSignals(ctx, cancel, sigCh)
	stop()
	cancel() // must not deadlock or panic with the watcher detached
}

func TestWatchSignalsClosedChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sigCh := make(chan os.Signal)
	close(sigCh)
	stop := watchSignals(ctx, cancel, sigCh)
	defer stop()
	select {
	case <-ctx.Done():
		t.Fatal("closed signal channel must not cancel the context")
	case <-time.After(50 * time.Millisecond):
	}
}
