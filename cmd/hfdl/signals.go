package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// watchSignals wires graceful shutdown: the first SIGINT/SIGTERM cancels the
// root context (Manager.Run stops leasing and final checkpoints persist); a
// second signal hard-exits like hf does. The channel is a seam so tests can
// inject signals without touching the process. Returns a stop func that
// detaches the watcher.
func watchSignals(ctx context.Context, cancel context.CancelFunc, sigCh <-chan os.Signal) (stop func()) {
	done := make(chan struct{})
	go func() {
		var canceled bool
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case sig, ok := <-sigCh:
				if !ok {
					return
				}
				if canceled {
					slog.LogAttrs(ctx, slog.LevelWarn, "second interrupt, forcing exit",
						slog.String("signal", sig.String()))
					os.Exit(1)
				}
				canceled = true
				slog.LogAttrs(ctx, slog.LevelInfo, "interrupt received, draining (press again to force)",
					slog.String("signal", sig.String()))
				cancel()
			}
		}
	}()
	return func() { close(done) }
}

// defaultSignalChan delivers SIGINT/SIGTERM for the real CLI.
func defaultSignalChan() <-chan os.Signal {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	return ch
}
