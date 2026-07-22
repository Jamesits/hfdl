package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
)

// watchSignals wires graceful shutdown: the first SIGINT/SIGTERM cancels the
// root context (Manager.Run stops leasing and final checkpoints persist); a
// second signal hard-exits like hf does. The watcher deliberately does NOT
// select on ctx.Done — the first signal cancels ctx, and returning there would
// kill the watcher before a second signal could be observed, making the
// force-exit unreachable. It runs until stop() or the channel closes. The
// channel is a seam so tests can inject signals without touching the process.
// stop() detaches the OS delivery (signal.Stop) and ends the watcher.
func watchSignals(ctx context.Context, cancel context.CancelFunc, sigCh chan os.Signal) (stop func()) {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		var canceled bool
		for {
			select {
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
	return func() {
		once.Do(func() {
			signal.Stop(sigCh)
			close(done)
		})
	}
}
