//go:build !windows

package main

import (
	"context"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestSecondSignalForcesExit(t *testing.T) {
	if os.Getenv("HFDL_SIGNAL_CHILD") == "1" {
		ctx, cancel := context.WithCancel(context.Background())
		_ = watchSignals(ctx, cancel, defaultSignalChan())
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		<-ctx.Done()
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		time.Sleep(2 * time.Second)
		os.Exit(2)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSecondSignalForcesExit$")
	cmd.Env = append(os.Environ(), "HFDL_SIGNAL_CHILD=1")
	err := cmd.Run()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
		t.Fatalf("child error = %v, want exit code 1", err)
	}
}
