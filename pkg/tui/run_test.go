package tui

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/jamesits/hfdl/pkg/logging"
)

// startProgram runs the TUI against a pipe-driven input and discarded
// output, then sends keys. Returns a channel for the run error.
func startProgram(t *testing.T, ctx context.Context, ring *logging.Ring) (chan<- string, <-chan error) {
	t.Helper()
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	// inR is owned by bubbletea's cancelreader once the program starts (its
	// shutdown closes it) — only the write end is ours to close.
	t.Cleanup(func() { _ = inW.Close() })
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, func() *Snapshot { return testSnapshot() }, ring, "test", Callbacks{},
			tea.WithInput(inR), tea.WithOutput(io.Discard), tea.WithoutSignals())
	}()
	keys := make(chan string, 8)
	go func() {
		for k := range keys {
			if _, err := inW.WriteString(k); err != nil {
				return
			}
		}
	}()
	return keys, errCh
}

func TestRunQuitReturnsNil(t *testing.T) {
	keys, errCh := startProgram(t, t.Context(), logging.NewRing(16))
	time.Sleep(300 * time.Millisecond) // let a few ticks render
	keys <- "q"
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("quit must return nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("program did not exit on q")
	}
}

func TestRunContextCancelEndsCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	_, errCh := startProgram(t, ctx, logging.NewRing(16))
	time.Sleep(300 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ctx cancel must return nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("program did not exit on ctx cancel")
	}
}

// TestRunZeroStdWrites proves nothing outside bubbletea's own output writer
// touches the process fds: in TUI mode nothing is ever written to stderr —
// bubbletea owns the terminal and a stray write corrupts the screen.
func TestRunZeroStdWrites(t *testing.T) {
	oldOut, oldErr := os.Stdout, os.Stderr
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout, os.Stderr = wOut, wErr
	defer func() { os.Stdout, os.Stderr = oldOut, oldErr }()

	ring := logging.NewRing(16)
	handler := logging.NewHandler(slog.LevelDebug, nil, ring)
	t.Cleanup(func() { _ = handler.Close(t.Context()) })
	log := slog.New(handler)
	keys, errCh := startProgram(t, t.Context(), ring)
	logsDone := make(chan struct{})
	go func() {
		defer close(logsDone)
		for i := range 100 {
			log.Info("concurrent TUI log", "record", i)
		}
	}()
	time.Sleep(400 * time.Millisecond)
	keys <- "p" // exercise a callback path mid-run
	time.Sleep(100 * time.Millisecond)
	keys <- "q"
	if err := <-errCh; err != nil {
		t.Fatalf("run: %v", err)
	}
	<-logsDone

	if err := wOut.Close(); err != nil {
		t.Errorf("close stdout pipe: %v", err)
	}
	if err := wErr.Close(); err != nil {
		t.Errorf("close stderr pipe: %v", err)
	}
	out, _ := io.ReadAll(rOut)
	errOut, _ := io.ReadAll(rErr)
	if len(out) != 0 {
		t.Errorf("stdout polluted: %q", out)
	}
	if len(errOut) != 0 {
		t.Errorf("stderr polluted: %q", errOut)
	}
}
