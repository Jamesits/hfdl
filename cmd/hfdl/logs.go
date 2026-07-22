package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/jamesits/hfdl/pkg/config"
	"github.com/jamesits/hfdl/pkg/store"
)

// logsFlags models `hfdl logs`, the debug companion command.
type logsFlags struct {
	levelStr    string
	sinceStr    string
	localDir    string
	stateDBFlag string
}

func newLogsCmd() *cobra.Command {
	f := &logsFlags{}
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Dump the persisted logs table",
		Long: "Print log rows persisted in the state database (the post-mortem\n" +
			"counterpart of the TUI logs tab).",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runLogs(cmd.Context(), f, os.Getenv, os.Stdout)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.levelStr, "level", "debug", "minimum level to print: debug|info|warn|error")
	fl.StringVar(&f.sinceStr, "since", "", "only rows newer than this duration ago (e.g. 1h, 30m)")
	fl.StringVar(&f.localDir, "local-dir", "", "read the state db of a --local-dir download instead of the HF cache")
	fl.StringVar(&f.stateDBFlag, "state-db", "", "state database path (default <dest>/.hfdl/state.db)")
	return cmd
}

func runLogs(ctx context.Context, f *logsFlags, getenv func(string) string, out io.Writer) error {
	lvl, err := config.ParseLogLevel(f.levelStr)
	if err != nil {
		return err
	}
	var since time.Time
	if f.sinceStr != "" {
		d, err := time.ParseDuration(f.sinceStr)
		if err != nil {
			return fmt.Errorf("--since: %w", err)
		}
		since = time.Now().Add(-d)
	}
	// Resolve the state db under the same per-destination work root the download
	// used: --local-dir points at that dir's .cache/huggingface, otherwise the
	// HF cache. An explicit --state-db still wins inside StateDBPath. localDir is
	// absolutized so it matches the path buildPlan recorded.
	localDir := f.localDir
	if localDir != "" {
		abs, err := filepath.Abs(localDir)
		if err != nil {
			return fmt.Errorf("--local-dir: %w", err)
		}
		localDir = abs
	}
	workRoot := config.WorkRoot(localDir, config.CacheDir("", getenv))
	// store.Open takes the single-process exclusive advisory lock; `hfdl logs`
	// is a read-mostly dump and still obeys it — a second instance fails fast.
	dbPath := config.StateDBPath(f.stateDBFlag, workRoot)
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close(ctx) }()

	rows, err := st.QueryLogs(ctx, lvl, since, 0)
	if err != nil {
		return err
	}
	for _, r := range rows {
		line := fmt.Sprintf("%s %s %s %s",
			r.Ts.UTC().Format(time.RFC3339Nano),
			slog.Level(r.Level).String(),
			r.Source, r.Msg)
		if r.Attrs != "" {
			line += " " + r.Attrs
		}
		if _, err := fmt.Fprintln(out, line); err != nil {
			return fmt.Errorf("write log row: %w", err)
		}
	}
	return nil
}
