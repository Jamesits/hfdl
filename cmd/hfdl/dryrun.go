package main

import (
	"fmt"
	"io"
	"sort"

	"github.com/jamesits/hfdl/pkg/sched"
)

// printDryRun renders the pinned hf 1.24.0 dry-run report to stdout,
// byte-for-byte (huggingface_hub/cli/download.py `_print_result` + agent-mode
// `out.table`): the summary line (trailing period, totalling sums only the
// would-download files), a TSV header, then one line per selected file —
// sorted by filename, humanized SI size, "-" when fully cached. No `path=`
// line is ever printed for a dry run, and quiet mode does not change this.
func printDryRun(w io.Writer, report []sched.DryRunEntry) error {
	entries := append([]sched.DryRunEntry(nil), report...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })

	var willDownload, total int64
	for _, e := range entries {
		if !e.Cached {
			willDownload++
			total += e.Size
		}
	}
	if _, err := fmt.Fprintf(w, "[dry-run] Will download %d files (out of %d) totalling %s.\n",
		willDownload, len(entries), dryRunFormatSize(total)); err != nil {
		return fmt.Errorf("write dry-run summary: %w", err)
	}
	if _, err := fmt.Fprintln(w, "file\tsize"); err != nil {
		return fmt.Errorf("write dry-run header: %w", err)
	}
	for _, e := range entries {
		size := "-"
		if !e.Cached {
			size = dryRunFormatSize(e.Size)
		}
		if _, err := fmt.Fprintf(w, "%s\t%s\n", e.Path, size); err != nil {
			return fmt.Errorf("write dry-run row: %w", err)
		}
	}
	return nil
}

// dryRunFormatSize is huggingface_hub.utils._format_size (Stack Overflow
// 1094933 port): SI units, one decimal, no "B" suffix for bytes — 973 bytes
// renders "973.0", 4900 renders "4.9K".
func dryRunFormatSize(num int64) string {
	f := float64(num)
	for _, unit := range []string{"", "K", "M", "G", "T", "P", "E", "Z"} {
		if f < 1000.0 && f > -1000.0 {
			return fmt.Sprintf("%.1f%s", f, unit)
		}
		f /= 1000.0
	}
	return fmt.Sprintf("%.1fY", f)
}
