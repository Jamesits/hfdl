package main

import (
	"strings"
	"testing"

	"github.com/jamesits/hfdl/pkg/sched"
)

// TestPrintDryRunExactBytes pins the hf 1.24.0 stdout byte format: summary
// (trailing period, would-download-only total), TSV header, files sorted by
// name, SI 1-decimal sizes, "-" for fully cached files.
func TestPrintDryRunExactBytes(t *testing.T) {
	report := []sched.DryRunEntry{
		{Path: "model.safetensors", Size: 4_900_000, Cached: false},
		{Path: "config.json", Size: 973, Cached: false},
		{Path: "cached.bin", Size: 12_000_000_000, Cached: true},
	}
	var out strings.Builder
	if err := printDryRun(&out, report); err != nil {
		t.Fatal(err)
	}
	want := "[dry-run] Will download 2 files (out of 3) totalling 4.9M.\n" +
		"file\tsize\n" +
		"cached.bin\t-\n" +
		"config.json\t973.0\n" +
		"model.safetensors\t4.9M\n"
	if out.String() != want {
		t.Fatalf("stdout mismatch:\n got %q\nwant %q", out.String(), want)
	}
}

func TestPrintDryRunAllCached(t *testing.T) {
	report := []sched.DryRunEntry{
		{Path: "a.bin", Size: 2048, Cached: true},
	}
	var out strings.Builder
	if err := printDryRun(&out, report); err != nil {
		t.Fatal(err)
	}
	want := "[dry-run] Will download 0 files (out of 1) totalling 0.0.\n" +
		"file\tsize\n" +
		"a.bin\t-\n"
	if out.String() != want {
		t.Fatalf("stdout mismatch:\n got %q\nwant %q", out.String(), want)
	}
}

func TestPrintDryRunEmpty(t *testing.T) {
	var out strings.Builder
	if err := printDryRun(&out, nil); err != nil {
		t.Fatal(err)
	}
	want := "[dry-run] Will download 0 files (out of 0) totalling 0.0.\nfile\tsize\n"
	if out.String() != want {
		t.Fatalf("stdout mismatch:\n got %q\nwant %q", out.String(), want)
	}
}

// TestDryRunFormatSize pins huggingface_hub.utils._format_size: SI units,
// one decimal, empty unit for bytes (973 → "973.0", not "973B").
func TestDryRunFormatSize(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0.0"},
		{1, "1.0"},
		{973, "973.0"},
		{999, "999.0"},
		{1000, "1.0K"},
		{4900, "4.9K"},
		{999_499, "999.5K"},
		{1_000_000, "1.0M"},
		{4_900_000_000, "4.9G"},
		{2_500_000_000_000, "2.5T"},
		{1_000_000_000_000_000_000, "1.0E"},
	}
	for _, tc := range cases {
		if got := dryRunFormatSize(tc.in); got != tc.want {
			t.Errorf("dryRunFormatSize(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
