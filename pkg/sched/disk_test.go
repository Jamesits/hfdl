package sched

import (
	"runtime"
	"testing"

	"github.com/jamesits/hfdl/pkg/fcio"
)

// TestDiskWorkerCount pins GOMAXPROCS so the starvation cap is deterministic,
// then checks the override/auto/media matrix against it.
func TestDiskWorkerCount(t *testing.T) {
	// A 4-thread box: cap = GOMAXPROCS-1 = 3.
	restore := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(restore)

	cases := []struct {
		name     string
		fs       fcio.FsType
		override int
		want     int
	}{
		{"auto ssd", fcio.FsSSD, 0, 2},
		{"auto hdd", fcio.FsHDD, 0, 1},
		{"auto netfs", fcio.FsNetFS, 0, 1},
		{"auto unknown", fcio.FsUnknown, 0, 1},
		{"negative treated as auto", fcio.FsSSD, -1, 2},
		{"override below cap", fcio.FsHDD, 3, 3},
		{"override above cap clamps", fcio.FsSSD, 99, 3},
		{"override one", fcio.FsSSD, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := diskWorkerCount(tc.fs, tc.override); got != tc.want {
				t.Fatalf("diskWorkerCount(%v, %d) = %d, want %d", tc.fs, tc.override, got, tc.want)
			}
		})
	}
}

// TestMaxDiskWorkersFloor verifies the cap never drops below 1, even on a
// single schedulable thread where GOMAXPROCS-1 == 0.
func TestMaxDiskWorkersFloor(t *testing.T) {
	restore := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(restore)

	if got := maxDiskWorkers(); got != 1 {
		t.Fatalf("maxDiskWorkers() with GOMAXPROCS=1 = %d, want 1", got)
	}
	// Even an explicit SSD auto-value of 2 must clamp to the single-thread floor.
	if got := diskWorkerCount(fcio.FsSSD, 0); got != 1 {
		t.Fatalf("diskWorkerCount(ssd, auto) with GOMAXPROCS=1 = %d, want 1", got)
	}
	if got := diskWorkerCount(fcio.FsSSD, 8); got != 1 {
		t.Fatalf("diskWorkerCount(ssd, 8) with GOMAXPROCS=1 = %d, want 1", got)
	}
}
