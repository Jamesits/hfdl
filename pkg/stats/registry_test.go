package stats

import (
	"math"
	"sync"
	"testing"
	"time"
)

func newTestRegistry() (*Registry, *fakeClock) {
	c := newFakeClock()
	return NewWithClock(c.now), c
}

func TestCounterAccumulation(t *testing.T) {
	r, _ := newTestRegistry()

	r.AddNetwork(100)
	r.AddNetwork(50)
	r.AddSalvaged(25)
	r.AddFile(1, 40)
	r.AddFile(1, 60)
	r.AddFile(2, 7)
	r.SetFileTotal(1, 1000)
	r.SetFileTotal(2, 70)
	r.AddUpstream("cdn-a", 90)
	r.AddUpstream("cdn-a", 10)
	r.AddUpstream("cdn-b", 5)
	r.AddStall("cdn-a")
	r.AddStall("cdn-a")
	r.AddRetry("cdn-b")

	s := r.Snapshot()
	if s.TotalNetwork != 150 {
		t.Errorf("TotalNetwork = %d, want 150", s.TotalNetwork)
	}
	if s.TotalSalvaged != 25 {
		t.Errorf("TotalSalvaged = %d, want 25", s.TotalSalvaged)
	}
	if s.Stalls != 2 || s.Retries != 1 {
		t.Errorf("Stalls/Retries = %d/%d, want 2/1", s.Stalls, s.Retries)
	}

	f1 := s.Files[1]
	if f1.Done != 100 || f1.Total != 1000 {
		t.Errorf("file 1 Done/Total = %d/%d, want 100/1000", f1.Done, f1.Total)
	}
	f2 := s.Files[2]
	if f2.Done != 7 || f2.Total != 70 {
		t.Errorf("file 2 Done/Total = %d/%d, want 7/70", f2.Done, f2.Total)
	}
	if s.Upstreams["cdn-a"].Bytes != 100 {
		t.Errorf("cdn-a Bytes = %d, want 100", s.Upstreams["cdn-a"].Bytes)
	}
	if s.Upstreams["cdn-b"].Bytes != 5 {
		t.Errorf("cdn-b Bytes = %d, want 5", s.Upstreams["cdn-b"].Bytes)
	}
}

func TestEMAUpdateAndPenalize(t *testing.T) {
	r, _ := newTestRegistry()

	r.ReportUpstreamRate("cdn-a", 100)
	// ema = 0.2*100 + 0.8*0 = 20
	if got := r.Snapshot().Upstreams["cdn-a"].EMABps; math.Abs(got-20) > 1e-9 {
		t.Fatalf("EMA after first sample = %v, want 20", got)
	}

	r.ReportUpstreamRate("cdn-a", 100)
	// ema = 0.2*100 + 0.8*20 = 36
	if got := r.Snapshot().Upstreams["cdn-a"].EMABps; math.Abs(got-36) > 1e-9 {
		t.Fatalf("EMA after second sample = %v, want 36", got)
	}

	r.PenalizeUpstream("cdn-a")
	// ema *= 0.5 → 18
	if got := r.Snapshot().Upstreams["cdn-a"].EMABps; math.Abs(got-18) > 1e-9 {
		t.Fatalf("EMA after penalize = %v, want 18", got)
	}
}

func TestFileAndGlobalEMA(t *testing.T) {
	r, _ := newTestRegistry()

	// Per-file EMA (α=0.2): 0.2*100 + 0.8*0 = 20, then 0.2*100 + 0.8*20 = 36.
	r.ReportFileRate(1, 100)
	if got := r.Snapshot().Files[1].EMABps; math.Abs(got-20) > 1e-9 {
		t.Fatalf("file EMA after first sample = %v, want 20", got)
	}
	r.ReportFileRate(1, 100)
	if got := r.Snapshot().Files[1].EMABps; math.Abs(got-36) > 1e-9 {
		t.Fatalf("file EMA after second sample = %v, want 36", got)
	}

	// Global EMA is fed by ReportUpstreamRate: 0.2*100 + 0.8*0 = 20.
	r.ReportUpstreamRate("cdn-a", 100)
	if got := r.Snapshot().GlobalEMABps; math.Abs(got-20) > 1e-9 {
		t.Fatalf("global EMA after first sample = %v, want 20", got)
	}
}

func TestUpstreamWindowedRate(t *testing.T) {
	r, c := newTestRegistry()
	r.AddUpstream("cdn-a", 100)
	c.advance(time.Second)
	r.AddUpstream("cdn-a", 50)
	c.advance(time.Second)
	// 150 B over the 10s window -> 15 B/s.
	if got := r.Snapshot().Upstreams["cdn-a"].WindowedRate; got != 15 {
		t.Fatalf("upstream WindowedRate = %v, want 15", got)
	}
	// Whole window expires -> rate drains, cumulative bytes stay.
	c.advance(20 * time.Second)
	s := r.Snapshot()
	if s.Upstreams["cdn-a"].WindowedRate != 0 {
		t.Fatalf("WindowedRate after expiry = %v, want 0", s.Upstreams["cdn-a"].WindowedRate)
	}
	if s.Upstreams["cdn-a"].Bytes != 150 {
		t.Fatalf("upstream Bytes = %d, want 150", s.Upstreams["cdn-a"].Bytes)
	}
}

func TestConnEndUnpairedClampsAtZero(t *testing.T) {
	r, _ := newTestRegistry()
	r.ConnStart(1, "cdn-a")

	// Duplicate/unpaired ends must not drive gauges negative.
	r.ConnEnd(1, "cdn-a") // the real pairing
	r.ConnEnd(1, "cdn-a") // duplicate: dropped
	r.ConnEnd(1, "cdn-a") // duplicate: dropped
	r.ConnEnd(2, "cdn-b") // never started: dropped

	s := r.Snapshot()
	if s.Conns != 0 {
		t.Errorf("global Conns = %d, want 0 (no negative)", s.Conns)
	}
	if got := s.Upstreams["cdn-a"].Conns; got != 0 {
		t.Errorf("cdn-a Conns = %d, want 0 (no negative)", got)
	}
}

func TestWindowedRatesWithFakeClock(t *testing.T) {
	r, c := newTestRegistry()

	r.AddNetwork(100)
	r.AddFile(1, 100)
	c.advance(time.Second)
	r.AddNetwork(50)
	r.AddFile(1, 30)
	c.advance(time.Second)

	s := r.Snapshot()
	// 150 B in the 10s window → 15 B/s globally, 130/10 per file.
	if s.GlobalRate != 15 {
		t.Errorf("GlobalRate = %v, want 15", s.GlobalRate)
	}
	if s.Files[1].Rate != 13 {
		t.Errorf("file 1 Rate = %v, want 13", s.Files[1].Rate)
	}

	// Let the whole window expire: rates must drain to zero while the
	// cumulative counters stay put.
	c.advance(20 * time.Second)
	s = r.Snapshot()
	if s.GlobalRate != 0 || s.Files[1].Rate != 0 {
		t.Errorf("rates after expiry = %v/%v, want 0/0", s.GlobalRate, s.Files[1].Rate)
	}
	if s.TotalNetwork != 150 || s.Files[1].Done != 130 {
		t.Errorf("counters after expiry = %d/%d, want 150/130", s.TotalNetwork, s.Files[1].Done)
	}
}

func TestConnGauges(t *testing.T) {
	r, _ := newTestRegistry()

	r.ConnStart(1, "cdn-a")
	r.ConnStart(1, "cdn-a")
	r.ConnStart(1, "cdn-b")
	r.ConnStart(2, "cdn-a")

	s := r.Snapshot()
	if s.Conns != 4 {
		t.Errorf("global Conns = %d, want 4", s.Conns)
	}
	if got := s.Files[1].Conns; got != 3 {
		t.Errorf("file 1 Conns = %d, want 3", got)
	}
	if got := s.Files[2].Conns; got != 1 {
		t.Errorf("file 2 Conns = %d, want 1", got)
	}
	if got := s.Upstreams["cdn-a"].Conns; got != 3 {
		t.Errorf("cdn-a Conns = %d, want 3", got)
	}
	if got := s.Upstreams["cdn-b"].Conns; got != 1 {
		t.Errorf("cdn-b Conns = %d, want 1", got)
	}
	wantUps := []string{"cdn-a", "cdn-b"}
	gotUps := s.Files[1].Upstreams
	if len(gotUps) != len(wantUps) || gotUps[0] != wantUps[0] || gotUps[1] != wantUps[1] {
		t.Errorf("file 1 Upstreams = %v, want %v", gotUps, wantUps)
	}

	// One of the two cdn-a conns on file 1 ends: cdn-a stays listed.
	r.ConnEnd(1, "cdn-a")
	s = r.Snapshot()
	if got := s.Files[1].Upstreams; len(got) != 2 {
		t.Errorf("file 1 Upstreams after one ConnEnd = %v, want 2 entries", got)
	}
	if s.Conns != 3 || s.Files[1].Conns != 2 || s.Upstreams["cdn-a"].Conns != 2 {
		t.Errorf("gauges after ConnEnd = %d/%d/%d, want 3/2/2",
			s.Conns, s.Files[1].Conns, s.Upstreams["cdn-a"].Conns)
	}

	// Last cdn-a conn on file 1 ends: only cdn-b remains.
	r.ConnEnd(1, "cdn-a")
	s = r.Snapshot()
	if got := s.Files[1].Upstreams; len(got) != 1 || got[0] != "cdn-b" {
		t.Errorf("file 1 Upstreams = %v, want [cdn-b]", got)
	}

	r.ConnEnd(1, "cdn-b")
	r.ConnEnd(2, "cdn-a")
	s = r.Snapshot()
	if s.Conns != 0 || s.Files[1].Conns != 0 || s.Upstreams["cdn-a"].Conns != 0 {
		t.Errorf("gauges after all ConnEnd = %d/%d/%d, want 0/0/0",
			s.Conns, s.Files[1].Conns, s.Upstreams["cdn-a"].Conns)
	}
	if len(s.Files[1].Upstreams) != 0 {
		t.Errorf("file 1 Upstreams = %v, want empty", s.Files[1].Upstreams)
	}
}

func TestRemoveFile(t *testing.T) {
	r, _ := newTestRegistry()

	r.AddFile(1, 10)
	r.AddFile(2, 20)
	r.ConnStart(1, "cdn-a") // straggler still open when the file finishes
	r.ConnStart(2, "cdn-a")
	r.RemoveFile(1)

	s := r.Snapshot()
	if _, ok := s.Files[1]; ok {
		t.Error("file 1 still present after RemoveFile")
	}
	if _, ok := s.Files[2]; !ok {
		t.Error("file 2 missing after unrelated RemoveFile")
	}
	// RemoveFile reconciles file 1's one live conn out of the global and
	// upstream gauges immediately, leaving only file 2's conn.
	if s.Conns != 1 || s.Upstreams["cdn-a"].Conns != 1 {
		t.Errorf("gauges after RemoveFile = %d/%d, want 1/1", s.Conns, s.Upstreams["cdn-a"].Conns)
	}

	// The straggler's ConnEnd arrives after removal: it is unpaired now (the
	// file entry is gone and was already reconciled), so it is dropped — the
	// gauges must NOT be decremented a second time, nor the file recreated.
	r.ConnEnd(1, "cdn-a")
	s = r.Snapshot()
	if _, ok := s.Files[1]; ok {
		t.Error("ConnEnd recreated removed file 1")
	}
	if got := s.Upstreams["cdn-a"].Conns; got != 1 {
		t.Errorf("cdn-a Conns = %d, want 1", got)
	}
	if got := s.Conns; got != 1 {
		t.Errorf("global Conns = %d, want 1", got)
	}
}

func TestRemoveFileWaitsForReachableUpdate(t *testing.T) {
	r, _ := newTestRegistry()
	r.AddFile(1, 1)
	updateStarted := make(chan struct{})
	releaseUpdate := make(chan struct{})
	updateDone := make(chan struct{})
	go func() {
		r.updateFile(1, func(f *fileEntry) {
			close(updateStarted)
			<-releaseUpdate
			f.done.Add(1)
		})
		close(updateDone)
	}()
	<-updateStarted

	removeDone := make(chan struct{})
	go func() {
		r.RemoveFile(1)
		close(removeDone)
	}()
	select {
	case <-removeDone:
		t.Fatal("RemoveFile returned while an update still held the entry reachable")
	case <-time.After(10 * time.Millisecond):
	}
	close(releaseUpdate)
	<-updateDone
	<-removeDone
	if _, ok := r.Snapshot().Files[1]; ok {
		t.Fatal("in-flight update resurrected file after RemoveFile returned")
	}
}

func TestSnapshotDeepCopyIsolation(t *testing.T) {
	r, _ := newTestRegistry()
	r.AddNetwork(10)
	r.AddFile(1, 10)
	r.ConnStart(1, "cdn-a")
	r.AddUpstream("cdn-a", 10)

	s1 := r.Snapshot()

	// Mutate the snapshot every way possible.
	fs := s1.Files[1]
	fs.Done = -999
	s1.Files[1] = fs
	s1.Files[1].Upstreams[0] = "corrupted"
	s1.Files[99] = FileStat{Done: 1}
	delete(s1.Files, 1)
	us := s1.Upstreams["cdn-a"]
	us.Bytes = -1
	s1.Upstreams["cdn-a"] = us
	s1.Upstreams["bogus"] = UpstreamStat{}
	s1.TotalNetwork = -1
	s1.Conns = -1

	s2 := r.Snapshot()
	if s2.TotalNetwork != 10 || s2.Conns != 1 {
		t.Errorf("registry scalars corrupted: %d/%d", s2.TotalNetwork, s2.Conns)
	}
	if len(s2.Files) != 1 {
		t.Fatalf("Files len = %d, want 1", len(s2.Files))
	}
	f := s2.Files[1]
	if f.Done != 10 || len(f.Upstreams) != 1 || f.Upstreams[0] != "cdn-a" {
		t.Errorf("file 1 corrupted: %+v", f)
	}
	if len(s2.Upstreams) != 1 || s2.Upstreams["cdn-a"].Bytes != 10 {
		t.Errorf("upstreams corrupted: %+v", s2.Upstreams)
	}
}

func TestConcurrentAddsRaceClean(t *testing.T) {
	r, c := newTestRegistry()
	_ = c

	const goroutines = 128
	const iters = 500

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			fileID := int64(g % 8)
			upstream := []string{"cdn-a", "cdn-b", "cdn-c"}[g%3]
			for i := range iters {
				r.AddNetwork(1)
				r.AddFile(fileID, 1)
				r.AddUpstream(upstream, 1)
				r.ReportUpstreamRate(upstream, 100)
				r.ConnStart(fileID, upstream)
				r.ConnEnd(fileID, upstream)
				if i%7 == 0 {
					r.AddStall(upstream)
					r.AddRetry(upstream)
					r.PenalizeUpstream(upstream)
				}
				if i%11 == 0 {
					r.Snapshot()
				}
			}
		}(g)
	}
	wg.Wait()

	s := r.Snapshot()
	want := int64(goroutines * iters)
	if s.TotalNetwork != want {
		t.Errorf("TotalNetwork = %d, want %d", s.TotalNetwork, want)
	}
	var fileDone int64
	for _, f := range s.Files {
		fileDone += f.Done
	}
	if fileDone != want {
		t.Errorf("sum of file Done = %d, want %d", fileDone, want)
	}
	var upBytes int64
	for _, u := range s.Upstreams {
		upBytes += u.Bytes
	}
	if upBytes != want {
		t.Errorf("sum of upstream Bytes = %d, want %d", upBytes, want)
	}
	if s.Conns != 0 {
		t.Errorf("Conns = %d, want 0 after paired Start/End", s.Conns)
	}
	for id, f := range s.Files {
		if f.Conns != 0 || len(f.Upstreams) != 0 {
			t.Errorf("file %d leftover conns: %+v", id, f)
		}
	}
	for name, u := range s.Upstreams {
		if u.Conns != 0 {
			t.Errorf("upstream %s leftover conns: %d", name, u.Conns)
		}
		if u.EMABps <= 0 {
			t.Errorf("upstream %s EMA = %v, want > 0", name, u.EMABps)
		}
	}
}
