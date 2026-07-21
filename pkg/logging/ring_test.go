package logging

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"
)

func TestRingEvictionKeepsNewest(t *testing.T) {
	r := NewRing(3)
	for i := 1; i <= 5; i++ {
		r.Write(Record{Msg: fmt.Sprintf("m%d", i)})
	}
	all := r.All()
	if len(all) != 3 {
		t.Fatalf("len(All) = %d, want 3", len(all))
	}
	for i, rec := range all {
		wantSeq := uint64(i + 3)
		wantMsg := fmt.Sprintf("m%d", i+3)
		if rec.Seq != wantSeq || rec.Msg != wantMsg {
			t.Fatalf("all[%d] = (seq %d, %q), want (seq %d, %q)", i, rec.Seq, rec.Msg, wantSeq, wantMsg)
		}
	}
}

func TestRingSincePaging(t *testing.T) {
	r := NewRing(20)
	for i := 0; i < 10; i++ {
		r.Write(Record{Msg: "m"})
	}
	if got := len(r.Since(0)); got != 10 {
		t.Fatalf("Since(0) = %d, want 10", got)
	}
	page := r.Since(4)
	if len(page) != 6 || page[0].Seq != 5 || page[5].Seq != 10 {
		t.Fatalf("Since(4) returned %d records, first seq %d", len(page), page[0].Seq)
	}
	if got := len(r.Since(10)); got != 0 {
		t.Fatalf("Since(10) = %d, want 0", got)
	}
}

func TestRingSinceAfterEviction(t *testing.T) {
	r := NewRing(5)
	for i := 0; i < 8; i++ {
		r.Write(Record{Msg: "m"})
	}
	// Cursor fell behind the eviction horizon: the caller sees everything
	// retained and detects the gap via the first Seq.
	page := r.Since(2)
	if len(page) != 5 || page[0].Seq != 4 {
		t.Fatalf("Since(2) = %d records first seq %d, want 5 records from seq 4", len(page), page[0].Seq)
	}
}

func TestRingCount(t *testing.T) {
	r := NewRing(10)
	r.Write(Record{Level: slog.LevelDebug})
	r.Write(Record{Level: slog.LevelInfo})
	r.Write(Record{Level: slog.LevelWarn})
	r.Write(Record{Level: slog.LevelError})
	tests := []struct {
		min  slog.Level
		want int
	}{
		{slog.LevelDebug, 4},
		{slog.LevelInfo, 3},
		{slog.LevelWarn, 2},
		{slog.LevelError, 1},
	}
	for _, tc := range tests {
		if got := r.Count(tc.min); got != tc.want {
			t.Fatalf("Count(%v) = %d, want %d", tc.min, got, tc.want)
		}
	}
}

func TestRingDefaultCap(t *testing.T) {
	r := NewRing(0)
	for i := 0; i < defaultRingCap+100; i++ {
		r.Write(Record{Msg: "m"})
	}
	if got := len(r.All()); got != defaultRingCap {
		t.Fatalf("default cap holds %d, want %d", got, defaultRingCap)
	}
}

func TestRingConcurrentWrite(t *testing.T) {
	r := NewRing(64 * 200)
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				r.Write(Record{Msg: "m", Level: slog.LevelWarn})
			}
		}()
	}
	wg.Wait()
	all := r.All()
	if len(all) != 64*200 {
		t.Fatalf("len(All) = %d, want %d", len(all), 64*200)
	}
	for i, rec := range all {
		if rec.Seq != uint64(i+1) {
			t.Fatalf("all[%d].Seq = %d, want %d (seqs must be gapless and ordered)", i, rec.Seq, i+1)
		}
	}
	if got := r.Count(slog.LevelWarn); got != 64*200 {
		t.Fatalf("Count(Warn) = %d", got)
	}
}
