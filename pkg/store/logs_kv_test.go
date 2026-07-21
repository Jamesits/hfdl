package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/jamesits/hfdl/pkg/logging"
)

func TestLogsRoundTripAndPrune(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	base := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

	rows := []logging.LogRow{
		{Time: base, Level: 0, Source: "sched", Msg: "info one", Attrs: `{"a":1}`},
		{Time: base.Add(time.Second), Level: 4, Source: "store", Msg: "warn two"},
		{Time: base.Add(2 * time.Second), Level: 8, Source: "transfer", Msg: "error three"},
	}
	if err := s.InsertLogs(ctx, rows); err != nil {
		t.Fatalf("InsertLogs: %v", err)
	}

	logs, err := s.QueryLogs(ctx, 0, time.Time{}, 10)
	if err != nil || len(logs) != 3 {
		t.Fatalf("QueryLogs = (%d, %v), want 3", len(logs), err)
	}
	if logs[0].Msg != "info one" || logs[1].Level != 4 || logs[2].Source != "transfer" {
		t.Errorf("QueryLogs order/fields = %+v", logs)
	}
	if !logs[0].Ts.Equal(base) {
		t.Errorf("ts = %v, want %v", logs[0].Ts, base)
	}

	// Level floor and since filter.
	logs, err = s.QueryLogs(ctx, 4, time.Time{}, 10)
	if err != nil || len(logs) != 2 {
		t.Fatalf("QueryLogs warn+ = (%d, %v), want 2", len(logs), err)
	}
	logs, err = s.QueryLogs(ctx, 0, base.Add(time.Second), 10)
	if err != nil || len(logs) != 2 {
		t.Fatalf("QueryLogs since = (%d, %v), want 2", len(logs), err)
	}
	logs, err = s.QueryLogs(ctx, 0, time.Time{}, 1)
	if err != nil || len(logs) != 1 {
		t.Fatalf("QueryLogs limit = (%d, %v), want 1", len(logs), err)
	}

	// Retention: keep the 2 newest.
	if err := s.PruneLogs(ctx, 2); err != nil {
		t.Fatalf("PruneLogs: %v", err)
	}
	logs, err = s.QueryLogs(ctx, -100, time.Time{}, 10)
	if err != nil || len(logs) != 2 {
		t.Fatalf("after prune = (%d, %v), want 2", len(logs), err)
	}
	if logs[0].Msg != "warn two" || logs[1].Msg != "error three" {
		t.Errorf("prune kept %q, %q — want the newest", logs[0].Msg, logs[1].Msg)
	}

	// Batched insert beyond one chunk.
	big := make([]logging.LogRow, 300)
	for i := range big {
		big[i] = logging.LogRow{Time: base, Level: 0, Msg: fmt.Sprintf("m%d", i)}
	}
	if err := s.InsertLogs(ctx, big); err != nil {
		t.Fatalf("InsertLogs 300: %v", err)
	}
	logs, err = s.QueryLogs(ctx, -100, time.Time{}, 1000)
	if err != nil || len(logs) != 302 {
		t.Fatalf("after batch = (%d, %v), want 302", len(logs), err)
	}
}

func TestKVAndCaps(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()

	if v, err := s.GetKV(ctx, "missing"); err != nil || v != "" {
		t.Fatalf("GetKV missing = (%q, %v), want (\"\", nil)", v, err)
	}
	if err := s.SetKV(ctx, "limits", `{"bw":"10M"}`); err != nil {
		t.Fatal(err)
	}
	if v, err := s.GetKV(ctx, "limits"); err != nil || v != `{"bw":"10M"}` {
		t.Fatalf("GetKV = (%q, %v)", v, err)
	}
	if err := s.SetKV(ctx, "limits", `{"bw":"20M"}`); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.GetKV(ctx, "limits"); v != `{"bw":"20M"}` {
		t.Fatalf("GetKV after update = %q", v)
	}

	if caps, err := s.GetCaps(ctx, "volcaps:42"); err != nil || caps != nil {
		t.Fatalf("GetCaps missing = (%v, %v), want (nil, nil)", caps, err)
	}
	want := []byte(`{"direct":true,"align":4096}`)
	if err := s.PutCaps(ctx, "volcaps:42", want); err != nil {
		t.Fatal(err)
	}
	caps, err := s.GetCaps(ctx, "volcaps:42")
	if err != nil || string(caps) != string(want) {
		t.Fatalf("GetCaps = (%s, %v)", caps, err)
	}
}
