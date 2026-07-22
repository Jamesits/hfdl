package netcfg

import "testing"

func TestParseIPQoS(t *testing.T) {
	cases := []struct {
		spec     string
		api      int
		download int
		otel     int
		wantSpec string
		wantErr  bool
	}{
		// 1-2 values set api/download; telemetry keeps its cs1 default.
		{spec: "", api: 0x48, download: 0x20, otel: 0x20, wantSpec: DefaultIPQoS},
		{spec: "af21,cs1", api: 0x48, download: 0x20, otel: 0x20, wantSpec: "af21,cs1"},
		{spec: "ef", api: 0xb8, download: 0xb8, otel: 0x20, wantSpec: "ef"},
		{spec: "none", api: TOSNone, download: TOSNone, otel: 0x20, wantSpec: "none"},
		{spec: "lowdelay,throughput", api: 0x10, download: 0x08, otel: 0x20, wantSpec: "lowdelay,throughput"},
		{spec: "af21,cs1,none", api: 0x48, download: 0x20, otel: TOSNone, wantSpec: "af21,cs1,none"},
		{spec: "AF21, CS1", api: 0x48, download: 0x20, otel: 0x20, wantSpec: "AF21,CS1"},
		// Numeric values must be 0x-prefixed hex.
		{spec: "0x48,0x20,0xB8", api: 0x48, download: 0x20, otel: 0xb8, wantSpec: "0x48,0x20,0xB8"},
		{spec: "0X48", api: 0x48, download: 0x48, otel: 0x20, wantSpec: "0X48"},
		{spec: "af21,0x20,ef", api: 0x48, download: 0x20, otel: 0xb8, wantSpec: "af21,0x20,ef"},
		{spec: "bogus", wantErr: true},
		{spec: "32", wantErr: true},  // bare decimal: 0x prefix required
		{spec: "010", wantErr: true}, // bare octal: 0x prefix required
		{spec: "0x100", wantErr: true},
		{spec: "0x", wantErr: true},
		{spec: "-0x1", wantErr: true},
		{spec: "af21 cs1", wantErr: true}, // space is not a separator
		{spec: "af21,,cs1", wantErr: true},
		{spec: "af21,cs1,le,ef", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.spec, func(t *testing.T) {
			q, err := ParseIPQoS(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseIPQoS(%q): expected error, got %+v", tc.spec, q)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseIPQoS(%q): %v", tc.spec, err)
			}
			if q.API != tc.api || q.Download != tc.download || q.Telemetry != tc.otel {
				t.Fatalf("classes = %#x/%#x/%#x, want %#x/%#x/%#x",
					q.API, q.Download, q.Telemetry, tc.api, tc.download, tc.otel)
			}
			if q.String() != tc.wantSpec {
				t.Fatalf("String() = %q, want %q", q.String(), tc.wantSpec)
			}
		})
	}
}

func TestParseIPQoSKeywordTable(t *testing.T) {
	// Spot-check the OpenSSH table identity: DSCP << 2 for the classed
	// keywords.
	for name, dscp := range map[string]int{"af11": 10, "af43": 38, "cs6": 48, "ef": 46, "le": 1} {
		q, err := ParseIPQoS(name)
		if err != nil {
			t.Fatal(err)
		}
		if q.API != dscp<<2 {
			t.Fatalf("%s = %#x, want %#x", name, q.API, dscp<<2)
		}
	}
}
