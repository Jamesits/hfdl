package transfer

import (
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestIntervalSetAddCoalesce(t *testing.T) {
	cases := []struct {
		name string
		adds [][2]int64
		want []Interval
	}{
		{"empty", nil, nil},
		{"single", [][2]int64{{10, 20}}, []Interval{{10, 20}}},
		{"disjoint ordered", [][2]int64{{0, 10}, {20, 30}}, []Interval{{0, 10}, {20, 30}}},
		{"disjoint unordered", [][2]int64{{20, 30}, {0, 10}}, []Interval{{0, 10}, {20, 30}}},
		{"adjacent merges", [][2]int64{{0, 10}, {10, 20}}, []Interval{{0, 20}}},
		{"adjacent left", [][2]int64{{10, 20}, {0, 10}}, []Interval{{0, 20}}},
		{"overlap merges", [][2]int64{{0, 15}, {10, 20}}, []Interval{{0, 20}}},
		{"contained", [][2]int64{{0, 30}, {10, 20}}, []Interval{{0, 30}}},
		{"contains", [][2]int64{{10, 20}, {0, 30}}, []Interval{{0, 30}}},
		{"bridge merges three", [][2]int64{{0, 10}, {20, 30}, {5, 25}}, []Interval{{0, 30}}},
		{"duplicate", [][2]int64{{0, 10}, {0, 10}}, []Interval{{0, 10}}},
		{"empty range ignored", [][2]int64{{10, 10}, {5, 5}}, nil},
		{"inverted ignored", [][2]int64{{20, 10}}, nil},
		{"gap preserved", [][2]int64{{0, 10}, {20, 30}, {40, 50}}, []Interval{{0, 10}, {20, 30}, {40, 50}}},
		{"fill gap", [][2]int64{{0, 10}, {20, 30}, {10, 20}}, []Interval{{0, 30}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s IntervalSet
			for _, a := range tc.adds {
				s.Add(a[0], a[1])
			}
			if !reflect.DeepEqual(s.iv, tc.want) {
				t.Fatalf("got %v, want %v", s.iv, tc.want)
			}
		})
	}
}

func TestIntervalSetMissing(t *testing.T) {
	cases := []struct {
		name  string
		adds  [][2]int64
		total int64
		want  []Interval
	}{
		{"empty set", nil, 100, []Interval{{0, 100}}},
		{"full", [][2]int64{{0, 100}}, 100, nil},
		{"head done", [][2]int64{{0, 30}}, 100, []Interval{{30, 100}}},
		{"tail done", [][2]int64{{70, 100}}, 100, []Interval{{0, 70}}},
		{"hole", [][2]int64{{0, 20}, {80, 100}}, 100, []Interval{{20, 80}}},
		{"holes", [][2]int64{{10, 20}, {40, 50}}, 100, []Interval{{0, 10}, {20, 40}, {50, 100}}},
		{"zero total", nil, 0, nil},
		{"set beyond total ignored", [][2]int64{{0, 200}}, 100, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s IntervalSet
			for _, a := range tc.adds {
				s.Add(a[0], a[1])
			}
			if got := s.Missing(tc.total); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSerdeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		size int64
		adds [][2]int64
	}{
		{"empty", 1000, nil},
		{"single", 1000, [][2]int64{{0, 1000}}},
		{"scattered", 1 << 20, [][2]int64{{0, 100}, {500, 600}, {1<<20 - 1, 1 << 20}}},
		{"adjacent coalesce", 100, [][2]int64{{0, 50}, {50, 100}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &IntervalSet{size: tc.size}
			for _, a := range tc.adds {
				s.Add(a[0], a[1])
			}
			blob, err := s.MarshalBinary()
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var back IntervalSet
			if err := back.UnmarshalBinary(blob); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if back.size != tc.size {
				t.Fatalf("size %d, want %d", back.size, tc.size)
			}
			if !s.equal(&back) {
				t.Fatalf("intervals %v, want %v", back.iv, s.iv)
			}
		})
	}
}

// TestSerdeRoundTripRandom is the property test: random adds, then
// marshal→unmarshal must be the identity on both intervals and missing set.
func TestSerdeRoundTripRandom(t *testing.T) {
	var seed [32]byte
	seed[0] = 0x42
	rng := rand.New(rand.NewChaCha8(seed))
	for iter := 0; iter < 500; iter++ {
		size := int64(1 + rng.Uint64N(1<<20))
		s := &IntervalSet{size: size}
		for i := 0; i < 1+rng.IntN(30); i++ {
			a := int64(rng.Uint64N(uint64(size)))
			b := int64(rng.Uint64N(uint64(size)))
			if a > b {
				a, b = b, a
			}
			s.Add(a, b)
		}
		blob, err := s.MarshalBinary()
		if err != nil {
			t.Fatalf("iter %d marshal: %v", iter, err)
		}
		var back IntervalSet
		if err := back.UnmarshalBinary(blob); err != nil {
			t.Fatalf("iter %d unmarshal: %v", iter, err)
		}
		if !s.equal(&back) || back.size != size {
			t.Fatalf("iter %d: round trip mismatch: %v/%d vs %v/%d", iter, back.iv, back.size, s.iv, s.size)
		}
		if !reflect.DeepEqual(s.Missing(size), back.Missing(size)) {
			t.Fatalf("iter %d: missing mismatch", iter)
		}
	}
}

func TestSerdeCorrupt(t *testing.T) {
	good := func() []byte {
		s := &IntervalSet{size: 1000}
		s.Add(0, 100)
		s.Add(200, 300)
		b, err := s.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}
	cases := []struct {
		name string
		blob []byte
	}{
		{"empty", nil},
		{"short", []byte("HF")},
		{"bad magic", append([]byte("XXXX"), good()[4:]...)},
		{"bad version", append([]byte("HFDP\x02"), good()[5:]...)},
		{"truncated size", []byte("HFDP\x01")},
		{"truncated count", []byte("HFDP\x01\xe8\x07")}, // size=1000, no count
		{"truncated interval", good()[:len(good())-1]},
		{"zero length", []byte("HFDP\x01\xe8\x07\x01\x00\x00")},       // 1 interval, startΔ=0 len=0
		{"out of bounds", []byte("HFDP\x01\x64\x01\x00\xc8\x01")},     // size=100, interval [0,200)
		{"count exceeds payload", []byte("HFDP\x01\xe8\x07\xff\x01")}, // claims 255 intervals
		{"trailing garbage", append(good(), 0x00)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s IntervalSet
			err := s.UnmarshalBinary(tc.blob)
			if err == nil {
				t.Fatalf("expected error for %q", tc.name)
			}
			var cpe *CorruptProgressError
			if !errors.As(err, &cpe) {
				t.Fatalf("error %v is not *CorruptProgressError", err)
			}
		})
	}
}

// TestSerdeAdjacentDecodesCoalesced: the format can encode adjacent (but
// non-overlapping) intervals; decode re-coalesces them into the canonical
// in-memory form.
func TestSerdeAdjacentDecodesCoalesced(t *testing.T) {
	// size=1000, 2 intervals: [0,100), [100,200)
	blob := []byte("HFDP\x01")
	blob = binary.AppendUvarint(blob, 1000)
	blob = binary.AppendUvarint(blob, 2)
	blob = binary.AppendUvarint(blob, 0)   // startΔ
	blob = binary.AppendUvarint(blob, 100) // len
	blob = binary.AppendUvarint(blob, 0)   // startΔ (adjacent)
	blob = binary.AppendUvarint(blob, 100) // len
	var s IntervalSet
	if err := s.UnmarshalBinary(blob); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := []Interval{{0, 200}}
	if !reflect.DeepEqual(s.iv, want) {
		t.Fatalf("got %v, want %v", s.iv, want)
	}
}
