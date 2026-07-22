package transfer

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Serde format: "HFDP" magic | 1B version | varint file size |
// varint count | delta-varint (startΔ, length) per interval, where startΔ is
// relative to the previous interval's end (0 for the first). Deltas keep
// varints small for dense sets and make non-monotonic input detectable.
const (
	serdeMagic   = "HFDP"
	serdeVersion = 1
)

// CorruptProgressError rejects a malformed progress blob on decode: bad
// magic or version, non-monotonic or overlapping intervals, out-of-bounds
// ends, or truncation. The caller resets the file to queued (full
// redownload) — the blob is the only resume record, so guessing is worse
// than restarting.
type CorruptProgressError struct {
	Reason string
}

func (e *CorruptProgressError) Error() string {
	return "transfer: corrupt progress blob: " + e.Reason
}

// MarshalBinary encodes the set. The blob is self-describing (carries the
// file size) so decode can validate bounds without external context. The set
// is validated first: a corrupt in-memory set must never be emitted as a blob
// its own decoder would reject (or worse, accept as different data).
func (s *IntervalSet) MarshalBinary() ([]byte, error) {
	if s.size < 0 {
		return nil, &CorruptProgressError{Reason: fmt.Sprintf("negative file size %d", s.size)}
	}
	prevEnd := int64(0)
	for i, v := range s.iv {
		if v.End <= v.Start {
			return nil, &CorruptProgressError{Reason: fmt.Sprintf("interval %d empty or reversed [%d,%d)", i, v.Start, v.End)}
		}
		if v.Start < prevEnd {
			return nil, &CorruptProgressError{Reason: fmt.Sprintf("interval %d start %d overlaps previous end %d", i, v.Start, prevEnd)}
		}
		if v.End > s.size {
			return nil, &CorruptProgressError{Reason: fmt.Sprintf("interval %d end %d exceeds size %d", i, v.End, s.size)}
		}
		prevEnd = v.End
	}
	out := make([]byte, 0, 16+len(s.iv)*8)
	out = append(out, serdeMagic...)
	out = append(out, serdeVersion)
	out = binary.AppendUvarint(out, uint64(s.size))
	out = binary.AppendUvarint(out, uint64(len(s.iv)))
	prevEnd = 0
	for _, v := range s.iv {
		out = binary.AppendUvarint(out, uint64(v.Start-prevEnd))
		out = binary.AppendUvarint(out, uint64(v.End-v.Start))
		prevEnd = v.End
	}
	return out, nil
}

// UnmarshalBinary decodes a blob, validating magic, version, monotonicity,
// bounds and non-overlap. Any violation is a *CorruptProgressError.
func (s *IntervalSet) UnmarshalBinary(b []byte) error {
	if len(b) < 5 || string(b[:4]) != serdeMagic {
		return &CorruptProgressError{Reason: "bad magic"}
	}
	if b[4] != serdeVersion {
		return &CorruptProgressError{Reason: fmt.Sprintf("unsupported version %d", b[4])}
	}
	rest := b[5:]
	size, n := binary.Uvarint(rest)
	if n <= 0 {
		return &CorruptProgressError{Reason: "truncated file size"}
	}
	// Every subsequent bound check is done in int64. A uvarint ≥ 2^63 would
	// wrap negative on the int64 cast and silently bypass the end<=size and
	// monotonicity checks, so reject it up front rather than propagate a
	// poisoned value.
	if size > math.MaxInt64 {
		return &CorruptProgressError{Reason: fmt.Sprintf("file size %d exceeds int64 range", size)}
	}
	rest = rest[n:]
	count, n := binary.Uvarint(rest)
	if n <= 0 {
		return &CorruptProgressError{Reason: "truncated interval count"}
	}
	rest = rest[n:]
	if count > uint64(len(rest))+1 { // each interval costs at least 2 bytes
		return &CorruptProgressError{Reason: "interval count exceeds payload"}
	}
	iv := make([]Interval, 0, count)
	prevEnd := int64(0)
	for i := range count {
		delta, n := binary.Uvarint(rest)
		if n <= 0 {
			return &CorruptProgressError{Reason: fmt.Sprintf("truncated interval %d start", i)}
		}
		rest = rest[n:]
		length, n := binary.Uvarint(rest)
		if n <= 0 {
			return &CorruptProgressError{Reason: fmt.Sprintf("truncated interval %d length", i)}
		}
		rest = rest[n:]
		if length == 0 {
			return &CorruptProgressError{Reason: fmt.Sprintf("interval %d has zero length", i)}
		}
		if delta > math.MaxInt64 || length > math.MaxInt64 {
			return &CorruptProgressError{Reason: fmt.Sprintf("interval %d delta/length exceeds int64 range", i)}
		}
		// Checked additions: start = prevEnd + delta, end = start + length,
		// both bounded by size (≤ MaxInt64), so any overflow is corruption.
		if int64(delta) > math.MaxInt64-prevEnd {
			return &CorruptProgressError{Reason: fmt.Sprintf("interval %d start overflows", i)}
		}
		start := prevEnd + int64(delta)
		if int64(length) > math.MaxInt64-start {
			return &CorruptProgressError{Reason: fmt.Sprintf("interval %d end overflows", i)}
		}
		end := start + int64(length)
		if end > int64(size) {
			return &CorruptProgressError{Reason: fmt.Sprintf("interval %d end %d out of bounds (size %d)", i, end, size)}
		}
		// Adjacent intervals (start == prevEnd) decode fine but are
		// re-coalesced on insert so the in-memory invariant always holds.
		iv = append(iv, Interval{start, end})
		prevEnd = end
	}
	if len(rest) != 0 {
		return &CorruptProgressError{Reason: "trailing garbage"}
	}
	merged := &IntervalSet{size: int64(size)}
	for _, v := range iv {
		merged.Add(v.Start, v.End)
	}
	*s = *merged
	return nil
}
