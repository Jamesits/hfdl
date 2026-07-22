package transfer

import "sort"

// Interval is a half-open byte range [Start, End) inside a file.
type Interval struct {
	Start, End int64
}

// IntervalSet is a sorted, coalesced set of [start,end) ranges. It is the
// durable record of which bytes are on disk, deliberately independent of
// block segmentation: block rows are an ephemeral scheduling view re-derived
// from this set on resume. Not goroutine-safe; the downloader guards it with
// intervalTracker.
//
// size is the total file size carried through the serde format (the blob is
// self-describing so decode can bounds-check); it is set by UnmarshalBinary
// or by the downloader seeding the set for a run.
type IntervalSet struct {
	iv   []Interval
	size int64
}

// Add merges [start,end) into the set, coalescing overlapping and adjacent
// ranges. Empty or inverted ranges are ignored.
func (s *IntervalSet) Add(start, end int64) {
	if end <= start {
		return
	}
	iv := s.iv
	// First interval whose End >= start: anything before it is strictly
	// left of the new range (its End < start, so no overlap and no
	// adjacency) and stays untouched.
	i := sort.Search(len(iv), func(i int) bool { return iv[i].End >= start })
	j := i
	ns, ne := start, end
	for j < len(iv) && iv[j].Start <= end {
		if iv[j].Start < ns {
			ns = iv[j].Start
		}
		if iv[j].End > ne {
			ne = iv[j].End
		}
		j++
	}
	// Replace iv[i:j] (all overlapping/adjacent) with the single merged
	// range; when j == i this is a pure insert at i.
	l := len(iv)
	iv = append(iv, Interval{})
	copy(iv[i+1:], iv[j:])
	iv = iv[:l-(j-i)+1]
	iv[i] = Interval{ns, ne}
	s.iv = iv
}

// Missing returns the complement of the set within [0, total) — the input to
// re-chunking on resume.
func (s *IntervalSet) Missing(total int64) []Interval {
	var out []Interval
	next := int64(0)
	for _, v := range s.iv {
		if v.Start > next {
			out = append(out, Interval{next, min(v.Start, total)})
		}
		if v.End > next {
			next = v.End
		}
		if next >= total {
			return out
		}
	}
	if next < total {
		out = append(out, Interval{next, total})
	}
	return out
}

// contains reports whether [start,end) is fully covered by the set.
func (s *IntervalSet) contains(start, end int64) bool {
	if end <= start {
		return true
	}
	for _, v := range s.iv {
		if v.Start > start {
			return false
		}
		if v.End >= end {
			return true
		}
		if v.End > start {
			start = v.End
		}
	}
	return end <= start
}

// total is the number of covered bytes (checkpoint event payload).
func (s *IntervalSet) total() int64 {
	var n int64
	for _, v := range s.iv {
		n += v.End - v.Start
	}
	return n
}

// clone deep-copies the set (checkpoint snapshots must not alias the live
// set while it keeps growing between fsync and persist).
func (s *IntervalSet) clone() *IntervalSet {
	out := &IntervalSet{size: s.size}
	out.iv = append(out.iv, s.iv...)
	return out
}
