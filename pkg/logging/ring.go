package logging

import (
	"log/slog"
	"sync"
	"time"
)

// defaultRingCap keeps the last 2k records when NewRing gets a
// non-positive capacity.
const defaultRingCap = 2048

// Record is one ring entry. Attrs is a compact JSON object ("" when the
// record carried no attributes); Seq is assigned by Write and increases
// monotonically per Ring, starting at 1.
type Record struct {
	Seq    uint64
	Time   time.Time
	Level  slog.Level
	Source string
	Msg    string
	Attrs  string
}

// Ring is a bounded last-N log buffer. It is the TUI logs view's only data
// source (no DB polling on the hot path) and the replay source for sinks
// attached after bootstrap. Safe for concurrent use.
type Ring struct {
	mu    sync.RWMutex
	buf   []Record
	start int // index of oldest record
	len   int
	seq   uint64
}

// NewRing returns a Ring holding the last cap records; cap <= 0 selects the
// default 2k capacity.
func NewRing(cap int) *Ring {
	if cap <= 0 {
		cap = defaultRingCap
	}
	return &Ring{buf: make([]Record, cap)}
}

// Write appends rec, assigning the next Seq, evicting the oldest record when
// full.
func (r *Ring) Write(rec Record) {
	r.mu.Lock()
	r.seq++
	rec.Seq = r.seq
	if r.len < len(r.buf) {
		r.buf[(r.start+r.len)%len(r.buf)] = rec
		r.len++
	} else {
		r.buf[r.start] = rec
		r.start = (r.start + 1) % len(r.buf)
	}
	r.mu.Unlock()
}

// All returns every retained record, oldest first.
func (r *Ring) All() []Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.collect(0)
}

// Since returns retained records with Seq > after, oldest first — the
// seq-based poll the TUI uses for follow mode. A caller whose cursor fell
// behind the eviction horizon sees the gap as a Seq jump in the first
// returned record.
func (r *Ring) Since(after uint64) []Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.collect(after)
}

func (r *Ring) collect(after uint64) []Record {
	out := make([]Record, 0, r.len)
	for i := 0; i < r.len; i++ {
		rec := r.buf[(r.start+i)%len(r.buf)]
		if rec.Seq > after {
			out = append(out, rec)
		}
	}
	return out
}

// Count reports how many retained records are at min level or above (the
// TUI's WARN+ badge).
func (r *Ring) Count(min slog.Level) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for i := 0; i < r.len; i++ {
		if r.buf[(r.start+i)%len(r.buf)].Level >= min {
			n++
		}
	}
	return n
}

// lastSeq reports the most recently assigned Seq (0 when empty); AttachSlog
// uses it to bound the replay so records logged after the attach are not
// delivered twice.
func (r *Ring) lastSeq() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.seq
}
