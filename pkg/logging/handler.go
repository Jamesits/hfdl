package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// attrEntry pins an attr to the group path that was current when WithAttrs
// added it, so WithGroup calls afterwards do not retroactively nest it.
type attrEntry struct {
	groups []string
	attr   slog.Attr
}

// core is the fan-out state shared by a Handler and all its WithAttrs /
// WithGroup children: one ring, one level floor, one set of sinks. Children
// only differ in their attr/group prefix.
type core struct {
	level slog.Leveler
	text  slog.Handler // nil when stderr is silenced
	ring  *Ring
	drops atomic.Int64

	mu      sync.Mutex // guards everything below
	extras  []*extraSink
	dbQueue chan LogRow
	dbStop  chan struct{}
	dbDone  chan struct{}
	// dbSince is the ring Seq horizon captured under c.mu when AttachDB took
	// its replay snapshot: Handle enqueues a record only when its Seq exceeds
	// this, so a record already in the replay is never also queued (no dup).
	dbSince uint64
	// dbClosed marks the queue permanently retired (sink loop drained on
	// ctx-cancel or Close). Once set, a record that would have gone to the DB
	// is a counted drop instead of silently vanishing into an orphaned queue.
	dbClosed bool
	closed   bool
}

// Handler is the fan-out root slog.Handler. The level floor is applied here
// so every sink (stderr text, ring, DB, extra handlers) sees the same
// records. Safe for concurrent use; Handle never blocks on sink IO.
type Handler struct {
	core   *core
	attrs  []attrEntry
	groups []string
	prefix []slog.Attr // attrs already nested into their group paths
}

// NewHandler builds the fan-out root. stderr == nil silences text output
// entirely (TUI mode); ring == nil creates a default-capacity ring.
func NewHandler(level slog.Leveler, stderr io.Writer, ring *Ring) *Handler {
	if level == nil {
		level = slog.LevelInfo
	}
	if ring == nil {
		ring = NewRing(0)
	}
	c := &core{level: level, ring: ring}
	if stderr != nil {
		c.text = slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level})
	}
	return &Handler{core: c}
}

// Enabled applies the root level floor; all sinks agree on it.
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.core.level.Level()
}

// Handle fans r out to the ring, stderr text, extra slog handlers and the DB
// queue. Sink errors are swallowed by design (best-effort logging); the DB
// leg is a non-blocking channel send.
func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	if !h.Enabled(ctx, r.Level) {
		return nil
	}
	c := h.core

	attrs, source := h.attrsJSON(r)
	rec := Record{Time: r.Time, Level: r.Level, Source: source, Msg: r.Message, Attrs: attrs}
	seq := c.ring.Write(rec)

	// The DB send is done under c.mu (a non-blocking channel op that never
	// blocks the lock) so it is mutually exclusive with AttachDB publishing
	// the queue and with Close/ctx-cancel retiring it: a record either lands
	// in the queue the sink loop still drains, or is a counted drop — never
	// lost into an orphaned queue, and never duplicated into the replay.
	c.mu.Lock()
	extras := append([]*extraSink(nil), c.extras...)
	switch {
	case c.dbQueue != nil && seq > c.dbSince:
		row := LogRow{Time: rec.Time, Level: int(rec.Level), Source: rec.Source, Msg: rec.Msg, Attrs: rec.Attrs}
		select {
		case c.dbQueue <- row:
		default:
			// Sink slower than the log rate: drop rather than block a
			// worker on log IO.
			c.drops.Add(1)
		}
	case c.dbQueue == nil && c.dbClosed:
		// A sink was attached but has been retired: count what it can no
		// longer accept instead of dropping it uncounted.
		c.drops.Add(1)
	}
	c.mu.Unlock()

	if c.text != nil || len(extras) > 0 {
		nr := h.slogRecord(r)
		if c.text != nil {
			_ = c.text.Handle(ctx, nr)
		}
		for _, x := range extras {
			x.replay(ctx, c.ring)
			// Only records newer than the extra's replay horizon are handled
			// live; records at/below it were already delivered by the replay,
			// so this avoids the attach-vs-Handle duplication.
			if seq > x.upTo && x.h.Enabled(ctx, nr.Level) {
				_ = x.h.Handle(ctx, nr)
			}
		}
	}
	return nil
}

// WithAttrs returns a child sharing the ring and every sink.
func (h *Handler) WithAttrs(as []slog.Attr) slog.Handler {
	nh := *h
	nh.attrs = append(append([]attrEntry(nil), h.attrs...), attrsAt(h.groups, as)...)
	nh.prefix = append(append([]slog.Attr(nil), h.prefix...), nest(h.groups, as)...)
	return &nh
}

// WithGroup returns a child sharing the ring and every sink; empty names are
// ignored per slog convention.
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	nh := *h
	nh.groups = append(append([]string(nil), h.groups...), name)
	return &nh
}

func attrsAt(groups []string, as []slog.Attr) []attrEntry {
	out := make([]attrEntry, len(as))
	for i, a := range as {
		out[i] = attrEntry{groups: groups, attr: a}
	}
	return out
}

// nest wraps attrs in nested group values, outermost group first.
func nest(groups []string, as []slog.Attr) []slog.Attr {
	for _, group := range slices.Backward(groups) {
		as = []slog.Attr{{Key: group, Value: slog.GroupValue(as...)}}
	}
	return as
}

// slogRecord rebuilds r with the handler's attr prefix (group-nested)
// prepended and the record's own attrs nested under the current group path,
// matching stdlib handler semantics for the fan-out targets.
func (h *Handler) slogRecord(r slog.Record) slog.Record {
	if len(h.prefix) == 0 && len(h.groups) == 0 {
		return r
	}
	nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	nr.AddAttrs(h.prefix...)
	var recAttrs []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		recAttrs = append(recAttrs, a)
		return true
	})
	nr.AddAttrs(nest(h.groups, recAttrs)...)
	return nr
}

// attrsJSON renders handler + record attrs as a compact JSON object for
// Record.Attrs, lifting a top-level SourceKey string attr out into the
// dedicated Source column. Returns ("", "") when there is nothing to render.
func (h *Handler) attrsJSON(r slog.Record) (jsonAttrs, source string) {
	root := &attrObj{}
	has := false
	put := func(groups []string, a slog.Attr) {
		a.Value = a.Value.Resolve()
		if a.Equal(slog.Attr{}) {
			return
		}
		if len(groups) == 0 && a.Key == SourceKey && a.Value.Kind() == slog.KindString {
			source = a.Value.String()
			return
		}
		root.insert(groups, a)
		has = true
	}
	for _, e := range h.attrs {
		put(e.groups, e.attr)
	}
	r.Attrs(func(a slog.Attr) bool {
		put(h.groups, a)
		return true
	})
	if !has {
		return "", source
	}
	var buf bytes.Buffer
	root.marshal(&buf)
	return buf.String(), source
}

// attrObj is an insertion-ordered JSON object used to merge group-nested
// attrs before marshaling; later duplicate keys overwrite in place.
type attrObj struct {
	keys []string
	vals map[string]any // slog.Value or *attrObj
}

func (o *attrObj) insert(groups []string, a slog.Attr) {
	if len(groups) > 0 {
		child, _ := o.child(groups[0])
		child.insert(groups[1:], a)
		return
	}
	if o.vals == nil {
		o.vals = map[string]any{}
	}
	if _, ok := o.vals[a.Key]; !ok {
		o.keys = append(o.keys, a.Key)
	}
	o.vals[a.Key] = a.Value
}

func (o *attrObj) child(key string) (*attrObj, bool) {
	if o.vals == nil {
		o.vals = map[string]any{}
	}
	if v, ok := o.vals[key]; ok {
		if c, ok := v.(*attrObj); ok {
			return c, true
		}
		// A leaf already occupies key: the group overwrites it.
	}
	c := &attrObj{}
	if _, ok := o.vals[key]; !ok {
		o.keys = append(o.keys, key)
	}
	o.vals[key] = c
	return c, false
}

func (o *attrObj) marshal(buf *bytes.Buffer) {
	buf.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		buf.Write(kb)
		buf.WriteByte(':')
		switch v := o.vals[k].(type) {
		case *attrObj:
			v.marshal(buf)
		case slog.Value:
			appendValueJSON(buf, v)
		}
	}
	buf.WriteByte('}')
}

// appendValueJSON writes v in the same shape slog.JSONHandler would produce:
// durations as nanoseconds, times as RFC3339Nano strings, groups as objects.
func appendValueJSON(buf *bytes.Buffer, v slog.Value) {
	v = v.Resolve()
	switch v.Kind() {
	case slog.KindString:
		b, _ := json.Marshal(v.String())
		buf.Write(b)
	case slog.KindBool:
		buf.Write(strconv.AppendBool(nil, v.Bool()))
	case slog.KindInt64:
		buf.Write(strconv.AppendInt(nil, v.Int64(), 10))
	case slog.KindUint64:
		buf.Write(strconv.AppendUint(nil, v.Uint64(), 10))
	case slog.KindFloat64:
		f := v.Float64()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			b, _ := json.Marshal(strconv.FormatFloat(f, 'g', -1, 64))
			buf.Write(b)
			return
		}
		buf.Write(strconv.AppendFloat(nil, f, 'g', -1, 64))
	case slog.KindDuration:
		buf.Write(strconv.AppendInt(nil, int64(v.Duration()), 10))
	case slog.KindTime:
		b, _ := json.Marshal(v.Time().Format(time.RFC3339Nano))
		buf.Write(b)
	case slog.KindGroup:
		obj := &attrObj{}
		for _, a := range v.Group() {
			a.Value = a.Value.Resolve()
			if !a.Equal(slog.Attr{}) {
				obj.insert(nil, a)
			}
		}
		obj.marshal(buf)
	default: // KindAny
		a := v.Any()
		if err, ok := a.(error); ok {
			b, _ := json.Marshal(err.Error())
			buf.Write(b)
			return
		}
		if b, err := json.Marshal(a); err == nil {
			buf.Write(b)
		} else {
			b, _ := json.Marshal(fmt.Sprintf("%v", a))
			buf.Write(b)
		}
	}
}
