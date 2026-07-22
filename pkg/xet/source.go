package xet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/jamesits/hfdl/pkg/hfapi"
	"github.com/jamesits/hfdl/pkg/logging"
	"github.com/jamesits/hfdl/pkg/transfer"
)

// maxReacquires bounds per-file reconstruction reacquires: a presigned-URL
// 403/expiry triggers a reacquire of the reconstruction, capped by this
// budget. The budget is shared across all of a file's concurrent Opens,
// matching hf_xet's single-flight refresh
// (file_reconstruction/.../retrieval_urls.rs).
const maxReacquires = 2

// reacquireBaseBackoff seeds the exponential backoff before each reacquire.
const reacquireBaseBackoff = 200 * time.Millisecond

// Source is a transfer.BlockSource for one xet-backed file. Prepare must
// complete before transfer uses it; Open is safe for concurrent blocks.
type Source struct {
	client *Client
	log    *slog.Logger
	fileID string // xet file id (keyed-BLAKE3 hex) — never a verify target
	size   int64
	route  string // token refresh route (from hfapi XetFileData)

	mu    sync.Mutex
	recon *reconstruction
	// gen is bumped every time recon is (re)fetched. A caller that 403'd on
	// generation g asks reacquire to refresh only if s.gen is still g; if a
	// concurrent refresher already advanced it, the caller retries against the
	// fresh URL without spending its own reacquire budget — true single-flight.
	gen        int
	reacquires int
	// fetchWait guards an in-flight reconstruction fetch (Prepare or
	// reacquire): it is non-nil while a fetch runs and is closed on
	// completion, so concurrent callers coalesce onto the one fetch instead of
	// each issuing their own.
	fetchWait chan struct{}
}

// Prepare fetches the file's full reconstruction (v2 with /v1 fallback).
// Idempotent and concurrently coalesced: a prepared Source is not re-fetched,
// and two concurrent first Prepares issue a single fetch (one leads, the rest
// wait on fetchWait). Reacquires during Open go through reacquire().
func (s *Source) Prepare(ctx context.Context) error {
	s.mu.Lock()
	for {
		if s.recon != nil {
			s.mu.Unlock()
			return nil
		}
		if s.fetchWait != nil {
			ch := s.fetchWait
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ch:
			}
			s.mu.Lock()
			continue
		}
		break
	}
	ch := make(chan struct{})
	s.fetchWait = ch
	s.mu.Unlock()

	recon, err := s.client.fetchReconstruction(ctx, s.route, s.fileID, nil)

	s.mu.Lock()
	s.fetchWait = nil
	close(ch)
	// Don't clobber a reconstruction a concurrent reacquire may have published
	// while this leader was fetching.
	if err == nil && s.recon == nil {
		s.recon = recon
		s.gen++
	}
	s.mu.Unlock()
	return err
}

func (s *Source) current() *reconstruction {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recon
}

// reconAndGen returns the current reconstruction together with its generation,
// so a fetch racing a reacquire can bind the URL it uses to a generation.
func (s *Source) reconAndGen() (*reconstruction, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recon, s.gen
}

// Boundaries expands both edges to enclosing term boundaries: fetched xorb
// bytes are not file bytes, so a whole term is decoded to yield any of its
// bytes and a block should own whole terms. The LEADING edge of each missing
// interval is snapped down to the enclosing term's fileStart (so a block
// starts on a term boundary — the head bytes it pulls in are decoded from that same term
// regardless), clamped to the previous expanded interval's end. The trailing
// edge is snapped up to its term end and clamped to the file size. The
// interior/forward edges are split at the next term file-end and at the
// blockSize grid, so no returned interval crosses a term boundary and none
// exceeds blockSize. transfer chunks these at blockSize afterwards; for
// snapped xet intervals that chunking is a no-op. Without a reconstruction the
// intervals pass through unchanged (identity, like httpSource).
func (s *Source) Boundaries(missing []transfer.Interval, blockSize int64) []transfer.Interval {
	recon := s.current()
	if recon == nil || len(recon.terms) == 0 {
		return missing
	}
	var out []transfer.Interval
	var prevEnd int64 // end of the previous expanded interval; leading-edge snap floor
	for _, m := range missing {
		start := max(recon.termStartAt(m.Start), prevEnd)
		end := min(recon.termEndAt(m.End), s.size)
		for p := start; p < end; {
			edge := end
			if te := recon.nextTermEdge(p); te > p && te < edge {
				edge = te
			}
			if blockSize > 0 {
				if ge := (p/blockSize + 1) * blockSize; ge > p && ge < edge {
					edge = ge
				}
			}
			out = append(out, transfer.Interval{Start: p, End: edge})
			p = edge
		}
		prevEnd = end
	}
	return out
}

func (r *reconstruction) termEndAt(pos int64) int64 {
	if pos <= 0 {
		return pos
	}
	i := sort.Search(len(r.terms), func(i int) bool { return r.terms[i].fileEnd >= pos })
	if i >= len(r.terms) || r.terms[i].fileStart >= pos {
		return pos
	}
	return r.terms[i].fileEnd
}

// nextTermEdge returns the smallest term file-end strictly greater than pos
// (or a term start if pos sits in a gap — defensive; a valid reconstruction
// tiles the file contiguously).
func (r *reconstruction) nextTermEdge(pos int64) int64 {
	i := sort.Search(len(r.terms), func(i int) bool { return r.terms[i].fileEnd > pos })
	if i >= len(r.terms) {
		return -1
	}
	if r.terms[i].fileStart > pos {
		return r.terms[i].fileStart
	}
	return r.terms[i].fileEnd
}

// termStartAt returns the fileStart of the term covering pos, or pos itself
// when pos precedes all terms or falls in a gap (defensive; a valid
// reconstruction tiles the file contiguously).
func (r *reconstruction) termStartAt(pos int64) int64 {
	i := sort.Search(len(r.terms), func(i int) bool { return r.terms[i].fileEnd > pos })
	if i >= len(r.terms) || r.terms[i].fileStart > pos {
		return pos
	}
	return r.terms[i].fileStart
}

// Open streams decoded file bytes of [off, off+length). A zero length returns
// an immediately-EOF reader without starting a producer. Other readers are
// backed by a producer goroutine; Close (or ctx cancel) unblocks it.
func (s *Source) Open(ctx context.Context, off, length int64) (io.ReadCloser, error) {
	recon := s.current()
	if recon == nil {
		return nil, ErrNotPrepared
	}
	// Overflow-safe bounds check: off+length can overflow int64 (e.g. a huge
	// length), so never form the sum in the comparison — check each side
	// against size instead.
	if off < 0 || length < 0 || off > s.size || length > s.size-off {
		return nil, fmt.Errorf("xet: open range off=%d length=%d outside file size %d: %w", off, length, s.size, ErrRangeNotSatisfiable)
	}
	if length == 0 {
		return io.NopCloser(&emptyReader{}), nil
	}
	pr, pw := io.Pipe()
	go s.stream(ctx, off, off+length, pw)
	// io.Pipe writes don't observe ctx; closing the reader on cancel
	// unblocks the producer with ErrClosedPipe.
	stop := context.AfterFunc(ctx, func() { _ = pr.CloseWithError(ctx.Err()) })
	return &reader{ReadCloser: pr, stop: stop}, nil
}

type emptyReader struct{}

func (*emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

type reader struct {
	io.ReadCloser
	stop func() bool
}

func (r *reader) Close() error {
	r.stop()
	return r.ReadCloser.Close()
}

// stream walks the terms covering [off, end), writing decoded bytes in
// order. Any error terminates the stream via CloseWithError; transfer
// requeues the block.
func (s *Source) stream(ctx context.Context, off, end int64, pw *io.PipeWriter) {
	err := s.streamTerms(ctx, off, end, pw)
	_ = pw.CloseWithError(err)
}

func (s *Source) streamTerms(ctx context.Context, off, end int64, pw *io.PipeWriter) error {
	if off >= end {
		return nil
	}
	for pos := off; pos < end; {
		recon := s.current()
		i := sort.Search(len(recon.terms), func(i int) bool { return recon.terms[i].fileEnd > pos })
		if i >= len(recon.terms) {
			return &DataError{Reason: fmt.Sprintf("no term covers file offset %d", pos)}
		}
		t := &recon.terms[i]
		if t.fileStart > pos {
			return &DataError{Reason: fmt.Sprintf("term gap at file offset %d", pos)}
		}
		// Decode the term's whole chunk range — chunk contents are only
		// addressable by index, and signed ranges authorize whole chunk
		// ranges, so partial-chunk-range fetching is impossible anyway.
		// The chunk cache absorbs repeats when blockSize splits a term.
		decoded, err := s.decodeTerm(ctx, t)
		if err != nil {
			return err
		}
		lo := max(pos, t.fileStart) - t.fileStart
		hi := min(end, t.fileEnd) - t.fileStart
		if _, err := pw.Write(decoded[lo:hi]); err != nil {
			return err // reader gone or ctx cancelled
		}
		pos = t.fileEnd
	}
	return nil
}

// decodeTerm fetches and decodes every chunk of t, validating the result
// against unpacked_length (LengthMismatchError on corruption).
func (s *Source) decodeTerm(ctx context.Context, t *reconTerm) ([]byte, error) {
	recon := s.current()
	entries := coverRanges(recon.fetch[t.xorb], t.chunkStart, t.chunkEnd)
	if entries == nil {
		return nil, &DataError{
			Xorb:   t.xorb,
			Reason: fmt.Sprintf("fetch info cannot cover term chunk range [%d,%d)", t.chunkStart, t.chunkEnd),
		}
	}
	var decoded []byte
	for _, e := range entries {
		raw, err := s.fetchSerialized(ctx, t.xorb, e)
		if err != nil {
			return nil, err
		}
		chunks, err := decodeXorbChunks(t.xorb, raw)
		if err != nil {
			return nil, err
		}
		if len(chunks) != int(e.chunkEnd-e.chunkStart) {
			return nil, &DataError{
				Xorb:   t.xorb,
				Reason: fmt.Sprintf("serialized range holds %d chunks, authorized range [%d,%d) implies %d", len(chunks), e.chunkStart, e.chunkEnd, e.chunkEnd-e.chunkStart),
			}
		}
		// An entry may span more chunks than this term needs; select the
		// term's share.
		lo := int(max(t.chunkStart, e.chunkStart) - e.chunkStart)
		hi := int(min(t.chunkEnd, e.chunkEnd) - e.chunkStart)
		for _, ch := range chunks[lo:hi] {
			decoded = append(decoded, ch...)
		}
	}
	if int64(len(decoded)) != t.unpackedLength {
		return nil, &LengthMismatchError{
			Xorb:       t.xorb,
			ChunkStart: t.chunkStart,
			ChunkEnd:   t.chunkEnd,
			Want:       t.unpackedLength,
			Got:        int64(len(decoded)),
		}
	}
	return decoded, nil
}

// coverRanges tiles [start,end) with entries from fetch info, or nil when a
// gap exists. Entries are sorted by chunkStart (normalize).
func coverRanges(entries []fetchRange, start, end uint32) []fetchRange {
	var out []fetchRange
	cur := start
	for cur < end {
		advanced := false
		for _, e := range entries {
			if e.chunkStart <= cur && cur < e.chunkEnd {
				out = append(out, e)
				cur = e.chunkEnd
				advanced = true
				break
			}
		}
		if !advanced {
			return nil
		}
	}
	return out
}

// fetchSerialized returns the authorized serialized range, from the chunk
// cache when present, else from the signed URL (cached afterwards).
func (s *Source) fetchSerialized(ctx context.Context, xorb string, e fetchRange) ([]byte, error) {
	key := cacheKey{xorb: xorb, start: e.chunkStart, end: e.chunkEnd}
	if cc := s.client.cache; cc != nil {
		if b, ok := cc.Get(key); ok {
			return b, nil
		}
	}
	b, err := s.fetchRangeHTTP(ctx, xorb, e)
	if err != nil {
		return nil, err
	}
	if cc := s.client.cache; cc != nil {
		cc.Put(key, b)
	}
	return b, nil
}

// fetchRangeHTTP GETs the signed range EXACTLY as authorized (Range:
// bytes=start-end, inclusive end; no Authorization header — presigned URLs
// are fetched with a plain client, remote_client.rs get_file_term_data).
// A 403 (expired signature) triggers a bounded reconstruction reacquire,
// after which the equivalent entry (same xorb + chunk range) is re-fetched
// under its fresh URL.
func (s *Source) fetchRangeHTTP(ctx context.Context, xorb string, e fetchRange) ([]byte, error) {
	_, gen := s.reconAndGen()
	for {
		b, forbidden, err := s.client.getSignedRange(ctx, e)
		if !forbidden {
			return b, err
		}
		s.log.Info("presigned URL rejected, reacquiring reconstruction", "xorb", xorb, "range", fmt.Sprintf("%d-%d", e.chunkStart, e.chunkEnd))
		if rerr := s.reacquire(ctx, gen); rerr != nil {
			return nil, rerr
		}
		var recon *reconstruction
		recon, gen = s.reconAndGen()
		e2, ok := locateEntry(recon, xorb, e.chunkStart, e.chunkEnd)
		if !ok {
			return nil, &DataError{
				Xorb:   xorb,
				Reason: fmt.Sprintf("reacquired reconstruction lost chunk range [%d,%d)", e.chunkStart, e.chunkEnd),
			}
		}
		e = e2
	}
}

// locateEntry finds the fetch entry for (xorb, chunkStart, chunkEnd) in recon.
func locateEntry(recon *reconstruction, xorb string, start, end uint32) (fetchRange, bool) {
	for _, e := range recon.fetch[xorb] {
		if e.chunkStart == start && e.chunkEnd == end {
			return e, true
		}
	}
	return fetchRange{}, false
}

// reacquire refreshes the file's reconstruction under a shared, bounded budget
// with backoff. failedGen is the generation the caller's 403'd URL came from:
// if the reconstruction has already advanced past it (a concurrent 403 already
// triggered a refresh), reacquire returns nil without spending budget so the
// caller simply retries the fresh URL. Otherwise this call either leads the
// single-flight refresh or coalesces onto one already in flight — so a burst
// of concurrent 403s costs at most one budget unit, not one per caller.
func (s *Source) reacquire(ctx context.Context, failedGen int) error {
	s.mu.Lock()
	for {
		if s.gen > failedGen {
			s.mu.Unlock()
			return nil
		}
		if s.fetchWait != nil {
			ch := s.fetchWait
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ch:
			}
			s.mu.Lock()
			continue
		}
		break
	}
	if s.reacquires >= maxReacquires {
		s.mu.Unlock()
		return &ReacquireError{Attempts: s.reacquires}
	}
	n := s.reacquires
	s.reacquires++
	ch := make(chan struct{})
	s.fetchWait = ch
	s.mu.Unlock()

	// Backoff and fetch run OUTSIDE the lock so coalesced waiters and other
	// blocks' Opens are not serialized behind the sleep.
	recon, err := s.backoffFetch(ctx, n)

	s.mu.Lock()
	s.fetchWait = nil
	close(ch)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.recon = recon
	s.gen++
	s.log.Debug("reconstruction reacquired", "file", s.fileID, "attempt", n+1,
		"terms", len(recon.terms), "ranges", logging.JSONValue(termRanges(recon)))
	s.mu.Unlock()
	return nil
}

// backoffFetch sleeps the nth exponential backoff then fetches a fresh
// reconstruction; ctx cancellation aborts the wait.
func (s *Source) backoffFetch(ctx context.Context, n int) (*reconstruction, error) {
	timer := time.NewTimer(reacquireBaseBackoff << n)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	return s.client.fetchReconstruction(ctx, s.route, s.fileID, nil)
}

// getSignedRange performs one presigned range GET. forbidden=true flags a
// 403 (caller reacquires). 429 is mapped to *hfapi.RateLimitError like CAS
// 429s — sched owns the cooldown either way.
func (c *Client) getSignedRange(ctx context.Context, e fetchRange) (b []byte, forbidden bool, err error) {
	// Validate the authorized range before building the request: a
	// non-negative, non-empty byte span mapped to a non-empty chunk range.
	// A malformed entry (from a buggy/corrupt reconstruction) would otherwise
	// produce a nonsense Range header and an opaque server error.
	if e.byteStart < 0 || e.byteEnd < e.byteStart || e.chunkStart >= e.chunkEnd {
		return nil, false, &DataError{
			Reason: fmt.Sprintf("invalid signed range: bytes [%d,%d] chunks [%d,%d)", e.byteStart, e.byteEnd, e.chunkStart, e.chunkEnd),
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.url, nil)
	if err != nil {
		return nil, false, fmt.Errorf("xet: build signed range request: %w", sanitizeSignedURLError(err, e.url))
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", e.byteStart, e.byteEnd))
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("xet: signed range GET: %w", sanitizeSignedURLError(err, e.url))
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
	case http.StatusForbidden:
		return nil, true, nil
	case http.StatusTooManyRequests:
		return nil, false, &hfapi.RateLimitError{RetryAfter: hfapi.ParseRetryAfter(resp.Header.Get("Retry-After"))}
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return nil, false, fmt.Errorf("xet: signed range GET: unexpected status %s: %s", resp.Status, hfapi.SanitizeErrorText(string(body)))
	}
	want := e.byteEnd - e.byteStart + 1
	b, err = io.ReadAll(io.LimitReader(resp.Body, want+1))
	if err != nil {
		return nil, false, fmt.Errorf("xet: read signed range body: %w", err)
	}
	if int64(len(b)) != want {
		return nil, false, &DataError{
			Reason: fmt.Sprintf("signed range returned %d bytes, Range authorized %d", len(b), want),
		}
	}
	return b, false, nil
}

func sanitizeSignedURLError(err error, rawURL string) error {
	cause := err
	if uerr, ok := errors.AsType[*url.Error](err); ok {
		cause = uerr.Err
	}
	u, parseErr := url.Parse(rawURL)
	if parseErr != nil {
		return cause
	}
	u.User, u.RawQuery, u.Fragment = nil, "", ""
	return fmt.Errorf("request to %s: %w", u.String(), cause)
}
