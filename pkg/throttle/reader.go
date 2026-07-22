package throttle

import (
	"context"
	"io"
	"net/http"
)

// Paced read installment bounds. Pacing at the HTTP transport counts wire
// bytes; small read installments keep the token debits fine-grained so the
// achieved rate tracks the configured ceiling instead of oscillating in
// multi-MiB bursts. The cap applies only while the bucket is limited —
// unlimited buckets pass reads through at the caller's size to keep copy
// overhead down.
//
// The chunk scales with the configured rate (~1/16s of the global budget,
// clamped) so one paced wait stays far below the stall window even when many
// connections share a small budget: a read parked in Bucket.Wait delivers
// zero bytes to the stall monitor, and a wait longer than the window would
// make the ungated idle-read layer kill a healthy, merely-limited connection.
const (
	pacedChunkMax = 64 << 10
	pacedChunkMin = 4 << 10
	// pacedChunkRateDiv divides the bucket rate into per-read installments:
	// rate/16 ≈ 1/16s of budget per read, so even 16 connections queueing
	// back-to-back waits stay around one second per turn.
	pacedChunkRateDiv = 16
)

// pacedChunkFor sizes one read installment for a limited bucket rate.
func pacedChunkFor(rate int64) int {
	c := rate / pacedChunkRateDiv
	if c > pacedChunkMax {
		return pacedChunkMax
	}
	if c < pacedChunkMin {
		return pacedChunkMin
	}
	return int(c)
}

// PacedTransport wraps rt so every response body is read through bucket b:
// each read is debited from b before the caller sees the bytes, blocking
// until the tokens accrue. This paces at the wire layer — compressed/raw
// transport bytes — so every consumer of the transport (block downloads,
// xet xorb fetches, the single-stream fallback) is limited by the same
// bucket without cooperating. Cache hits and other non-network reads are
// never debited because they never cross this transport.
func PacedTransport(rt http.RoundTripper, b *Bucket) http.RoundTripper {
	return &pacedTransport{rt: rt, b: b}
}

type pacedTransport struct {
	rt http.RoundTripper
	b  *Bucket
}

func (t *pacedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.rt.RoundTrip(req)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}
	// The request ctx gates Wait: a cancelled attempt (stall kill, drain,
	// shutdown) must not park in the bucket queue.
	resp.Body = &pacedBody{body: resp.Body, b: t.b, ctx: req.Context()}
	return resp, err
}

// pacedBody debits the bucket after each read, with reads capped to a
// rate-scaled chunk while limited. Post-read debit keeps the accounting
// exact (the bucket never needs refunds for short reads); the chunk cap
// bounds the per-connection overshoot to one chunk.
type pacedBody struct {
	body io.ReadCloser
	b    *Bucket
	ctx  context.Context
}

func (r *pacedBody) Read(p []byte) (int, error) {
	if rate := r.b.rate.Load(); rate > 0 {
		if c := pacedChunkFor(rate); len(p) > c {
			p = p[:c]
		}
	}
	n, err := r.body.Read(p)
	if n > 0 {
		if werr := r.b.Wait(r.ctx, int64(n)); werr != nil {
			// The bytes were delivered to the caller; the debit is already
			// recorded up to the cancellation point. Surface the ctx error so
			// the attempt aborts instead of streaming on unpaced.
			return n, werr
		}
	}
	return n, err
}

func (r *pacedBody) Close() error { return r.body.Close() }
