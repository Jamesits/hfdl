package xet

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ErrNotPrepared is returned by Open/Boundaries when Prepare has not run
// (sched calls Prepare before handing the Source to transfer).
var ErrNotPrepared = errors.New("xet: source not prepared")

// ErrRangeNotSatisfiable maps a CAS 416 on a ranged reconstruction query:
// the requested file range lies beyond end of file. hf_xet maps the same
// status to Ok(None) (remote_client.rs get_reconstruction_impl).
var ErrRangeNotSatisfiable = errors.New("xet: range not satisfiable")

// AuthError is a persistent CAS 401: the token was already refreshed once
// and the retry was rejected again.
type AuthError struct {
	Msg string
}

func (e *AuthError) Error() string {
	return "xet: CAS authentication failed: " + e.Msg
}

// LengthMismatchError reports a term whose decoded output length differs
// from the reconstruction's unpacked_length. Per xet-core
// (cas_types/mod.rs XorbReconstructionTerm) unpacked_length exists exactly
// for this validation, so a mismatch means corruption or a CAS bug.
type LengthMismatchError struct {
	Xorb                 string
	ChunkStart, ChunkEnd uint32
	Want, Got            int64
}

func (e *LengthMismatchError) Error() string {
	return fmt.Sprintf("xet: term %s chunks [%d,%d): decoded %d bytes, unpacked_length says %d",
		e.Xorb, e.ChunkStart, e.ChunkEnd, e.Got, e.Want)
}

// DataError is malformed xorb data: bad chunk framing, unknown compression
// scheme, decompressor failure, truncated/oversized signed-range body, or a
// reconstruction whose fetch info cannot cover a term.
type DataError struct {
	Xorb   string // may be "" when unrelated to a specific xorb
	Reason string
	Err    error
}

func (e *DataError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("xet: data error (xorb %s): %s: %v", e.Xorb, e.Reason, e.Err)
	}
	return fmt.Sprintf("xet: data error (xorb %s): %s", e.Xorb, e.Reason)
}

func (e *DataError) Unwrap() error { return e.Err }

// ReacquireError reports that the presigned-URL reacquire budget for a file
// was exhausted: every freshly-issued signed URL still answers 403.
type ReacquireError struct {
	Attempts int
}

func (e *ReacquireError) Error() string {
	return fmt.Sprintf("xet: presigned URL reacquire exhausted after %d attempt(s)", e.Attempts)
}

// parseRetryAfter interprets a Retry-After header (delta-seconds or
// HTTP-date); 0 when absent/unparseable. Mirrors hfapi's parser; duplicated
// because hfapi's is unexported and the xet package must map CAS 429s onto
// hfapi.RateLimitError for sched's cooldown handling.
func parseRetryAfter(h string) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	if n, err := strconv.Atoi(h); err == nil {
		if n < 0 {
			return 0
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		return max(time.Until(t), 0)
	}
	return 0
}
