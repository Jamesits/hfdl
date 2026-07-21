// Package hfapi is a thin client for the Hugging Face Hub HTTP API:
// {endpoint}/api/... metadata endpoints plus resolve URLs.
//
// The client is deliberately stateless with respect to queues and SQLite:
// rate-limit hits are reported as typed errors (*RateLimitError) and the
// caller (sched) owns cooldown policy. Revision pins rev -> commit_sha once
// per repo so a moving branch cannot mix file versions mid-run.
//
// Auth: the bearer token is attached to Hub-host requests only and stripped
// on cross-host (CDN/object-storage) redirects. This is implemented as a
// RoundTripper wrapping the caller-supplied http.Client's transport, so
// every redirect hop re-enters RoundTrip and re-evaluates the destination
// host (net/http's own header forwarding on redirects is not trusted).
//
// Timeouts are RESPONSE timeouts (context deadlines around the whole
// metadata exchange), not connect timeouts: etagTimeout bounds metadata
// HEAD/listing calls, downloadTimeout is only exposed via DownloadTimeout()
// for the transfer package's download GETs.
//
// Tracing is optional: SetTracer installs a tracer that wraps each public
// API method in a span named "hfapi.<Op>"; nil (the default) is a no-op.
package hfapi
