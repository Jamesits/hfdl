// Package hfapi is a thin client for the Hugging Face Hub HTTP API:
// {endpoint}/api/... metadata endpoints plus resolve URLs.
//
// The client is deliberately stateless with respect to queues and SQLite:
// rate-limit hits are reported as typed errors (*RateLimitError) and the
// caller (sched) owns cooldown policy. Revision pins rev -> commit_sha once
// per repo so a moving branch cannot mix file versions mid-run.
//
// Auth: the bearer token is attached to Hub-host requests only and stripped
// on cross-host (CDN/object-storage) redirects. This is the shared
// config.NewAuthTransport RoundTripper wrapping the caller-supplied
// http.Client's transport, so every redirect hop re-enters RoundTrip and
// re-evaluates the destination host (net/http's own header forwarding on
// redirects is not trusted). transfer wraps the same primitive for its payload
// downloads, so the security-sensitive strip decision lives in one place.
//
// Timeouts are RESPONSE timeouts (context deadlines around the whole
// metadata exchange), not connect timeouts: etagTimeout bounds metadata
// HEAD/listing calls (the download response timeout lives on the transfer
// package, sourced from config.DownloadTimeout).
//
// Tracing is optional: SetTracer installs a tracer that wraps each public
// API method in a span named "hfapi.<Op>"; nil (the default) is a no-op.
package hfapi
