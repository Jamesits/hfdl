// Package fcio is the file IO engine: all block writes,
// verify reads, de-sparse zero-fills and install copies go through it. It
// owns a FastCopy mainBuf-style arena (Pool) and a tiered per-volume IO
// model:
//
//   - fadvise (TierAuto, default): buffered IO plus trailing
//     posix_fadvise(DONTNEED) behind the flush offset, keeping page-cache
//     pollution in check (FastCopy DisableLocalBuffering analog);
//   - direct (TierDirect, opt-in): O_DIRECT for the aligned interior of each
//     block range, with unaligned head/tail fragments on a secondary
//     buffered fd — no read-modify-write anywhere and no write ever crosses
//     a block boundary. Capability is probed once per volume (one aligned
//     block written/read back via a temp file) and cached through the
//     injected CapsCache (kv key volcaps:<dev>); EINVAL/EOPNOTSUPP
//     downgrades the volume to the fadvise tier;
//   - plain (TierPlain): buffered IO without fadvise, for exotic netfs that
//     rejects even advice.
//
// Windows uses sparse-file FSCTLs, allocated-range queries, no-buffering IO,
// and volume identity/media probes. macOS uses F_NOCACHE/F_RDAHEAD,
// F_PREALLOCATE, extent queries, and volume identity. Only the generic GOOS
// fallback degrades to buffered IO, unknown filesystem classification, and
// whole-file extent reporting.
//
// fcio never imports store: CapsCache is defined here and satisfied
// structurally by the store package; cmd wires the two together.
package fcio
