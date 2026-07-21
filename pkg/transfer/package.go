// Package transfer is the multi-upstream HTTP range downloader: blocks are
// the unit of scheduling, written byte ranges (an IntervalSet) are the unit
// of resume, so restarts may change block size or connection count without
// invalidating partial progress.
//
// The Downloader owns scheduling against the BlockLeaser interface (sched
// implements it over the store; transfer stays DB-free), RAM buffering
// through the fcio pool, the layered stall/timeout policy, durable-only
// checkpointing (snapshot -> fsync -> ProgressSink, in that order), and hot
// parallelism. Byte sources are pluggable via BlockSource: the package's own
// httpSource (multi-upstream ranged GETs) and xet.Source (which imports this
// package; transfer never imports xet).
//
// Durability invariant: the persisted progress blob records only bytes that
// were fsynced first. RAM-buffered bytes are invisible to checkpoints, so a
// crash loses at most one checkpoint interval of work and the durable record
// can only under-report, never over-report.
package transfer
