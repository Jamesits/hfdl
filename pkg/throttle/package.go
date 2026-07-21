// Package throttle provides the two independent rate-limiting mechanisms
// composed by sched: a hot-settable token Bucket (HF API IOPS and global
// download bandwidth) and a DutyLimiter enforcing a disk active-time
// ceiling. The DutyLimiter is a direct port of FastCopy's WaitCheck() and
// TransSize() (src/fastcopy.cpp:2196-2270, fastcopy.h WaitCalc/TransSize).
//
// Level semantics are an active-time percentage: p means "disk may be
// busy at most p% of wall time", so the duty fraction is A = p/100 with
// FastCopy's literal media derating applied to A (netfs A/1.4, non-SSD
// A/2); the sleep debt per Checkpoint is busy*(1-A)/A + lastRemain exactly
// as in FastCopy's WaitCheck. p=100 is unlimited: Checkpoint never sleeps
// and does no accounting. p=0 is a pause (sched's SetLimits(disk=0)):
// Checkpoint blocks in <=200ms ctx-aware slices until the level changes.
// Callers pass --disk-active through unchanged.
//
// Both types keep a 10s ring of per-second atomic counters for windowed
// stats; the ring is advanced lazily on read — no background goroutine.
//
// Clocks and the sleep function are injectable through unexported fields
// for deterministic in-package tests; production construction is always
// NewBucket/NewDutyLimiter. Pacing inside Bucket.Wait uses real timers
// even with an injected clock — the injected clock drives accounting and
// refill only.
package throttle
