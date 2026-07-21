// Package sched is the queue manager: the integrator that sits between the
// durable queues in pkg/store and the DB-free workers (hfapi, transfer,
// verify, cache, xet). Four durable queues — meta, download, disk
// (reference-hash / salvage-apply / verify) and install — where SQLite
// rows are the queue and in-memory channels are wakeups. Rows are claimed
// via store leases, gated by constraints (api-iops bucket +
// per-(endpoint,stage) 429 cooldowns, bandwidth bucket, disk duty
// limiter, ENOSPC global pause, operator pause) before every dequeue.
//
// Concurrency model: fixed worker pools per queue; the download queue is an
// orchestrator that keeps at most Limits.MaxWorkers files in 'downloading',
// each driven by one transfer.Downloader.Run call in its own goroutine.
// Block-level scheduling inside a file is transfer's job; sched supplies
// the BlockLeaser over store.LeaseBlocks and translates completion,
// requeue (with durable available_at backoff and give-up at 8 retries) and
// file transitions.
//
// Durability: every state change goes through the store's guarded,
// token-fenced transitions — a crashed or fenced worker can never publish
// stale progress. Salvage rows are the one tokenless claim (MarkSalvaging);
// sched keeps an in-memory claimed set for them since hfdl is
// single-process per state DB.
package sched
