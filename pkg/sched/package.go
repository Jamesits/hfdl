// Package sched is the queue manager: the integrator that sits between the
// durable queues in pkg/store and the DB-free workers (hfapi, transfer,
// verify, cache, xet). Four durable queues — meta, download, disk
// (reference-hash / salvage-apply / verify) and install — where SQLite
// rows are the queue and in-memory channels are wakeups. Rows are claimed
// via store leases, gated by constraints (api-iops bucket +
// per-(endpoint,stage) 429 cooldowns, bandwidth bucket, disk duty
// limiter, ENOSPC global pause, operator pause) before queue work. Metadata
// cooldowns require the leased row's endpoint and are checked immediately
// after dequeue; blocked rows are promptly deferred without consuming retry.
//
// Concurrency model: fixed worker pools per queue; the download queue is an
// orchestrator that deepens already-downloading files first (autotune.go —
// a hill-climbed budget of up to Limits.MaxWorkers total connections,
// oldest file filled up to one connection per remaining block) and admits
// the next file only with connections the active set cannot use; each file
// is driven by one transfer.Downloader.Run call in its own goroutine.
// Block-level scheduling inside a file is transfer's job; sched supplies
// the BlockLeaser over store.LeaseBlocks and translates completion,
// requeue (with durable available_at backoff and give-up at 8 retries) and
// file transitions.
//
// Durability: every state change goes through the store's guarded,
// token-fenced transitions — a crashed or fenced worker can never publish
// stale progress. Salvage application uses the durable LeaseSalvage protocol:
// salvaging rows carry a token, are heartbeated during copy, and are recovered
// to queued after lease expiry.
package sched
