package transfer

import "context"

// EventKind identifies a block- or file-lifecycle transition surfaced to
// stats/TUI consumers via Downloader.Events.
type EventKind int

const (
	EventBlockStart EventKind = iota // attempt connected; block streaming began
	EventBlockDone                   // block fully flushed and completed with the leaser
	EventBlockRetry                  // retriable attempt failure (net/header/status/validation)
	EventStall                       // stall policy killed the connection
	EventCheckpoint                  // durable checkpoint persisted (Bytes = fsynced total)
	EventRequeued                    // block handed back to the leaser with backoff
	EventRangeless                   // upstream answered 200 to a ranged request
)

func (k EventKind) String() string {
	switch k {
	case EventBlockStart:
		return "block-start"
	case EventBlockDone:
		return "block-done"
	case EventBlockRetry:
		return "block-retry"
	case EventStall:
		return "stall"
	case EventCheckpoint:
		return "checkpoint"
	case EventRequeued:
		return "requeued"
	case EventRangeless:
		return "rangeless"
	}
	return "unknown"
}

// Event is one block lifecycle transition. BlockID is 0 for file-level
// kinds (EventCheckpoint). Err is set on failure kinds.
type Event struct {
	FileID, BlockID int64
	Kind            EventKind
	Bytes           int64
	Upstream        string
	Err             error
}

// emit delivers an event without ever blocking a worker. Events are
// best-effort telemetry; most emits use the detached,
// never-cancelled ctx (done/requeue/checkpoint), so a blocking send against a
// gone or wedged consumer with a full 1024-buffer would deadlock every worker.
// On a full buffer we drop and count instead. ctx is retained for call-site
// symmetry but no longer gates delivery.
func (d *Downloader) emit(_ context.Context, ev Event) {
	select {
	case d.events <- ev:
	default:
		d.droppedEvents.Add(1)
	}
}
