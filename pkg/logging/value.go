package logging

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

// SourceKey is the attr key Component stamps on a logger. The fan-out
// Handler lifts a top-level SourceKey attr out of the JSON attrs blob into
// Record.Source / LogRow.Source so sinks can index it as a column.
const SourceKey = "source"

// JSONValue renders v as JSON for slog. Plain slog text output prints slices
// and maps with bare spaces ("[a b c]"), which is unreadable when elements
// themselves contain spaces; JSON keeps every boundary visible.
func JSONValue(v any) slog.Value {
	b, err := json.Marshal(v)
	if err != nil {
		return slog.StringValue(fmt.Sprintf("<json: %v>", err))
	}
	return slog.StringValue(string(b))
}

// Component returns a logger carrying source=<name> as an attr.
func Component(log *slog.Logger, name string) *slog.Logger {
	return log.With(slog.String(SourceKey, name))
}
