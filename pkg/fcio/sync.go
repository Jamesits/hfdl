package fcio

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

// SyncFile fsyncs an already-complete file at path for durability. It exists for callers
// that finalize a file they opened outside the Engine and only
// need the durability + cache-hygiene primitive, not a tiered *File.
func SyncFile(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, sp := startDetailSpan(ctx, "fcio.fsync", attribute.String("path", path))
	defer endDetailSpan(sp)
	f, err := openForSync(path)
	if err != nil {
		return fmt.Errorf("fcio: open %s: %w", path, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fcio: fsync %s: %w", path, err)
	}
	// The file is durable now and this process won't re-read it, so drop it from
	// the page cache — an install pass over many blobs would otherwise evict the
	// hot working set to hold data nobody reads again. Must follow the fsync:
	// fadvise(DONTNEED) only drops clean pages. No-op where the open flags
	// already handle it (Windows) or there is no equivalent (macOS).
	dropFromCache(f)
	return nil
}
