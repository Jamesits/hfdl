package verify

import (
	"context"
	"errors"
	"fmt"

	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/throttle"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// De-sparse methods: fallocate first, zero-fill walk as fallback.
const (
	methodFallocate = "fallocate"
	methodWalk      = "walk"
)

// DeSparse guarantees blob density: first a bare fallocate(0, size) which
// converts holes to allocated unwritten extents with no data IO; on
// ENOSYS/EOPNOTSUPP/EINVAL (tier-B volumes) a SEEK_DATA/SEEK_HOLE walk
// zero-fills each gap from the pool's pre-zeroed slab (a fresh read-only
// ZeroSlice per chunk, never mutating shared state). The walk is real disk
// IO, so it checkpoints the disk DutyLimiter per slab. Windows
// sparse-attribute clearing is handled by the cross-platform fcio path, not
// here. The de-sparse method and hole byte count are reported on the trace
// span.
//
// fsync ordering is owned by the disk-queue caller, not this method
// (fsync → de-sparse → fsync → hash); DeSparse deliberately does no fsync.
func (c *Checker) DeSparse(ctx context.Context, f *fcio.File, size int64) (method string, holeBytes int64, err error) {
	ctx, span := c.tracer.Start(ctx, "verify.desparse")
	defer span.End()
	span.SetAttributes(
		attribute.String("path", f.Path()),
		attribute.Int64("size", size),
	)
	method, holeBytes, err = c.deSparse(ctx, f, size)
	span.SetAttributes(
		attribute.String("method", method),
		attribute.Int64("hole_bytes", holeBytes),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return method, holeBytes, err
}

func (c *Checker) deSparse(ctx context.Context, f *fcio.File, size int64) (string, int64, error) {
	err := c.fallocateFn(f, size)
	switch {
	case err == nil:
		if derr := f.Densify(); derr != nil {
			return "", 0, fmt.Errorf("verify: desparse densify %s: %w", f.Path(), derr)
		}
		return methodFallocate, 0, nil
	case !fallocateUnsupported(err):
		return "", 0, fmt.Errorf("verify: desparse fallocate %s: %w", f.Path(), err)
	}

	// Tier B: hole walk. Non-sparse filesystems report the whole file as one
	// data extent, making the walk a no-op exactly when there is nothing to
	// fix.
	if c.p == nil {
		return "", 0, fmt.Errorf("verify: desparse walk on %s requires a pool", f.Path())
	}
	extents, err := f.DataExtents()
	if err != nil {
		return "", 0, fmt.Errorf("verify: desparse extents %s: %w", f.Path(), err)
	}
	slab := c.p.SlabSize()
	var calc throttle.DutyCalc
	var holeBytes int64
	fill := func(start, end int64) error {
		for off := start; off < end; {
			// Shrink the write chunk under heavier duty limiting (FastCopy
			// TransSize) so a checkpoint lands more often; full slab when
			// unlimited (WriteChunkSize returns the passed size).
			chunk := slab
			if c.d != nil {
				chunk = min(chunk, c.d.WriteChunkSize(slab))
			}
			n := min(chunk, end-off)
			// Fresh read-only zero view per chunk — no SetLen on a shared buf,
			// so concurrent de-sparse walks never race on one fill length.
			zero := c.p.ZeroSlice(int(n))
			if werr := f.WriteAt(zero, off); werr != nil {
				// Gap boundaries are page-aligned on Linux, but a fragment
				// that still misses the direct-tier alignment rule goes
				// through the buffered fd instead of failing the walk.
				var uerr *fcio.UnalignedError
				if !errors.As(werr, &uerr) {
					return fmt.Errorf("verify: desparse write %s @%d: %w", f.Path(), off, werr)
				}
				if werr := f.WriteUnaligned(zero.Data(), off); werr != nil {
					return fmt.Errorf("verify: desparse write %s @%d: %w", f.Path(), off, werr)
				}
			}
			holeBytes += n
			off += n
			if c.d != nil {
				if cerr := c.d.Checkpoint(ctx, &calc); cerr != nil {
					return cerr
				}
			}
		}
		return nil
	}

	pos := int64(0)
	for _, ex := range extents {
		if pos >= size {
			break
		}
		if ex[0] > pos {
			if err := fill(pos, min(ex[0], size)); err != nil {
				return "", 0, err
			}
		}
		if ex[1] > pos {
			pos = ex[1]
		}
	}
	if pos < size {
		if err := fill(pos, size); err != nil {
			return "", 0, err
		}
	}
	// Now physically dense: strip any platform sparse attribute (Windows
	// FSCTL_SET_SPARSE=FALSE + fsync; no-op elsewhere) so the finished blob is
	// an ordinary non-sparse file.
	if derr := f.Densify(); derr != nil {
		return "", 0, fmt.Errorf("verify: desparse densify %s: %w", f.Path(), derr)
	}
	return methodWalk, holeBytes, nil
}
