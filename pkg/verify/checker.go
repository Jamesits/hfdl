package verify

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"log/slog"
	"strings"

	"github.com/jamesits/hfdl/pkg/fcio"
	"github.com/jamesits/hfdl/pkg/otel"
	"github.com/jamesits/hfdl/pkg/throttle"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

const (
	hashSHA256  = "sha256"
	hashGitSHA1 = "git-sha1"
)

// MismatchError reports a terminal hash-check failure.
type MismatchError struct {
	Path, Want, Got string
}

func (e *MismatchError) Error() string {
	return fmt.Sprintf("verify: hash mismatch on %s: want %s, got %s", e.Path, e.Want, e.Got)
}

// Checker hashes and de-sparses cache files through the fcio engine,
// throttled by the disk DutyLimiter (one DutyCalc per call = one worker).
type Checker struct {
	e   *fcio.Engine
	p   *fcio.Pool
	d   *throttle.DutyLimiter
	log *slog.Logger

	tracer   trace.Tracer
	failures metric.Int64Counter

	// fallocateFn is the de-sparse tier-B probe seam (tests inject errnos).
	fallocateFn func(f *fcio.File, size int64) error
}

// NewChecker builds a Checker. prov may be nil (noop providers).
func NewChecker(e *fcio.Engine, p *fcio.Pool, d *throttle.DutyLimiter, log *slog.Logger, prov *otel.Providers) *Checker {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if prov == nil {
		prov = otel.Noop()
	}
	failures, err := prov.Meter("hfdl.verify").Int64Counter("hfdl.verify.failures",
		metric.WithUnit("{event}"),
		metric.WithDescription("Terminal file hash verification failures."))
	if err != nil {
		failures, _ = metricnoop.NewMeterProvider().Meter("hfdl.verify").Int64Counter("hfdl.verify.failures")
	}
	return &Checker{
		e:           e,
		p:           p,
		d:           d,
		log:         log,
		tracer:      prov.Tracer("hfdl.verify"),
		failures:    failures,
		fallocateFn: func(f *fcio.File, size int64) error { return f.Fallocate(size) },
	}
}

// Hash computes the verify target over f: sha256 for LFS files, git blob
// sha1 ("blob {size}\x00"+content) otherwise. Reads run through the engine
// ReadAll pipeline with a duty checkpoint between chunks.
func (c *Checker) Hash(ctx context.Context, f *fcio.File, size int64, isLFS bool) (string, error) {
	ctx, span := c.tracer.Start(ctx, "verify.file")
	defer span.End()
	span.SetAttributes(
		attribute.String("path", f.Path()),
		attribute.Int64("size", size),
		attribute.String("hash", hashKind(isLFS)),
	)
	sum, err := c.hash(ctx, f, size, isLFS)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	return sum, nil
}

// Verify hashes f and compares against want, returning *MismatchError on
// divergence (span Error status + hfdl.verify.failures counter).
func (c *Checker) Verify(ctx context.Context, f *fcio.File, size int64, isLFS bool, want string) error {
	ctx, span := c.tracer.Start(ctx, "verify.file")
	defer span.End()
	span.SetAttributes(
		attribute.String("path", f.Path()),
		attribute.Int64("size", size),
		attribute.String("hash", hashKind(isLFS)),
	)
	got, err := c.hash(ctx, f, size, isLFS)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if !strings.EqualFold(got, want) {
		merr := &MismatchError{Path: f.Path(), Want: want, Got: got}
		span.RecordError(merr)
		span.SetStatus(codes.Error, "hash mismatch")
		c.failures.Add(ctx, 1)
		c.log.WarnContext(ctx, "hash mismatch", "path", f.Path(), "want", want, "got", got)
		return merr
	}
	return nil
}

func hashKind(isLFS bool) string {
	if isLFS {
		return hashSHA256
	}
	return hashGitSHA1
}

func (c *Checker) hash(ctx context.Context, f *fcio.File, size int64, isLFS bool) (string, error) {
	var h hash.Hash
	if isLFS {
		h = sha256.New()
	} else {
		h = sha1.New()
		if _, err := fmt.Fprintf(h, "blob %d\x00", size); err != nil {
			return "", fmt.Errorf("verify: hash header %s: %w", f.Path(), err)
		}
	}
	var calc throttle.DutyCalc
	err := c.e.ReadAll(ctx, f, func(p []byte, off int64) error {
		if _, err := h.Write(p); err != nil {
			return err
		}
		if c.d != nil {
			if err := c.d.Checkpoint(ctx, &calc); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("verify: read %s: %w", f.Path(), err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
