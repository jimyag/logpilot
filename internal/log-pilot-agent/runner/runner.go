package runner

import (
	"context"
	"sync/atomic"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/clean"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/output"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/transform"
)

// Config holds everything needed to build a Runner.
type Config struct {
	Name       string
	Input      input.Input
	Transforms []transform.Transform
	Output     output.Output
	Clean      clean.Clean
	BatchLen   int
}

// Runner executes an Input→Transform→Output→Clean pipeline.
type Runner struct {
	cfg     Config
	stopped int32 // atomic flag for graceful shutdown
}

// New creates a Runner from the given Config.
func New(cfg Config) *Runner {
	if cfg.BatchLen == 0 {
		cfg.BatchLen = 1000
	}
	return &Runner{cfg: cfg}
}

// Run blocks until ctx is cancelled, then drains and shuts down gracefully.
func (r *Runner) Run(ctx context.Context) {
	for {
		if atomic.LoadInt32(&r.stopped) > 0 {
			break
		}
		records, err := r.cfg.Input.ReadBatch(ctx, r.cfg.BatchLen)
		if err != nil || ctx.Err() != nil {
			break
		}
		if len(records) == 0 {
			continue
		}
		records = r.applyTransforms(ctx, records)
		if len(records) == 0 {
			continue
		}
		if r.cfg.Output != nil {
			_ = r.cfg.Output.WriteBatch(ctx, records)
		}
	}
	r.shutdown()
}

// Lag returns the number of unread bytes remaining in the input.
func (r *Runner) Lag() int64 {
	if r.cfg.Input == nil {
		return 0
	}
	return r.cfg.Input.Lag()
}

// Stop signals the runner to finish the current batch and shut down.
func (r *Runner) Stop() {
	atomic.StoreInt32(&r.stopped, 1)
}

func (r *Runner) applyTransforms(ctx context.Context, records []input.Record) []input.Record {
	for _, t := range r.cfg.Transforms {
		var err error
		records, err = t.Transform(ctx, records)
		if err != nil || len(records) == 0 {
			return nil
		}
	}
	return records
}

// shutdown drains remaining buffered input, flushes output, and commits all offsets.
func (r *Runner) shutdown() {
	ctx := context.Background()
	if r.cfg.Input != nil {
		for {
			records, err := r.cfg.Input.ReadBatch(ctx, r.cfg.BatchLen)
			if err != nil || len(records) == 0 {
				break
			}
			records = r.applyTransforms(ctx, records)
			if len(records) > 0 && r.cfg.Output != nil {
				_ = r.cfg.Output.WriteBatch(ctx, records)
			}
		}
		r.cfg.Input.Close()
	}
	// Flush buffered output and release connections.
	if r.cfg.Output != nil {
		r.cfg.Output.Close()
	}
	// Force-commit all offsets regardless of offsetCommitEvery setting.
	if r.cfg.Clean != nil {
		_ = r.cfg.Clean.Clean(clean.RunnerMeta{})
	}
}
