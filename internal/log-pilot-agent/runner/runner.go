package runner

import (
	"context"
	"sync/atomic"
	"time"

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
	stopped int32         // atomic flag for graceful shutdown
	done    chan struct{} // closed when Run() returns, for callers to wait on
	sent    int64
}

// New creates a Runner from the given Config.
func New(cfg Config) *Runner {
	if cfg.BatchLen == 0 {
		cfg.BatchLen = 1000
	}
	return &Runner{cfg: cfg, done: make(chan struct{})}
}

// Run blocks until stopped or ctx cancelled, then drains and shuts down gracefully.
// Closes the Done() channel when it returns.
func (r *Runner) Run(ctx context.Context) {
	defer close(r.done)
	for {
		if atomic.LoadInt32(&r.stopped) > 0 {
			break
		}
		records, err := r.cfg.Input.ReadBatch(ctx, r.cfg.BatchLen)
		if err != nil || ctx.Err() != nil {
			break
		}
		if len(records) == 0 {
			// Back off to avoid busy-looping when the source has no new data.
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		records = r.applyTransforms(ctx, records)
		if len(records) == 0 {
			continue
		}
		if err := r.writeBatch(ctx, records, true); err != nil {
			break
		}
		if r.cfg.Input != nil {
			_ = r.cfg.Input.Commit()
		}
		atomic.AddInt64(&r.sent, int64(len(records)))
	}
	r.shutdown()
}

// Done returns a channel that is closed when Run() has fully completed,
// including drain and output flush. Use this to wait for clean shutdown.
func (r *Runner) Done() <-chan struct{} { return r.done }

// Lag returns the number of unread bytes remaining in the input.
func (r *Runner) Lag() int64 {
	if r.cfg.Input == nil {
		return 0
	}
	return r.cfg.Input.Lag()
}

// Sent returns the number of records successfully written by this runner.
func (r *Runner) Sent() int64 { return atomic.LoadInt64(&r.sent) }

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

func (r *Runner) writeBatch(ctx context.Context, records []input.Record, stopAware bool) error {
	if r.cfg.Output == nil || len(records) == 0 {
		return nil
	}
	backoff := 200 * time.Millisecond
	for {
		if stopAware && atomic.LoadInt32(&r.stopped) > 0 {
			return context.Canceled
		}
		if err := r.cfg.Output.WriteBatch(ctx, records); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// shutdown drains remaining buffered input, flushes output, and commits all offsets.
func (r *Runner) shutdown() {
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if r.cfg.Input != nil {
		for {
			records, err := r.cfg.Input.ReadBatch(drainCtx, r.cfg.BatchLen)
			if err != nil || len(records) == 0 {
				break
			}
			records = r.applyTransforms(drainCtx, records)
			if len(records) > 0 {
				if err := r.writeBatch(drainCtx, records, false); err != nil {
					break
				}
				_ = r.cfg.Input.Commit()
				atomic.AddInt64(&r.sent, int64(len(records)))
			}
		}
		r.cfg.Input.Close()
	}
	if r.cfg.Output != nil {
		r.cfg.Output.Close()
	}
	if r.cfg.Clean != nil {
		_ = r.cfg.Clean.Clean(clean.RunnerMeta{})
	}
}
