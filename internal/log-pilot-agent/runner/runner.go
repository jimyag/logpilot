package runner

import (
	"context"
	"sync"
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
	cfg  Config
	done chan struct{} // closed when Run() returns, for callers to wait on
	sent int64

	// cancelMu guards cancel so that Stop() can safely call it before Run() sets it.
	cancelMu sync.Mutex
	cancel   context.CancelFunc
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

	// Create a cancellable child context. Stop() will cancel it.
	runCtx, cancel := context.WithCancel(ctx)
	r.cancelMu.Lock()
	// If Stop() was called before Run(), cancel immediately.
	if r.cancel != nil {
		r.cancelMu.Unlock()
		cancel()
	} else {
		r.cancel = cancel
		r.cancelMu.Unlock()
	}
	defer cancel()

	for runCtx.Err() == nil {
		// Check for cancellation before reading so we don't consume records
		// from the input without processing them (which would lose them on drain).

		records, err := r.cfg.Input.ReadBatch(runCtx, r.cfg.BatchLen)
		if err != nil {
			break
		}
		if len(records) == 0 {
			// Back off to avoid busy-looping when the source has no new data.
			select {
			case <-runCtx.Done():
				r.shutdown()
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		records = r.applyTransforms(runCtx, records)
		if len(records) == 0 {
			continue
		}
		if err := r.writeBatch(runCtx, records); err != nil {
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

// Stop cancels the runner's context, causing Run() to break out of its loop
// and begin graceful shutdown. It is safe to call before Run() starts.
func (r *Runner) Stop() {
	r.cancelMu.Lock()
	defer r.cancelMu.Unlock()
	if r.cancel != nil {
		r.cancel()
	} else {
		// Mark that Stop() was called before Run(); Run() will cancel immediately.
		r.cancel = func() {}
	}
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

func (r *Runner) writeBatch(ctx context.Context, records []input.Record) error {
	if r.cfg.Output == nil || len(records) == 0 {
		return nil
	}
	backoff := 200 * time.Millisecond
	for {
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

// shutdown drains remaining buffered input, flushes output, and conditionally
// cleans (deletes log files) only if all data was successfully drained.
func (r *Runner) shutdown() {
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// drained tracks whether all remaining data was successfully sent.
	drained := true

	if r.cfg.Input != nil {
		for {
			records, err := r.cfg.Input.ReadBatch(drainCtx, r.cfg.BatchLen)
			if err != nil || len(records) == 0 {
				// ReadBatch returns empty on EOF; real error = not drained.
				if err != nil {
					drained = false
				}
				break
			}
			records = r.applyTransforms(drainCtx, records)
			if len(records) > 0 {
				if err := r.writeBatch(drainCtx, records); err != nil {
					drained = false
					break
				}
				_ = r.cfg.Input.Commit()
				atomic.AddInt64(&r.sent, int64(len(records)))
			}
		}
		// drainCtx expiry also means we didn't drain everything.
		if drainCtx.Err() != nil {
			drained = false
		}
		_ = r.cfg.Input.Close()
	}
	if r.cfg.Output != nil {
		_ = r.cfg.Output.Close()
	}
	if r.cfg.Clean != nil && drained {
		lag := int64(0)
		if r.cfg.Input != nil {
			lag = r.cfg.Input.Lag()
		}
		_ = r.cfg.Clean.Clean(clean.RunnerMeta{Lag: lag})
	}
}
