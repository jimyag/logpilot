package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/clean"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/output"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/transform"
)

// mockInput returns a fixed set of records, then blocks.
type mockInput struct {
	records []input.Record
	pos     int32
	lag     int64
	commits int64
}

func (m *mockInput) ReadBatch(ctx context.Context, size int) ([]input.Record, error) {
	pos := int(atomic.LoadInt32(&m.pos))
	if pos >= len(m.records) {
		select {
		case <-ctx.Done():
			return nil, nil
		case <-time.After(10 * time.Millisecond):
			return nil, nil
		}
	}
	end := min(pos+size, len(m.records))
	batch := m.records[pos:end]
	atomic.StoreInt32(&m.pos, int32(end))
	atomic.StoreInt64(&m.lag, int64(len(m.records)-end))
	return batch, nil
}

func (m *mockInput) Lag() int64 { return atomic.LoadInt64(&m.lag) }
func (m *mockInput) Commit() error {
	atomic.AddInt64(&m.commits, 1)
	return nil
}
func (m *mockInput) Close() error { return nil }

// mockOutput collects received records.
type mockOutput struct {
	received []input.Record
}

func (m *mockOutput) WriteBatch(_ context.Context, records []input.Record) error {
	m.received = append(m.received, records...)
	return nil
}
func (m *mockOutput) Close() error { return nil }

var _ output.Output = (*mockOutput)(nil)

type flakyOutput struct {
	failures int32
	received []input.Record
}

func (f *flakyOutput) WriteBatch(_ context.Context, records []input.Record) error {
	if atomic.AddInt32(&f.failures, -1) >= 0 {
		return errors.New("temporary output failure")
	}
	f.received = append(f.received, records...)
	return nil
}

func (f *flakyOutput) Close() error { return nil }

type alwaysFailOutput struct{ attempts int64 }

func (a *alwaysFailOutput) WriteBatch(_ context.Context, _ []input.Record) error {
	atomic.AddInt64(&a.attempts, 1)
	return errors.New("permanent output failure")
}

func (a *alwaysFailOutput) Close() error { return nil }

type timeoutOnDrainInput struct {
	records  []input.Record
	pos      int32
	commits  int64
	lagValue int64
}

func (t *timeoutOnDrainInput) ReadBatch(ctx context.Context, _ int) ([]input.Record, error) {
	if atomic.CompareAndSwapInt32(&t.pos, 0, 1) {
		atomic.StoreInt64(&t.lagValue, 0)
		return t.records, nil
	}
	<-ctx.Done()
	return nil, nil
}

func (t *timeoutOnDrainInput) Lag() int64 { return atomic.LoadInt64(&t.lagValue) }
func (t *timeoutOnDrainInput) Commit() error {
	atomic.AddInt64(&t.commits, 1)
	return nil
}
func (t *timeoutOnDrainInput) Close() error { return nil }

type mockTransform struct {
	err     error
	records []input.Record
}

func (m *mockTransform) Transform(_ context.Context, records []input.Record) ([]input.Record, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.records != nil {
		return m.records, nil
	}
	return records, nil
}

type mockClean struct{ cleaned bool }

func (c *mockClean) ShouldClean(_ clean.RunnerMeta) (bool, error) { return true, nil }
func (c *mockClean) Clean(_ clean.RunnerMeta) error {
	c.cleaned = true
	return nil
}

type errorInput struct{ err error }

func (e *errorInput) ReadBatch(context.Context, int) ([]input.Record, error) { return nil, e.err }
func (e *errorInput) Lag() int64                                             { return 0 }
func (e *errorInput) Commit() error                                          { return nil }
func (e *errorInput) Close() error                                           { return nil }

var _ transform.Transform = (*mockTransform)(nil)

func TestRunnerProcessesAllRecords(t *testing.T) {
	records := []input.Record{
		{Data: []byte("line1")},
		{Data: []byte("line2")},
		{Data: []byte("line3")},
	}
	in := &mockInput{records: records, lag: int64(len(records))}
	out := &mockOutput{}

	r := New(Config{
		Name:     "test",
		Input:    in,
		Output:   out,
		BatchLen: 10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	r.Run(ctx)

	if len(out.received) != 3 {
		t.Fatalf("expected 3 records, got %d", len(out.received))
	}
	if string(out.received[0].Data) != "line1" {
		t.Errorf("unexpected first record: %q", out.received[0].Data)
	}
}

func TestRunnerStopDrainsBuffer(t *testing.T) {
	records := []input.Record{
		{Data: []byte("a")},
		{Data: []byte("b")},
	}
	in := &mockInput{records: records, lag: int64(len(records))}
	out := &mockOutput{}

	r := New(Config{Input: in, Output: out, BatchLen: 1})

	// Stop before running to trigger drain-on-shutdown path.
	r.Stop()
	r.Run(context.Background())

	// After shutdown all records should be drained.
	if len(out.received) != 2 {
		t.Fatalf("expected 2 records after drain, got %d", len(out.received))
	}
}

func TestRunnerLag(t *testing.T) {
	in := &mockInput{lag: 42}
	r := New(Config{Input: in, BatchLen: 1})
	if r.Lag() != 42 {
		t.Fatalf("expected lag 42, got %d", r.Lag())
	}
}

func TestRunnerRetriesOutputBeforeCommit(t *testing.T) {
	records := []input.Record{{Data: []byte("line1")}}
	in := &mockInput{records: records, lag: int64(len(records))}
	out := &flakyOutput{failures: 1}

	r := New(Config{Input: in, Output: out, BatchLen: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	r.Run(ctx)

	if len(out.received) != 1 {
		t.Fatalf("expected record after retry, got %d", len(out.received))
	}
	if atomic.LoadInt64(&in.commits) == 0 {
		t.Fatal("expected input commit after successful output")
	}
}

func TestRunnerDoneChannelClosedAfterRun(t *testing.T) {
	r := New(Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	go r.Run(ctx)

	select {
	case <-r.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected Done channel to close after Run returns")
	}
}

func TestRunnerSentCount(t *testing.T) {
	records := []input.Record{{Data: []byte("a")}, {Data: []byte("b")}, {Data: []byte("c")}}
	in := &mockInput{records: records, lag: int64(len(records))}
	out := &mockOutput{}

	r := New(Config{Input: in, Output: out, BatchLen: 3})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	g := make(chan struct{})
	go func() {
		defer close(g)
		r.Run(ctx)
	}()
	<-g

	if got := r.Sent(); got != 3 {
		t.Fatalf("expected Sent to report 3, got %d", got)
	}
}

func TestRunnerLagNilInput(t *testing.T) {
	if got := New(Config{}).Lag(); got != 0 {
		t.Fatalf("expected nil input lag to be 0, got %d", got)
	}
}

func TestRunnerApplyTransformsError(t *testing.T) {
	records := []input.Record{{Data: []byte("line1")}}
	in := &mockInput{records: records, lag: int64(len(records))}
	out := &mockOutput{}

	r := New(Config{
		Input:      in,
		Output:     out,
		BatchLen:   1,
		Transforms: []transform.Transform{&mockTransform{err: errors.New("transform failed")}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	g := make(chan struct{})
	go func() {
		defer close(g)
		r.Run(ctx)
	}()
	<-g

	if len(out.received) != 0 {
		t.Fatalf("expected records to be dropped, got %d delivered", len(out.received))
	}
	if atomic.LoadInt64(&in.commits) != 0 {
		t.Fatalf("expected no commit when transform fails, got %d", atomic.LoadInt64(&in.commits))
	}
}

func TestRunnerApplyTransformsEmpty(t *testing.T) {
	records := []input.Record{{Data: []byte("line1")}}
	in := &mockInput{records: records, lag: int64(len(records))}
	out := &mockOutput{}

	r := New(Config{
		Input:      in,
		Output:     out,
		BatchLen:   1,
		Transforms: []transform.Transform{&mockTransform{records: []input.Record{}}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	g := make(chan struct{})
	go func() {
		defer close(g)
		r.Run(ctx)
	}()
	<-g

	if len(out.received) != 0 {
		t.Fatalf("expected records to be dropped, got %d delivered", len(out.received))
	}
	if atomic.LoadInt64(&in.commits) != 0 {
		t.Fatalf("expected no commit when transform returns no records, got %d", atomic.LoadInt64(&in.commits))
	}
}

func TestRunnerApplyTransformsPassthrough(t *testing.T) {
	records := []input.Record{{Data: []byte("line1")}}
	r := New(Config{})

	got := r.applyTransforms(context.Background(), records)
	if len(got) != 1 || string(got[0].Data) != "line1" {
		t.Fatalf("expected records to pass through unchanged, got %#v", got)
	}
}

func TestRunnerStopBeforeRunImmediateShutdown(t *testing.T) {
	r := New(Config{})
	r.Stop()

	go r.Run(context.Background())

	select {
	case <-r.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected Run to exit immediately after Stop before Run")
	}
}

func TestRunnerShutdownSkipsCleanWhenDrainFails(t *testing.T) {
	cleaner := &mockClean{}
	r := New(Config{
		Input:    &errorInput{err: errors.New("drain failed")},
		Clean:    cleaner,
		BatchLen: 1,
	})
	g := make(chan struct{})

	r.Stop()
	go func() {
		defer close(g)
		r.Run(context.Background())
	}()
	<-g

	if cleaner.cleaned {
		t.Fatal("expected Clean not to be called when drain fails")
	}

	select {
	case <-r.Done():
	default:
		t.Fatal("expected Done channel to be closed after shutdown")
	}
}

func TestRunnerRunStopsOnInputError(t *testing.T) {
	r := New(Config{Input: &errorInput{err: errors.New("read failed")}, BatchLen: 1})
	r.Run(context.Background())

	select {
	case <-r.Done():
	default:
		t.Fatal("expected Done channel to be closed after input error")
	}
}

func TestRunnerStopCancelsRunningRunner(t *testing.T) {
	r := New(Config{Input: &mockInput{}, BatchLen: 1})
	go r.Run(context.Background())

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		r.cancelMu.Lock()
		started := r.cancel != nil
		r.cancelMu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("runner did not start in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	r.Stop()

	select {
	case <-r.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected Stop to cancel a running runner")
	}
}

func TestRunnerWriteBatchNoOutput(t *testing.T) {
	r := New(Config{})
	if err := r.writeBatch(context.Background(), []input.Record{{Data: []byte("line1")}}); err != nil {
		t.Fatalf("expected nil error with no output, got %v", err)
	}
}

func TestRunnerRunStopsWhenWriteBatchFails(t *testing.T) {
	in := &mockInput{records: []input.Record{{Data: []byte("line1")}}, lag: 1}
	out := &alwaysFailOutput{}
	r := New(Config{Input: in, Output: out, BatchLen: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	r.Run(ctx)

	if got := atomic.LoadInt64(&in.commits); got != 0 {
		t.Fatalf("expected no commits when output never succeeds, got %d", got)
	}
	if got := r.Sent(); got != 0 {
		t.Fatalf("expected Sent to remain 0, got %d", got)
	}
	if got := atomic.LoadInt64(&out.attempts); got == 0 {
		t.Fatal("expected output write attempts")
	}
}

func TestRunnerShutdownSkipsCleanWhenDrainWriteFails(t *testing.T) {
	in := &mockInput{records: []input.Record{{Data: []byte("line1")}}, lag: 1}
	out := &alwaysFailOutput{}
	cleaner := &mockClean{}
	r := New(Config{Input: in, Output: out, Clean: cleaner, BatchLen: 1})

	r.Stop()
	r.Run(context.Background())

	if cleaner.cleaned {
		t.Fatal("expected Clean not to be called when drain write fails")
	}
	if got := atomic.LoadInt64(&in.commits); got != 0 {
		t.Fatalf("expected no commits when drain write fails, got %d", got)
	}
	if got := r.Sent(); got != 0 {
		t.Fatalf("expected Sent to remain 0 when drain write fails, got %d", got)
	}
}

func TestRunnerShutdownSkipsCleanWhenDrainTimesOut(t *testing.T) {
	in := &timeoutOnDrainInput{records: []input.Record{{Data: []byte("line1")}}, lagValue: 1}
	out := &mockOutput{}
	cleaner := &mockClean{}
	r := New(Config{Input: in, Output: out, Clean: cleaner, BatchLen: 1})

	r.Stop()
	r.Run(context.Background())

	if cleaner.cleaned {
		t.Fatal("expected Clean not to be called when drain context times out")
	}
	if len(out.received) != 1 {
		t.Fatalf("expected drained batch to be written before timeout, got %d records", len(out.received))
	}
	if got := atomic.LoadInt64(&in.commits); got != 1 {
		t.Fatalf("expected one commit before drain timeout, got %d", got)
	}
	if got := r.Sent(); got != 1 {
		t.Fatalf("expected Sent to report 1 before timeout, got %d", got)
	}
}

func TestRunnerShutdownCleansWhenFullyDrained(t *testing.T) {
	in := &mockInput{records: []input.Record{{Data: []byte("line1")}}, lag: 1}
	out := &mockOutput{}
	cleaner := &mockClean{}
	r := New(Config{Input: in, Output: out, Clean: cleaner, BatchLen: 1})

	r.Stop()
	r.Run(context.Background())

	if !cleaner.cleaned {
		t.Fatal("expected Clean to be called after a fully drained shutdown")
	}
	if len(out.received) != 1 {
		t.Fatalf("expected drained record to be written, got %d", len(out.received))
	}
	if got := r.Sent(); got != 1 {
		t.Fatalf("expected Sent to report 1 after drain, got %d", got)
	}
}
