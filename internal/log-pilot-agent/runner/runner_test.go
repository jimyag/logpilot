package runner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/output"
)

// mockInput returns a fixed set of records, then blocks.
type mockInput struct {
	records []input.Record
	pos     int32
	lag     int64
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
	end := pos + size
	if end > len(m.records) {
		end = len(m.records)
	}
	batch := m.records[pos:end]
	atomic.StoreInt32(&m.pos, int32(end))
	atomic.StoreInt64(&m.lag, int64(len(m.records)-end))
	return batch, nil
}

func (m *mockInput) Lag() int64   { return atomic.LoadInt64(&m.lag) }
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
