package input

import "context"

// Record is a single log record read from a source.
type Record struct {
	Data   []byte
	Offset int64
	Meta   map[string]string
}

// Input reads records from a source in batches.
type Input interface {
	ReadBatch(ctx context.Context, size int) ([]Record, error)
	Lag() int64
	Close() error
}
