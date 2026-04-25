package output

import (
	"context"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

// Output sends a batch of records to a destination.
type Output interface {
	// WriteBatch sends records. Offset advances only after successful send.
	WriteBatch(ctx context.Context, records []input.Record) error

	// Close flushes buffered records and releases resources.
	Close() error
}
