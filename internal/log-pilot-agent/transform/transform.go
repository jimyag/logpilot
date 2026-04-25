package transform

import (
	"context"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

// Transform processes a batch of records.
// Records may be added, removed, or modified.
type Transform interface {
	Transform(ctx context.Context, records []input.Record) ([]input.Record, error)
}
