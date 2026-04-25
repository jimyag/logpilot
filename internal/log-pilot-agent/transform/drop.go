package transform

import (
	"context"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

type dropTransform struct {
	key   string
	value string
}

// NewDropTransform discards records where Meta[key] == value.
func NewDropTransform(key, value string) Transform {
	return &dropTransform{key: key, value: value}
}

func (t *dropTransform) Transform(_ context.Context, records []input.Record) ([]input.Record, error) {
	out := records[:0]
	for _, r := range records {
		if r.Meta[t.key] != t.value {
			out = append(out, r)
		}
	}
	return out, nil
}
