package transform

import (
	"context"
	"maps"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

type labelTransform struct {
	fields map[string]string
}

// NewLabelTransform adds fixed key-value pairs to every record's Meta.
func NewLabelTransform(fields map[string]string) Transform {
	return &labelTransform{fields: fields}
}

func (t *labelTransform) Transform(_ context.Context, records []input.Record) ([]input.Record, error) {
	for i := range records {
		if records[i].Meta == nil {
			records[i].Meta = make(map[string]string)
		}
		maps.Copy(records[i].Meta, t.fields)
	}
	return records, nil
}
