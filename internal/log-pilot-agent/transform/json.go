package transform

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

type jsonTransform struct{}

// NewJSONTransform parses each record's Data as JSON and merges keys into Meta.
func NewJSONTransform() Transform { return &jsonTransform{} }

func (t *jsonTransform) Transform(_ context.Context, records []input.Record) ([]input.Record, error) {
	for i := range records {
		var m map[string]string
		if err := json.Unmarshal(records[i].Data, &m); err == nil {
			if records[i].Meta == nil {
				records[i].Meta = make(map[string]string)
			}
			maps.Copy(records[i].Meta, m)
		}
	}
	return records, nil
}
