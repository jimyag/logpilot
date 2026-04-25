package output

import (
	"context"
	"encoding/json"
	"os"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

type fileOutput struct {
	path string
	f    *os.File
}

// NewFileOutput writes records as newline-delimited JSON to a local file.
// Primarily intended for debugging.
func NewFileOutput(path string) Output {
	return &fileOutput{path: path}
}

func (o *fileOutput) WriteBatch(_ context.Context, records []input.Record) error {
	if o.f == nil {
		f, err := os.OpenFile(o.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		o.f = f
	}
	enc := json.NewEncoder(o.f)
	for _, r := range records {
		entry := make(map[string]interface{}, len(r.Meta)+1)
		entry["data"] = string(r.Data)
		for k, v := range r.Meta {
			entry[k] = v
		}
		if err := enc.Encode(entry); err != nil {
			return err
		}
	}
	return nil
}

func (o *fileOutput) Close() error {
	if o.f != nil {
		return o.f.Close()
	}
	return nil
}
