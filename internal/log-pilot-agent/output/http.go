package output

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

// HTTPConfig configures an HTTP output.
type HTTPConfig struct {
	URL string `yaml:"url"`
}

type httpOutput struct {
	url    string
	client *http.Client
}

// NewHTTPOutput sends batches of records as JSON arrays via HTTP POST.
func NewHTTPOutput(cfg HTTPConfig) Output {
	return &httpOutput{
		url:    cfg.URL,
		client: &http.Client{},
	}
}

func (o *httpOutput) WriteBatch(ctx context.Context, records []input.Record) error {
	entries := make([]map[string]interface{}, len(records))
	for i, r := range records {
		entry := make(map[string]interface{}, len(r.Meta)+1)
		entry["data"] = string(r.Data)
		for k, v := range r.Meta {
			entry[k] = v
		}
		entries[i] = entry
	}

	body, err := json.Marshal(entries)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("http output: status %d from %s", resp.StatusCode, o.url)
	}
	return nil
}

func (o *httpOutput) Close() error { return nil }
