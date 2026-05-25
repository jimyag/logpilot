package output

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

// HTTPConfig configures an HTTP output.
type HTTPConfig struct {
	URL string `yaml:"url"`
	// Headers contains optional HTTP headers added to every request
	// (e.g., "Authorization: Bearer <token>").
	Headers map[string]string `yaml:"headers"`
	// TLSSkipVerify disables TLS certificate verification. Use only in dev/test.
	TLSSkipVerify bool `yaml:"tlsSkipVerify"`
	// TLSCACert is a PEM-encoded CA certificate used to verify the server.
	TLSCACert string `yaml:"tlsCACert"`
}

type httpOutput struct {
	cfg    HTTPConfig
	client *http.Client
}

// NewHTTPOutput sends batches of records as JSON arrays via HTTP POST.
func NewHTTPOutput(cfg HTTPConfig) (Output, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.TLSSkipVerify, //nolint:gosec // opt-in per config
	}
	if cfg.TLSCACert != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(cfg.TLSCACert)) {
			return nil, fmt.Errorf("http output: failed to parse TLSCACert")
		}
		tlsCfg.RootCAs = pool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg

	return &httpOutput{
		cfg: cfg,
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
	}, nil
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range o.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("http output: status %d from %s", resp.StatusCode, o.cfg.URL)
	}
	return nil
}

func (o *httpOutput) Close() error { return nil }
