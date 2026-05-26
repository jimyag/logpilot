package output

import (
	"encoding/json"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

// NewFromSpec creates an Output from an OutputSpec.
func NewFromSpec(spec logpilotv1alpha1.OutputSpec) (Output, error) {
	switch spec.Type {
	case "http":
		url, err := extractString(spec.Config, "url")
		if err != nil {
			return nil, fmt.Errorf("http output: %w", err)
		}
		cfg := HTTPConfig{URL: url}
		// Optional: custom request headers (e.g. Authorization).
		if headers, err := extractStringMap(spec.Config, "headers"); err == nil {
			cfg.Headers = headers
		}
		// Optional: TLS settings.
		if skip, err := extractBool(spec.Config, "tlsSkipVerify"); err == nil {
			cfg.TLSSkipVerify = skip
		}
		if ca, err := extractString(spec.Config, "tlsCACert"); err == nil {
			cfg.TLSCACert = ca
		}
		return NewHTTPOutput(cfg)

	case "file":
		path, err := extractString(spec.Config, "path")
		if err != nil {
			return nil, fmt.Errorf("file output: %w", err)
		}
		return NewFileOutput(path), nil

	default:
		return nil, fmt.Errorf("unknown output type: %q", spec.Type)
	}
}

func extractString(config map[string]apiextensionsv1.JSON, key string) (string, error) {
	v, ok := config[key]
	if !ok {
		return "", fmt.Errorf("missing required config key %q", key)
	}
	var s string
	if err := json.Unmarshal(v.Raw, &s); err != nil {
		return "", fmt.Errorf("config key %q: %w", key, err)
	}
	return s, nil
}

func extractStringMap(config map[string]apiextensionsv1.JSON, key string) (map[string]string, error) { //nolint:unparam
	v, ok := config[key]
	if !ok {
		return nil, fmt.Errorf("missing config key %q", key)
	}
	var m map[string]string
	if err := json.Unmarshal(v.Raw, &m); err != nil {
		return nil, fmt.Errorf("config key %q: %w", key, err)
	}
	return m, nil
}

func extractBool(config map[string]apiextensionsv1.JSON, key string) (bool, error) { //nolint:unparam
	v, ok := config[key]
	if !ok {
		return false, fmt.Errorf("missing config key %q", key)
	}
	var b bool
	if err := json.Unmarshal(v.Raw, &b); err != nil {
		return false, fmt.Errorf("config key %q: %w", key, err)
	}
	return b, nil
}
