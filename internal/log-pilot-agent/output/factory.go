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
		return NewHTTPOutput(HTTPConfig{URL: url}), nil

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
