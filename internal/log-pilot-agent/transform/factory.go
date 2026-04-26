package transform

import (
	"encoding/json"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

// NewFromSpec creates a Transform from a TransformSpec.
func NewFromSpec(spec logpilotv1alpha1.TransformSpec) (Transform, error) {
	switch spec.Type {
	case "json":
		return NewJSONTransform(), nil

	case "label":
		fields, err := extractStringMap(spec.Config, "fields")
		if err != nil {
			return nil, fmt.Errorf("label transform: %w", err)
		}
		return NewLabelTransform(fields), nil

	case "drop":
		key, err := extractString(spec.Config, "key")
		if err != nil {
			return nil, fmt.Errorf("drop transform: %w", err)
		}
		value, err := extractString(spec.Config, "value")
		if err != nil {
			return nil, fmt.Errorf("drop transform: %w", err)
		}
		return NewDropTransform(key, value), nil

	default:
		return nil, fmt.Errorf("unknown transform type: %q", spec.Type)
	}
}

// NewSliceFromSpecs creates a slice of Transforms from a slice of TransformSpecs.
func NewSliceFromSpecs(specs []logpilotv1alpha1.TransformSpec) ([]Transform, error) {
	transforms := make([]Transform, 0, len(specs))
	for _, s := range specs {
		t, err := NewFromSpec(s)
		if err != nil {
			return nil, err
		}
		transforms = append(transforms, t)
	}
	return transforms, nil
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

func extractStringMap(config map[string]apiextensionsv1.JSON, key string) (map[string]string, error) {
	v, ok := config[key]
	if !ok {
		return nil, fmt.Errorf("missing required config key %q", key)
	}
	var m map[string]string
	if err := json.Unmarshal(v.Raw, &m); err != nil {
		return nil, fmt.Errorf("config key %q: %w", key, err)
	}
	return m, nil
}
