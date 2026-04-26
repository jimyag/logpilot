package output

import (
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func TestNewOutputHTTP(t *testing.T) {
	spec := logpilotv1alpha1.OutputSpec{
		Type: "http",
		Config: map[string]apiextensionsv1.JSON{
			"url": {Raw: []byte(`"http://localhost:9999"`)},
		},
	}
	out, err := NewFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
}

func TestNewOutputFile(t *testing.T) {
	spec := logpilotv1alpha1.OutputSpec{
		Type: "file",
		Config: map[string]apiextensionsv1.JSON{
			"path": {Raw: []byte(`"/tmp/test-out.json"`)},
		},
	}
	out, err := NewFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
}

func TestNewOutputMissingConfig(t *testing.T) {
	spec := logpilotv1alpha1.OutputSpec{Type: "http"}
	_, err := NewFromSpec(spec)
	if err == nil {
		t.Fatal("expected error when url config is missing")
	}
}

func TestNewOutputUnknown(t *testing.T) {
	spec := logpilotv1alpha1.OutputSpec{Type: "unknown"}
	_, err := NewFromSpec(spec)
	if err == nil {
		t.Fatal("expected error for unknown output type")
	}
}
