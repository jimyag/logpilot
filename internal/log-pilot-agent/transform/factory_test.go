package transform

import (
	"context"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

func TestNewFromSpecJSON(t *testing.T) {
	spec := logpilotv1alpha1.TransformSpec{Type: "json"}
	tr, err := NewFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	records := []input.Record{{Data: []byte(`{"level":"INFO"}`)}}
	out, _ := tr.Transform(context.Background(), records)
	if out[0].Meta["level"] != "INFO" {
		t.Errorf("expected level=INFO, got %q", out[0].Meta["level"])
	}
}

func TestNewFromSpecLabel(t *testing.T) {
	spec := logpilotv1alpha1.TransformSpec{
		Type: "label",
		Config: map[string]apiextensionsv1.JSON{
			"fields": {Raw: []byte(`{"pod":"mypod"}`)},
		},
	}
	tr, err := NewFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	records := []input.Record{{Data: []byte("hello")}}
	out, _ := tr.Transform(context.Background(), records)
	if out[0].Meta["pod"] != "mypod" {
		t.Errorf("expected pod=mypod, got %q", out[0].Meta["pod"])
	}
}

func TestNewFromSpecDrop(t *testing.T) {
	spec := logpilotv1alpha1.TransformSpec{
		Type: "drop",
		Config: map[string]apiextensionsv1.JSON{
			"key":   {Raw: []byte(`"level"`)},
			"value": {Raw: []byte(`"DEBUG"`)},
		},
	}
	tr, err := NewFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	records := []input.Record{
		{Data: []byte("debug"), Meta: map[string]string{"level": "DEBUG"}},
		{Data: []byte("info"), Meta: map[string]string{"level": "INFO"}},
	}
	out, _ := tr.Transform(context.Background(), records)
	if len(out) != 1 {
		t.Fatalf("expected 1 record after drop, got %d", len(out))
	}
}

func TestNewFromSpecUnknown(t *testing.T) {
	_, err := NewFromSpec(logpilotv1alpha1.TransformSpec{Type: "unknown"})
	if err == nil {
		t.Fatal("expected error for unknown transform type")
	}
}

func TestNewSliceFromSpecs(t *testing.T) {
	specs := []logpilotv1alpha1.TransformSpec{
		{Type: "json"},
		{Type: "label", Config: map[string]apiextensionsv1.JSON{
			"fields": {Raw: []byte(`{"app":"test"}`)},
		}},
	}
	transforms, err := NewSliceFromSpecs(specs)
	if err != nil {
		t.Fatal(err)
	}
	if len(transforms) != 2 {
		t.Fatalf("expected 2 transforms, got %d", len(transforms))
	}
}

func TestNewSliceFromSpecsEmpty(t *testing.T) {
	transforms, err := NewSliceFromSpecs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(transforms) != 0 {
		t.Fatalf("expected 0 transforms, got %d", len(transforms))
	}
}
