package transform

import (
	"context"
	"testing"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

func TestJSONTransform(t *testing.T) {
	tr := NewJSONTransform()
	records := []input.Record{{Data: []byte(`{"level":"INFO","msg":"hello"}`)}}
	out, err := tr.Transform(context.Background(), records)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
	if out[0].Meta["level"] != "INFO" {
		t.Errorf("expected level=INFO, got %q", out[0].Meta["level"])
	}
	if out[0].Meta["msg"] != "hello" {
		t.Errorf("expected msg=hello, got %q", out[0].Meta["msg"])
	}
}

func TestJSONTransformInvalidJSON(t *testing.T) {
	tr := NewJSONTransform()
	records := []input.Record{{Data: []byte("not json")}}
	out, err := tr.Transform(context.Background(), records)
	if err != nil {
		t.Fatal(err)
	}
	// Invalid JSON records pass through unchanged.
	if len(out) != 1 {
		t.Fatalf("expected 1 record (pass-through), got %d", len(out))
	}
}

func TestLabelTransform(t *testing.T) {
	tr := NewLabelTransform(map[string]string{"pod": "mypod", "ns": "default"})
	records := []input.Record{{Data: []byte("hello")}}
	out, err := tr.Transform(context.Background(), records)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Meta["pod"] != "mypod" {
		t.Errorf("expected pod=mypod, got %q", out[0].Meta["pod"])
	}
	if out[0].Meta["ns"] != "default" {
		t.Errorf("expected ns=default, got %q", out[0].Meta["ns"])
	}
}

func TestDropTransform(t *testing.T) {
	tr := NewDropTransform("level", "DEBUG")
	records := []input.Record{
		{Data: []byte("debug line"), Meta: map[string]string{"level": "DEBUG"}},
		{Data: []byte("info line"), Meta: map[string]string{"level": "INFO"}},
		{Data: []byte("warn line"), Meta: map[string]string{"level": "WARN"}},
	}
	out, err := tr.Transform(context.Background(), records)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 records after drop, got %d", len(out))
	}
	if string(out[0].Data) != "info line" {
		t.Errorf("expected 'info line', got %q", out[0].Data)
	}
}

func TestDropTransformNoMatch(t *testing.T) {
	tr := NewDropTransform("level", "DEBUG")
	records := []input.Record{
		{Data: []byte("info"), Meta: map[string]string{"level": "INFO"}},
	}
	out, _ := tr.Transform(context.Background(), records)
	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
}
