package output

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

func TestFileOutput(t *testing.T) {
	f, err := os.CreateTemp("", "logpilot-out-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.Close()

	out := NewFileOutput(f.Name())
	records := []input.Record{
		{Data: []byte("hello"), Meta: map[string]string{"pod": "mypod"}},
		{Data: []byte("world"), Meta: map[string]string{"pod": "mypod"}},
	}
	if err := out.WriteBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Verify two JSON lines were written.
	var line1 map[string]interface{}
	lines := splitLines(data)
	if len(lines) < 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if err := json.Unmarshal(lines[0], &line1); err != nil {
		t.Fatalf("invalid JSON on line 1: %v", err)
	}
	if line1["data"] != "hello" {
		t.Errorf("expected data=hello, got %v", line1["data"])
	}
	if line1["pod"] != "mypod" {
		t.Errorf("expected pod=mypod, got %v", line1["pod"])
	}
}

func TestHTTPOutput(t *testing.T) {
	var received []map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	out := NewHTTPOutput(HTTPConfig{URL: srv.URL})
	records := []input.Record{
		{Data: []byte("log line"), Meta: map[string]string{"ns": "default"}},
	}
	if err := out.WriteBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}

	if len(received) != 1 {
		t.Fatalf("expected 1 record, got %d", len(received))
	}
	if received[0]["data"] != "log line" {
		t.Errorf("expected data='log line', got %v", received[0]["data"])
	}
}

func TestHTTPOutputError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", 500)
	}))
	defer srv.Close()

	out := NewHTTPOutput(HTTPConfig{URL: srv.URL})
	err := out.WriteBatch(context.Background(), []input.Record{{Data: []byte("x")}})
	if err == nil {
		t.Fatal("expected error on 500 response")
	}
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			if i > start {
				lines = append(lines, data[start:i])
			}
			start = i + 1
		}
	}
	return lines
}
