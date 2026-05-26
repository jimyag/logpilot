package output

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

func TestFileOutput(t *testing.T) {
	f, err := os.CreateTemp("", "logpilot-out-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	_ = f.Close()

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
	var line1 map[string]any
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
	var received []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	out, err := NewHTTPOutput(HTTPConfig{URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
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

	out, err := NewHTTPOutput(HTTPConfig{URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	err = out.WriteBatch(context.Background(), []input.Record{{Data: []byte("x")}})
	if err == nil {
		t.Fatal("expected error on 500 response")
	}
}

func TestNewHTTPOutputWithTLSCACert(t *testing.T) {
	out, err := NewHTTPOutput(HTTPConfig{
		URL:       "https://example.com",
		TLSCACert: mustTLSCACertPEM(t),
	})
	if err != nil {
		t.Fatal(err)
	}

	httpOut, ok := out.(*httpOutput)
	if !ok {
		t.Fatalf("expected *httpOutput, got %T", out)
	}
	transport, ok := httpOut.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", httpOut.client.Transport)
	}
	if transport.TLSClientConfig == nil || transport.TLSClientConfig.RootCAs == nil {
		t.Fatal("expected RootCAs to be configured")
	}
}

func TestHTTPOutputClose(t *testing.T) {
	out, err := NewHTTPOutput(HTTPConfig{URL: "http://example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPOutputWithHeaders(t *testing.T) {
	authCh := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authCh <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	out, err := NewHTTPOutput(HTTPConfig{
		URL:     srv.URL,
		Headers: map[string]string{"Authorization": "Bearer token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := out.WriteBatch(context.Background(), []input.Record{{Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if got := <-authCh; got != "Bearer token" {
		t.Fatalf("expected Authorization header to be forwarded, got %q", got)
	}
}

func TestFileOutputCloseNilFile(t *testing.T) {
	out := NewFileOutput(filepath.Join(t.TempDir(), "out.json"))
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileOutputCloseAfterWrite(t *testing.T) {
	out := &fileOutput{path: filepath.Join(t.TempDir(), "out.json")}
	if err := out.WriteBatch(context.Background(), []input.Record{{Data: []byte("hello")}}); err != nil {
		t.Fatal(err)
	}
	if out.f == nil {
		t.Fatal("expected file to be opened during write")
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := out.f.Write([]byte("x")); err == nil {
		t.Fatal("expected write to closed file to fail")
	}
}

func TestFileOutputWriteBatchOpenError(t *testing.T) {
	out := &fileOutput{path: t.TempDir()}
	if err := out.WriteBatch(context.Background(), []input.Record{{Data: []byte("x")}}); err == nil {
		t.Fatal("expected error when opening directory as output file")
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

func mustTLSCACertPEM(t *testing.T) string {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
}
