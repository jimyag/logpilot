package output

import (
	"encoding/json"
	"net/http"
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

func TestExtractStringMapMissingKey(t *testing.T) {
	_, err := extractStringMap(map[string]apiextensionsv1.JSON{}, "headers")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestExtractStringMapTypeMismatch(t *testing.T) {
	_, err := extractStringMap(map[string]apiextensionsv1.JSON{
		"headers": mustJSON(t, 1),
	}, "headers")
	if err == nil {
		t.Fatal("expected error for non-object headers config")
	}
}

func TestExtractStringMapSuccess(t *testing.T) {
	got, err := extractStringMap(map[string]apiextensionsv1.JSON{
		"headers": mustJSON(t, map[string]string{"Authorization": "Bearer token", "X-Test": "value"}),
	}, "headers")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["Authorization"] != "Bearer token" || got["X-Test"] != "value" {
		t.Fatalf("unexpected map: %#v", got)
	}
}

func TestExtractBoolMissingKey(t *testing.T) {
	_, err := extractBool(map[string]apiextensionsv1.JSON{}, "tlsSkipVerify")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestExtractBoolTypeMismatch(t *testing.T) {
	_, err := extractBool(map[string]apiextensionsv1.JSON{
		"tlsSkipVerify": mustJSON(t, "yes"),
	}, "tlsSkipVerify")
	if err == nil {
		t.Fatal("expected error for non-bool tlsSkipVerify config")
	}
}

func TestExtractBoolSuccess(t *testing.T) {
	got, err := extractBool(map[string]apiextensionsv1.JSON{
		"tlsSkipVerify": mustJSON(t, true),
	}, "tlsSkipVerify")
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("expected true")
	}
}

func TestNewFromSpecHTTPWithHeaders(t *testing.T) {
	out, err := NewFromSpec(logpilotv1alpha1.OutputSpec{
		Type: "http",
		Config: map[string]apiextensionsv1.JSON{
			"url":     mustJSON(t, "http://localhost:9999"),
			"headers": mustJSON(t, map[string]string{"Authorization": "Bearer token"}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	httpOut, ok := out.(*httpOutput)
	if !ok {
		t.Fatalf("expected *httpOutput, got %T", out)
	}
	if got := httpOut.cfg.Headers["Authorization"]; got != "Bearer token" {
		t.Fatalf("expected Authorization header to be set, got %q", got)
	}
}

func TestNewFromSpecHTTPTLSSkipVerify(t *testing.T) {
	out, err := NewFromSpec(logpilotv1alpha1.OutputSpec{
		Type: "http",
		Config: map[string]apiextensionsv1.JSON{
			"url":           mustJSON(t, "https://localhost:9999"),
			"tlsSkipVerify": mustJSON(t, true),
		},
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
	if transport.TLSClientConfig == nil || !transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("expected TLS skip verify to be enabled")
	}
}

func TestNewFromSpecHTTPInvalidCACert(t *testing.T) {
	_, err := NewFromSpec(logpilotv1alpha1.OutputSpec{
		Type: "http",
		Config: map[string]apiextensionsv1.JSON{
			"url":       mustJSON(t, "https://localhost:9999"),
			"tlsCACert": mustJSON(t, "not-valid-pem"),
		},
	})
	if err == nil {
		t.Fatal("expected error for invalid TLS CA cert")
	}
}

func mustJSON(t *testing.T, v interface{}) apiextensionsv1.JSON {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return apiextensionsv1.JSON{Raw: raw}
}
